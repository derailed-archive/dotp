"""
MIT License

Copyright (c) 2023 Derailed

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

import asyncio
from typing import Any

import anyio
import msgspec
from anyio import create_tcp_listener
from anyio.abc import SocketStream

from .pid import PID


class ForeignServer:
    def __init__(self, ip: str, pids: list[PID], client: SocketStream) -> None:
        self.ip = ip
        self.pids = pids
        self._client = client

    async def send(self, data: dict) -> None:
        try:
            await self._client.send(msgspec.msgpack.encode(data))
        except anyio.EndOfStream:
            return


class Server:
    def __init__(self) -> None:
        self.foreign_pids: list[PID] = []
        self.home_pids: list[PID] = []
        self.pids: list[PID] = []
        self.foreign_servers: list[ForeignServer] = []
        self.message_futures: dict[str, asyncio.Future] = {}

    async def new_message(self, data: Any, client: SocketStream) -> None:
        msg = msgspec.msgpack.decode(data, type=dict)

        op = msg["op"]
        d = msg["d"]

        match op:
            # OP 1: call to PID
            case 1:
                call = d["c"]
                dpid = d["pid"]
                message_id = d["id"]

                for pid in self.home_pids:
                    if dpid["vid"] == pid.visible_id:
                        resp = await pid.isolate.call(
                            call["func"], None, *call["args"], **call["kwargs"]
                        )
                        await client.send(
                            msgspec.msgpack.encode(
                                {"op": 2, "d": {"id": message_id, "cr": resp}}
                            )
                        )
                        break
            case 2:
                message_id = d["id"]
                resp = d["cr"]

                fut = self.message_futures[message_id]
                fut.set_result(resp)

    async def handle(self, client: SocketStream) -> None:
        fs = None
        try:
            async with client:
                await client.send(msgspec.msgpack.encode({"op": 0}))
                welcome = await client.receive()
                decoded = msgspec.msgpack.decode(welcome)
                ip = decoded["ip"]
                fs = ForeignServer(ip, [], client)
                self.foreign_servers.append(fs)

                while True:
                    msg = await client.receive()
                    asyncio.create_task(self.new_message(msg, SocketStream))

        except (anyio.EndOfStream, KeyError, ValueError):
            if fs:
                self.foreign_servers.remove(fs)
            return

    async def start(self) -> None:
        listener = await create_tcp_listener("0.0.0.0", local_port=7398)
        await listener.serve()


server = Server()
