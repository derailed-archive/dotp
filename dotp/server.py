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
import os
from typing import Any

import anyio
import msgspec
from anyio import connect_tcp, create_tcp_listener
from anyio.abc import SocketStream

from .pid import IP, PID


async def _send(pid: PID, *args, **kwargs) -> None:
    for fs in server.foreign_servers:
        if fs.ip == pid.machine_ip:
            await fs.send(
                {"op": 5, "d": {"pid": pid.d, "args": args, "kwargs": kwargs}}
            )
            break


async def send(pid: PID, *args, timeout: int = 14, **kwargs) -> None:
    await asyncio.wait_for(_send(pid, *args, **kwargs), timeout)


async def send2(pids: PID, *args, timeout: int = 14, **kwargs) -> None:
    sends = [send(pid, *args, timeout=timeout, **kwargs) for pid in pids]
    await asyncio.gather(*sends)


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


class ForeignClient:
    def __init__(self, ip: str, client: SocketStream) -> None:
        self.ip = ip
        self._client = client

    async def send(self, data: dict) -> None:
        try:
            await self._client.send(msgspec.msgpack.encode(data))
        except anyio.EndOfStream:
            return


class Server:
    def __init__(self) -> None:
        self.home_pids: list[PID] = []
        self.foreign_servers: list[ForeignServer] = []
        self.message_futures: dict[str, asyncio.Future] = {}
        self.foreign_clients: dict[str, ForeignClient] = {}

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
                            dpid["vid"], call["func"], *call["args"], **call["kwargs"]
                        )
                        await client.send(
                            msgspec.msgpack.encode(
                                {"op": 2, "d": {"id": message_id, "cr": resp}}
                            )
                        )
                        break

            # OP 5: send to isolate
            case 5:
                args, kwargs = d["args"], d["kwargs"]
                dpid = d["pid"]

                for pid in self.home_pids:
                    if dpid["vid"] == pid.visible_id:
                        await pid.isolate.receive(*args, **kwargs)
                        break

    async def handle(self, client: SocketStream) -> None:
        fs = None
        try:
            async with client:
                await client.send(msgspec.msgpack.encode({"op": 0, "d": {"ip": IP}}))
                welcome = await client.receive()
                decoded = msgspec.msgpack.decode(welcome)
                ip = decoded["ip"]
                fs = ForeignServer(ip, PID.decode_bulk(decoded["pids"]), client)
                self.foreign_servers.append(fs)

                while True:
                    msg = await client.receive()
                    asyncio.create_task(self.new_message(msg, SocketStream))

        except (anyio.EndOfStream, KeyError, ValueError):
            if fs:
                self.foreign_servers.remove(fs)
            return

    async def client_message(self, data: Any, client: SocketStream) -> None:
        msg = msgspec.msgpack.decode(data, type=dict)

        op = msg["op"]
        d = msg["d"]

        match op:
            # OP 2: isolate response
            case 2:
                message_id = d["id"]
                resp = d["cr"]

                fut = self.message_futures[message_id]
                fut.set_result(resp)

    async def __client_connection(self, host: str) -> None:
        cont = False
        try:
            async with await connect_tcp(
                host.split(":")[0], host.split(":")[1]
            ) as client:
                # hello message
                ip = msgspec.msgpack.decode(await client.receive(), type=dict)
                iden = ip["d"]["ip"]
                self.foreign_clients[iden] = ForeignClient(iden, client)
                cont = True
                await client.send(
                    msgspec.msgpack.encode(
                        {"ip": IP, "pids": [pid.d for pid in self.pids]}
                    )
                )

                while True:
                    msg = await client.receive()
                    asyncio.create(self.client_message(msg, client))

        except (anyio.EndOfStream, KeyError, ValueError):
            if cont:
                del self.foreign_clients[iden]
            return

    async def connect_all(self) -> None:
        hosts = os.environ.get("DOTP_PARTNERS", "").split(",")

        for host in hosts:
            asyncio.create_task(self.__client_connection(host))

    async def start(self) -> None:
        listener = await create_tcp_listener("0.0.0.0", local_port=7398)
        asyncio.create_task(self.connect_all())
        await listener.serve(self.handle)


server = Server()
