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


import asyncio
from inspect import isfunction
from typing import Any

import ulid

from .pid import IP, PID
from .server import server


class Isolate:
    """A class for isolated OTP processes."""

    def __init__(self) -> None:
        self.pid = PID(ulid.new().hex, IP, self)
        asyncio.create_task(self.new())

    async def new(self) -> None:
        ...

    @classmethod
    async def call(
        self, func: str, pid: PID | None = None, timeout: int = 15, *args, **kwargs
    ) -> Any:
        if pid:
            for fs in server.foreign_servers:
                if fs.ip == pid.machine_ip:
                    message_id = ulid.new().hex
                    future = asyncio.Future()
                    await fs.send(
                        {
                            "op": 1,
                            "d": {
                                "c": {"func": func, "args": args, "kwargs": kwargs},
                                "pid": pid.packed,
                                "id": message_id,
                            },
                        }
                    )
                    return await asyncio.wait_for(future, timeout)
        else:
            fnc = getattr(func)

            if isfunction(fnc):
                return fnc(*args, **kwargs)
            else:
                return fnc
