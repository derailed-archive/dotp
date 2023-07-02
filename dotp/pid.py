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

from base64 import b85decode, b85encode
from typing import TYPE_CHECKING, Any, Self

import msgspec
import ulid

if TYPE_CHECKING:
    from .isolation import Isolate


IP = ulid.new().hex


class PID:
    def __init__(
        self, visible_id: str, machine_ip: str, isolate: Isolate | None = None
    ) -> None:
        self.visible_id = visible_id
        self.machine_ip = machine_ip
        self.d = {"vid": visible_id, "ip": b85encode(machine_ip)}
        self.packed = msgspec.msgpack.encode(self.d)
        self.isolate = isolate

    @classmethod
    def decode(cls, data: Any) -> Self:
        unpacked = msgspec.msgpack.decode(data)
        return cls(unpacked["vid"], b85decode(unpacked["ip"]))

    @classmethod
    def decode_bulk(cls, data: list[dict[str, Any]]) -> None:
        return [cls(d["vid"], b85decode(d["ip"])) for d in data]
