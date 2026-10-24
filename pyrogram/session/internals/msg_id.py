#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import logging
import threading
import time

log = logging.getLogger(__name__)


class _MsgIdGenerator:
    """Per-instance monotonic msg_id generator with server-clock synchronisation.

    All state is instance-level so that multiple Client instances running in
    the same process each maintain their own clock offset.  A skewed server
    clock on DC-2 therefore does not shift the time offset for DC-4 or any
    other session owned by a different client.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._last_msg_id = 0
        self._base_wall = time.time()
        self._base_mono = time.monotonic()
        self.time_offset = 0.0

    def now(self) -> float:
        return self._base_wall + (time.monotonic() - self._base_mono) + self.time_offset

    def sync(self, server_msg_id: int, rejected: bool = False):
        with self._lock:
            self._base_wall = time.time()
            self._base_mono = time.monotonic()
            self.time_offset = (server_msg_id >> 32) - self._base_wall

            if rejected:
                self._last_msg_id = 0

        if abs(self.time_offset) > 30:
            log.info("System clock is off, time offset set to %.1fs", self.time_offset)

    def __call__(self) -> int:
        now = self.now()

        with self._lock:
            msg_id = int(now * 2 ** 32) & ~3

            if msg_id <= self._last_msg_id:
                msg_id = self._last_msg_id + 4

            self._last_msg_id = msg_id
            return msg_id


# Process-wide singleton kept for callers that have no session context
# (primarily auth.py during the initial key exchange).  Session instances
# use a dedicated generator via MsgFactory so their clock offsets are
# isolated from one another.
MsgId = _MsgIdGenerator()
