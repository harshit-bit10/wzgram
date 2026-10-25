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

import asyncio
import logging
import sys
import weakref
from typing import Iterable, Optional, Tuple, Union

import pyrogram
from pyrogram import raw
from pyrogram.errors import ChannelInvalid, ChannelPrivate, PeerIdInvalid, PersistentTimestampInvalid, PersistentTimestampOutdated
from pyrogram.utils import ZERO_CHANNEL_ID

log = logging.getLogger(__name__)


class RecoverGaps:
    MAX_STALE_TIMESTAMP_RETRIES = 3

    async def recover_gaps(
        self: "pyrogram.Client",
        ids: Optional[Union[int, Iterable[int]]] = None
    ) -> Tuple[int, int]:
        """Restores updates for the time while the client was offline.

        It works whatever ``Client.skip_updates`` is set to: that parameter only decides whether
        the client recovers on its own, at start and when updates stop arriving.

        .. note::

            The update state is kept in the session storage. With ``Client.in_memory`` set to True
            it is lost when the client stops, so only gaps opened since the last start can be recovered.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            ids (``int`` | Iterable of ``int``, *optional*):
                Identifiers of the chats to recover, 0 for private chats and other updates.
                By default, every known chat is recovered.

        Returns:
            ``tuple``: The number of messages and updates recovered is returned.
        """
        message_updates_counter = 0
        other_updates_counter = 0

        states = await self.storage.update_state()

        if ids is not None:
            wanted = {ids} if isinstance(ids, int) else set(ids)
            states = [state for state in states or () if state[0] in wanted]

        if not states:
            log.info("No states found, skipping recovery")
            return (message_updates_counter, other_updates_counter)

        log.info("Started gaps recovering...")

        # Build the list of peers that actually need work.
        eligible = [
            s for s in states
            if not (s[1] is None and (s[0] != 0 or s[2] is None))
        ]

        async def _run_one(local_state) -> Tuple[int, int]:
            id, local_pts, local_qts, local_date, local_seq = local_state
            # _peer_recovery_locks is a WeakValueDictionary: entries are
            # collected automatically once no coroutine holds the lock, so
            # the dict never grows unbounded for accounts with many channels.
            peer_lock = self._peer_recovery_locks.setdefault(id, asyncio.Lock())
            return await self._recover_peer(
                id, local_pts, local_qts, local_date, local_seq, peer_lock
            )

        # On Python 3.11+ use TaskGroup so all peers are recovered concurrently
        # and a single peer failure does not silently abort the rest.
        # On 3.10 fall back to asyncio.gather with return_exceptions=True so
        # behaviour is equivalent: failures are logged, not propagated.
        if sys.version_info >= (3, 11):
            results: list[Tuple[int, int] | BaseException] = []
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(_run_one(s)) for s in eligible]
            results = [t.result() for t in tasks]
        else:
            raw_results = await asyncio.gather(
                *(_run_one(s) for s in eligible),
                return_exceptions=True,
            )
            results = []
            for r in raw_results:
                if isinstance(r, BaseException):
                    log.exception("Gap recovery task raised unexpectedly: %r", r)
                else:
                    results.append(r)

        for msgs, upds in results:
            message_updates_counter += msgs
            other_updates_counter += upds

        log.info("Recovered %s messages and %s updates", message_updates_counter, other_updates_counter)
        return (message_updates_counter, other_updates_counter)

    async def _recover_peer(
        self: "pyrogram.Client",
        id: int,
        local_pts,
        local_qts,
        local_date,
        local_seq,
        lock: asyncio.Lock,
    ) -> Tuple[int, int]:
        """Run the GetDifference loop for a single peer under its per-peer lock."""
        message_updates_counter = 0
        other_updates_counter = 0

        async with lock:
            stale_attempts = 0
            unusable = False
            failed = False

            while True:
                try:
                    if local_pts is None:
                        state = await self.invoke(raw.functions.updates.GetState())
                        local_pts, local_date, local_seq = state.pts, state.date, state.seq

                    request = (local_pts, local_qts)

                    diff = await self.invoke(
                        raw.functions.updates.GetChannelDifference(
                            channel=await self.resolve_peer(id),
                            filter=raw.types.ChannelMessagesFilterEmpty(),
                            pts=local_pts,
                            limit=10000,
                            force=False
                        ) if id < ZERO_CHANNEL_ID else
                        raw.functions.updates.GetDifference(
                            pts=local_pts,
                            date=local_date,
                            qts=local_qts or 0
                        )
                    )
                except (ChannelPrivate, ChannelInvalid, PeerIdInvalid):
                    unusable = True
                    break
                except (PersistentTimestampOutdated, PersistentTimestampInvalid) as e:
                    stale_attempts += 1

                    if stale_attempts >= RecoverGaps.MAX_STALE_TIMESTAMP_RETRIES:
                        log.info(
                            "Dropping the stored state of %s: %s after %s attempts",
                            id, e.ID, stale_attempts
                        )
                        unusable = True
                        break

                    await asyncio.sleep(stale_attempts)
                    continue
                except BaseException as e:
                    # Re-raise task cancellation and interpreter shutdown — these
                    # must not be swallowed.  Everything else (RPC errors, network
                    # failures, unexpected server replies) is logged and the loop
                    # moves on: one unrecoverable peer must not abort the others.
                    if isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                        raise
                    log.exception("Gap recovery failed for %s", id)
                    failed = True
                    break

                if isinstance(diff, raw.types.updates.DifferenceEmpty):
                    await self._save_update_state(
                        (
                            id,
                            local_pts,
                            local_qts,
                            diff.date,
                            diff.seq
                        )
                    )
                    break
                elif isinstance(diff, raw.types.updates.DifferenceTooLong):
                    local_pts = diff.pts
                    await self._save_update_state(
                        (
                            id,
                            local_pts,
                            local_qts,
                            local_date,
                            local_seq
                        )
                    )
                    continue
                elif isinstance(diff, raw.types.updates.Difference):
                    local_pts = diff.state.pts
                    local_qts = diff.state.qts
                    local_date = diff.state.date
                    local_seq = diff.state.seq
                elif isinstance(diff, raw.types.updates.DifferenceSlice):
                    local_pts = diff.intermediate_state.pts
                    local_qts = diff.intermediate_state.qts
                    local_date = diff.intermediate_state.date
                    local_seq = diff.intermediate_state.seq
                elif isinstance(diff, raw.types.updates.ChannelDifferenceEmpty):
                    await self._save_update_state(
                        (
                            id,
                            diff.pts,
                            local_qts,
                            local_date,
                            local_seq
                        )
                    )
                    break
                elif isinstance(diff, raw.types.updates.ChannelDifferenceTooLong):
                    pts_from_dialog = getattr(diff.dialog, "pts", None)

                    if pts_from_dialog is None:
                        # Dialog.pts is flags.0?int in the TL schema — it is absent for
                        # certain channel types (e.g. broadcast channels without a pts
                        # counter).  Without a pts anchor we cannot resume, so drop the
                        # stored state and stop trying to recover this peer.
                        log.warning(
                            "ChannelDifferenceTooLong for %s returned a dialog with no "
                            "pts field; dropping stored state to avoid a stuck loop.",
                            id
                        )
                        unusable = True
                        break

                    local_pts = pts_from_dialog
                    await self._save_update_state(
                        (
                            id,
                            local_pts,
                            local_qts,
                            local_date,
                            local_seq
                        )
                    )
                    continue
                elif isinstance(diff, raw.types.updates.ChannelDifference):
                    local_pts = diff.pts

                users = {i.id: i for i in diff.users}
                chats = {i.id: i for i in diff.chats}

                for message in diff.new_messages:
                    message_updates_counter += 1
                    await self.dispatcher.enqueue_update(
                        raw.types.UpdateNewMessage(
                            message=message,
                            pts=local_pts,
                            pts_count=-1
                        ),
                        users,
                        chats
                    )

                for update in diff.other_updates:
                    other_updates_counter += 1
                    await self.dispatcher.enqueue_update(update, users, chats)

                if isinstance(diff, raw.types.updates.Difference):
                    break

                if isinstance(diff, raw.types.updates.ChannelDifference) and diff.final:
                    break

                if (local_pts, local_qts) == request:
                    break

            if failed:
                return (message_updates_counter, other_updates_counter)

            if unusable:
                await self._save_update_state(id)
                return (message_updates_counter, other_updates_counter)

            await self._save_update_state(
                (
                    id,
                    local_pts,
                    local_qts,
                    local_date,
                    local_seq
                )
            )

        return (message_updates_counter, other_updates_counter)
