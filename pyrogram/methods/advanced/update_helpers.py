#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.

from pyrogram import raw


def encrypted_updates(diff, previous_qts, state_qts):
    messages = getattr(diff, "new_encrypted_messages", ())
    if not messages:
        return []

    previous_qts = previous_qts or 0
    if state_qts is None or state_qts < previous_qts or state_qts - previous_qts > 10000:
        return None

    used = {
        update.qts
        for update in getattr(diff, "other_updates", ())
        if getattr(update, "qts", None) is not None
        and previous_qts < update.qts <= state_qts
    }
    missing = [qts for qts in range(previous_qts + 1, state_qts + 1) if qts not in used]
    if len(missing) != len(messages):
        return None

    return [
        raw.types.UpdateNewEncryptedMessage(message=message, qts=qts)
        for message, qts in zip(messages, missing)
    ]


def ordered_difference_updates(other_updates, encrypted):
    other_updates = list(other_updates)
    sequenced = [
        update for update in other_updates
        if getattr(update, "qts", None) is not None
    ]
    sequenced.extend(encrypted)
    sequenced.sort(key=lambda update: update.qts)
    unsequenced = [
        update for update in other_updates
        if getattr(update, "qts", None) is None
    ]
    return sequenced + unsequenced
