import pytest

import pyrogram.methods.rate_limiter as rate_limiter
from pyrogram.methods.rate_limiter import AdaptiveBucket, RateLimiter


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def make(monkeypatch, **kwargs):
    clock = Clock()
    monkeypatch.setattr(rate_limiter.time, "monotonic", clock)
    return AdaptiveBucket(rate=16, ceiling=100, burst=8, **kwargs), clock


def test_a_flood_lowers_the_rate_once_per_second(monkeypatch):
    bucket, clock = make(monkeypatch)

    for _ in range(8):
        bucket.on_flood()

    assert bucket.rate == 16 * 0.85

    clock.now += 1.0
    bucket.on_flood()

    assert bucket.rate == 16 * 0.85 * 0.85


def test_the_rate_never_drops_below_the_floor(monkeypatch):
    bucket, clock = make(monkeypatch, floor=2.0)

    for _ in range(100):
        clock.now += 1.0
        bucket.on_flood()

    assert bucket.rate == 2.0


def test_clean_seconds_raise_the_rate_up_to_the_ceiling(monkeypatch):
    bucket, clock = make(monkeypatch, step=0.5)

    for _ in range(8):
        bucket.on_success()

    assert bucket.rate == 16

    clock.now += 1.0
    bucket.on_success()

    assert bucket.rate == 16.5

    for _ in range(1000):
        clock.now += 1.0
        bucket.on_success()

    assert bucket.rate == 100


def test_no_raise_right_after_a_flood(monkeypatch):
    bucket, clock = make(monkeypatch)

    clock.now += 1.0
    bucket.on_flood()
    clock.now += 1.5
    bucket.on_success()

    assert bucket.rate == 16 * 0.85

    clock.now += 0.5
    bucket.on_success()

    assert bucket.rate == 16 * 0.85 + 0.5


def test_acquire_nowait_refunds_tokens_without_rewinding_time(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(rate_limiter.time, "monotonic", clock)
    limiter = RateLimiter(limits={
        "message": {"rate": 1.0, "burst": 4.0},
        "global": {"rate": 1.0, "burst": 1.0},
    })
    bucket = limiter._buckets["message"]
    global_bucket = limiter._global
    bucket._tokens = 2.0
    bucket._last_refill = clock.now - 1.0
    global_bucket._tokens = 0.0
    global_bucket._last_refill = clock.now

    assert not limiter.acquire_nowait("message")
    assert bucket._tokens == pytest.approx(3.0)
    assert bucket._last_refill == clock.now
    assert bucket._total_taken == 0


async def test_acquire_nowait_does_not_mutate_locked_buckets(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(rate_limiter.time, "monotonic", clock)
    limiter = RateLimiter(limits={
        "message": {"rate": 1.0, "burst": 4.0},
        "global": {"rate": 1.0, "burst": 2.0},
    })
    bucket = limiter._buckets["message"]
    global_bucket = limiter._global

    for locked in (bucket, global_bucket):
        before = (
            bucket._tokens,
            bucket._last_refill,
            bucket._total_taken,
            global_bucket._tokens,
            global_bucket._last_refill,
            global_bucket._total_taken,
        )
        await locked._lock.acquire()
        try:
            assert not limiter.acquire_nowait("message")
            after = (
                bucket._tokens,
                bucket._last_refill,
                bucket._total_taken,
                global_bucket._tokens,
                global_bucket._last_refill,
                global_bucket._total_taken,
            )
            assert after == before
        finally:
            locked._lock.release()
