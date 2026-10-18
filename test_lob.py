"""
Failure tests for lob.py against a fake Binance (REST + WebSocket) on localhost.

    pip install pytest
    pytest -q test_lob.py

The fake exchange keeps a "true" order book and streams diff events with update
IDs, exactly like Binance. Each test injects a failure, then pauses the exchange
and checks that the local book is identical, level for level, to the true book.
"""

import asyncio
import contextlib
import json
import logging
import random
import time
from decimal import Decimal

from aiohttp import web

import lob

SYMBOL = "TESTUSDT"
KILL = object()


# =========================================================================== fake exchange

class FakeExchange:
    def __init__(self, seed: int = 7, levels: int = 200):
        self.rng = random.Random(seed)
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.uid = 1000
        self.clients: list[asyncio.Queue] = []
        # injected faults
        self.paused = False
        self.drop_next = False       # skip one event on the wire (creates a gap)
        self.garbage_next = False    # send an unparseable frame
        self.bad_levels_next = False  # send a valid-JSON event with nonsense prices
        self.rest_429 = 0            # answer the next N snapshot calls with 429
        self.rest_delay = 0.05
        self.snapshots_served = 0
        for i in range(1, levels + 1):
            self.bids[self._px(100 - i * Decimal("0.01"))] = self._qty()
            self.asks[self._px(100 + i * Decimal("0.01"))] = self._qty()

    @staticmethod
    def _px(p: Decimal) -> str:
        return f"{p:.8f}"

    def _qty(self) -> str:
        return f"{Decimal(self.rng.randint(1, 50000)) / 1000:.8f}"

    def _step(self) -> dict:
        b, a = [], []
        for _ in range(self.rng.randint(1, 5)):
            if self.rng.random() < 0.5:
                side, out, price = self.bids, b, 100 - Decimal(self.rng.randint(1, 1000)) / 100
            else:
                side, out, price = self.asks, a, 100 + Decimal(self.rng.randint(1, 1000)) / 100
            p = self._px(price)
            if self.rng.random() < 0.3:
                side.pop(p, None)
                out.append([p, "0.00000000"])
            else:
                q = self._qty()
                side[p] = q
                out.append([p, q])
        first = self.uid + 1
        self.uid += self.rng.randint(1, 3)
        return {"e": "depthUpdate", "E": int(time.time() * 1000), "s": SYMBOL,
                "U": first, "u": self.uid, "b": b, "a": a}

    def snapshot(self, limit: int) -> dict:
        bids = sorted(self.bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[:limit]
        asks = sorted(self.asks.items(), key=lambda kv: Decimal(kv[0]))[:limit]
        return {"lastUpdateId": self.uid, "bids": [list(x) for x in bids],
                "asks": [list(x) for x in asks]}

    def kill_connections(self):
        for q in self.clients:
            q.put_nowait(KILL)

    async def generator(self):
        while True:
            await asyncio.sleep(0.002)
            if self.paused:
                continue
            ev = self._step()
            if self.drop_next:
                self.drop_next = False
                continue
            frames = []
            if self.garbage_next:
                self.garbage_next = False
                frames.append('{"e": "depthUpdate", "U": 1, broken')
            if self.bad_levels_next:
                self.bad_levels_next = False
                bad = dict(ev, b=[["NaN", "abc"]])
                frames.append(json.dumps(bad))
                # the bad frame consumed this event's IDs; real event is skipped
                ev = None
            if ev is not None:
                frames.append(json.dumps(ev))
            for q in self.clients:
                for f in frames:
                    q.put_nowait(f)

    # ---- HTTP handlers

    async def depth(self, request: web.Request):
        if self.rest_429 > 0:
            self.rest_429 -= 1
            return web.Response(status=429, headers={"Retry-After": "1"})
        await asyncio.sleep(self.rest_delay)   # let events pile up behind the snapshot
        self.snapshots_served += 1
        snap = self.snapshot(int(request.query.get("limit", 100)))
        return web.json_response(snap, headers={"X-MBX-USED-WEIGHT-1M": "250"})

    async def ws(self, request: web.Request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        q: asyncio.Queue = asyncio.Queue()
        self.clients.append(q)
        try:
            while True:
                msg = await q.get()
                if msg is KILL:
                    await ws.close()
                    break
                await ws.send_str(msg)
        finally:
            self.clients.remove(q)
        return ws


@contextlib.asynccontextmanager
async def exchange(**kw):
    ex = FakeExchange(**kw)
    app = web.Application()
    app.router.add_get("/api/v3/depth", ex.depth)
    app.router.add_get("/ws/{stream}", ex.ws)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    gen = asyncio.create_task(ex.generator())
    try:
        yield ex, port
    finally:
        gen.cancel()
        ex.kill_connections()
        await runner.cleanup()


@contextlib.asynccontextmanager
async def client(port: int, **cfg_overrides):
    import aiohttp
    cfg = lob.Config(symbol=SYMBOL, ws_base=f"ws://127.0.0.1:{port}",
                     rest_base=f"http://127.0.0.1:{port}", stale_after=1.0,
                     backoff_base=0.05, min_snapshot_interval=0.05)
    for k, v in cfg_overrides.items():
        setattr(cfg, k, v)
    async with aiohttp.ClientSession() as session:
        sync = lob.BookSync(cfg, lob.RateLimitedRest(session, cfg.rest_base, weight_limit=10**9))
        task = asyncio.create_task(sync.run())
        try:
            yield sync
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            assert task.cancelled(), "run() must only end by cancellation"


async def wait_for(cond, timeout=8.0, what="condition"):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


async def assert_matches(ex: FakeExchange, sync: lob.BookSync, top: int | None = None):
    """Pause the exchange, let the client catch up, compare every level."""
    await wait_for(lambda: sync.state is lob.State.LIVE, what="LIVE")
    ex.paused = True
    try:
        await wait_for(lambda: sync.book.last_update_id == ex.uid, what="catch-up")
        truth_b = sorted(((Decimal(p), Decimal(q)) for p, q in ex.bids.items()), reverse=True)
        truth_a = sorted((Decimal(p), Decimal(q)) for p, q in ex.asks.items())
        local_b, local_a = list(sync.book.bids.items()), list(sync.book.asks.items())
        if top is not None:
            truth_b, truth_a = truth_b[:top], truth_a[:top]
            local_b, local_a = local_b[:top], local_a[:top]
        assert local_b == truth_b, "bid side differs from exchange"
        assert local_a == truth_a, "ask side differs from exchange"
        assert not sync.book.is_crossed()
    finally:
        ex.paused = False


def run(coro):
    asyncio.run(asyncio.wait_for(coro, 60))


# =========================================================================== tests

def test_syncs_and_matches_exchange_exactly():
    async def t():
        async with exchange() as (ex, port), client(port) as sync:
            await assert_matches(ex, sync)
            await asyncio.sleep(0.5)              # keep streaming, compare again
            await assert_matches(ex, sync)
            assert sync.stats["resyncs"] == 0 and sync.stats["reconnects"] == 0
    run(t())


def test_gap_triggers_resync_on_same_connection():
    async def t():
        async with exchange() as (ex, port), client(port) as sync:
            await assert_matches(ex, sync)
            ex.drop_next = True
            await wait_for(lambda: sync.stats["resyncs"] >= 1, what="resync")
            await assert_matches(ex, sync)
            assert sync.stats["reconnects"] == 0, "a gap should not need a new connection"
    run(t())


def test_disconnect_reconnects():
    async def t():
        async with exchange() as (ex, port), client(port) as sync:
            await assert_matches(ex, sync)
            ex.kill_connections()
            await wait_for(lambda: sync.stats["reconnects"] >= 1, what="reconnect")
            await assert_matches(ex, sync)
    run(t())


def test_unparseable_frame_is_survived():
    async def t():
        async with exchange() as (ex, port), client(port) as sync:
            await assert_matches(ex, sync)
            ex.garbage_next = True
            await wait_for(lambda: sync.stats["parse_errors"] >= 1, what="parse error")
            await assert_matches(ex, sync)
    run(t())


def test_nonsense_prices_resync_without_corrupting_book():
    async def t():
        async with exchange() as (ex, port), client(port) as sync:
            await assert_matches(ex, sync)
            ex.bad_levels_next = True
            await wait_for(lambda: sync.stats["resyncs"] >= 1, what="resync")
            await assert_matches(ex, sync)
    run(t())


def test_429_on_snapshot_waits_and_recovers():
    async def t():
        async with exchange() as (ex, port):
            ex.rest_429 = 1
            start = time.monotonic()
            async with client(port) as sync:
                await assert_matches(ex, sync)
                assert time.monotonic() - start >= 1.0, "must honour Retry-After"
    run(t())


def test_silent_feed_is_detected_and_replaced():
    async def t():
        async with exchange() as (ex, port), client(port) as sync:
            await assert_matches(ex, sync)
            ex.paused = True                        # connection open, nothing sent
            await wait_for(lambda: not sync.ready, timeout=3, what="not ready")
            await wait_for(lambda: sync.stats["stale"] >= 1, timeout=3, what="stale")
            ex.paused = False
            await assert_matches(ex, sync)
    run(t())


def test_snapshot_older_than_buffer_is_refetched():
    async def t():
        async with exchange() as (ex, port):
            # Serve a stale snapshot first: client must notice and fetch again.
            real = ex.snapshot
            calls = {"n": 0}

            def stale_once(limit):
                calls["n"] += 1
                snap = real(limit)
                if calls["n"] == 1:
                    snap["lastUpdateId"] -= 10_000
                return snap
            ex.snapshot = stale_once
            async with client(port) as sync:
                await assert_matches(ex, sync)
                assert calls["n"] >= 2
    run(t())


def test_memory_bounded_by_max_levels():
    async def t():
        async with exchange(levels=300) as (ex, port), client(port, max_levels=50) as sync:
            await assert_matches(ex, sync, top=20)
            await asyncio.sleep(0.5)
            assert len(sync.book.bids) <= 50 and len(sync.book.asks) <= 50
    run(t())


def test_planned_rotation_keeps_book_correct():
    async def t():
        async with exchange() as (ex, port), client(port, rotate_after=0.4) as sync:
            await wait_for(lambda: sync.stats["rotations"] >= 2, what="rotations")
            await assert_matches(ex, sync)
            assert sync.stats["reconnects"] == 0, "rotation is not a failure"
    run(t())


def test_callback_exception_does_not_stop_book():
    async def t():
        async with exchange() as (ex, port):
            def boom(_):
                raise RuntimeError("user bug")
            async with client(port) as sync:
                sync.on_update = boom
                await assert_matches(ex, sync)
                assert sync.stats["callback_errors"] > 0
    run(t())


def test_exchange_down_then_up():
    async def t():
        # Grab a free port, start client against nothing, then bring exchange up there.
        import socket
        s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
        async with client(port) as sync:
            await asyncio.sleep(0.5)
            assert sync.stats["reconnects"] >= 1 and not sync.ready
            ex = FakeExchange()
            app = web.Application()
            app.router.add_get("/api/v3/depth", ex.depth)
            app.router.add_get("/ws/{stream}", ex.ws)
            runner = web.AppRunner(app); await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", port).start()
            gen = asyncio.create_task(ex.generator())
            try:
                await assert_matches(ex, sync)
            finally:
                gen.cancel(); ex.kill_connections(); await runner.cleanup()
    run(t())


def test_crossed_book_unit():
    b = lob.OrderBook()
    b.load_snapshot({"lastUpdateId": 1, "bids": [["10", "1"]], "asks": [["11", "1"]]})
    b._apply([(Decimal(12), Decimal(1))], [], 2)
    assert b.is_crossed()


def test_bad_snapshot_never_touches_existing_book():
    b = lob.OrderBook()
    b.load_snapshot({"lastUpdateId": 1, "bids": [["10", "1"]], "asks": [["11", "1"]]})
    try:
        b.load_snapshot({"lastUpdateId": 2, "bids": [["10", "1"]], "asks": [["oops", "1"]]})
    except Exception:
        pass
    assert b.last_update_id == 1 and b.best_ask() == (Decimal(11), Decimal(1))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
