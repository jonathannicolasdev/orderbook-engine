"""
Tests for recorder.py against a fake Binance that also streams trades.

Checks that every trade ID ends up on disk exactly once (even through dropped
messages, disconnects and file rotation), that book rows are well-formed, and that
files from a crashed run are recovered.
"""

import asyncio
import contextlib
import csv
import gzip
import io
import json
import time
from decimal import Decimal
from pathlib import Path

import aiohttp
from aiohttp import web

import lob
import recorder
import test_lob as T


# =========================================================================== fake exchange with trades

class FakeExchangeWithTrades(T.FakeExchange):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.trades: list[dict] = []
        self.next_trade = 5000
        self.trade_clients: list[asyncio.Queue] = []
        self.drop_trades = 0           # skip N trades on the wire (they stay in REST history)
        self.trade_garbage_next = False

    def _make_trade(self) -> dict:
        self.next_trade += 1
        t = {"a": self.next_trade,
             "p": f"{Decimal(10000 + self.rng.randint(-50, 50)) / 100:.8f}",
             "q": f"{Decimal(self.rng.randint(1, 9999)) / 1000:.8f}",
             "f": self.next_trade * 3, "l": self.next_trade * 3 + 1,
             "T": int(time.time() * 1000), "m": self.rng.random() < 0.5}
        self.trades.append(t)
        return t

    async def trade_generator(self):
        while True:
            await asyncio.sleep(0.003)
            if self.paused:
                continue
            t = self._make_trade()
            if self.drop_trades > 0:
                self.drop_trades -= 1
                continue
            frames = []
            if self.trade_garbage_next:
                self.trade_garbage_next = False
                frames.append("{nope")
            frames.append(json.dumps({"e": "aggTrade", "E": t["T"], "s": T.SYMBOL, **t}))
            for q in self.trade_clients:
                for f in frames:
                    q.put_nowait(f)

    def kill_connections(self):
        super().kill_connections()
        for q in self.trade_clients:
            q.put_nowait(T.KILL)

    async def ws(self, request):
        if not request.match_info["stream"].endswith("@aggTrade"):
            return await super().ws(request)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        q: asyncio.Queue = asyncio.Queue()
        self.trade_clients.append(q)
        try:
            while True:
                msg = await q.get()
                if msg is T.KILL:
                    await ws.close()
                    break
                try:
                    await ws.send_str(msg)
                except ConnectionResetError:
                    break
        finally:
            self.trade_clients.remove(q)
        return ws

    async def agg_trades(self, request):
        from_id = int(request.query["fromId"])
        limit = min(int(request.query.get("limit", 500)), 1000)
        out = [t for t in self.trades if t["a"] >= from_id][:limit]
        return web.json_response(out, headers={"X-MBX-USED-WEIGHT-1M": "4"})


@contextlib.asynccontextmanager
async def exchange(**kw):
    ex = FakeExchangeWithTrades(**kw)
    app = web.Application()
    app.router.add_get("/api/v3/depth", ex.depth)
    app.router.add_get("/api/v3/aggTrades", ex.agg_trades)
    app.router.add_get("/ws/{stream}", ex.ws)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    gens = [asyncio.create_task(ex.generator()), asyncio.create_task(ex.trade_generator())]
    try:
        yield ex, port
    finally:
        for g in gens:
            g.cancel()
        ex.kill_connections()
        await runner.cleanup()


@contextlib.asynccontextmanager
async def running_recorder(port: int, root: Path, **kw):
    overrides = dict(ws_base=f"ws://127.0.0.1:{port}", rest_base=f"http://127.0.0.1:{port}",
                     stale_after=1.0, backoff_base=0.05, min_snapshot_interval=0.05)
    async with aiohttp.ClientSession() as session:
        rest = lob.RateLimitedRest(session, overrides["rest_base"], weight_limit=10**9)
        rec = recorder.SymbolRecorder(T.SYMBOL, rest, root, cfg_overrides=overrides, **kw)
        tasks = rec.tasks()
        try:
            yield rec
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            rec.close()


# =========================================================================== reading back

def read_rows(folder: Path, kind: str) -> list[dict]:
    rows = []
    for p in sorted(folder.rglob(f"{kind}_*.csv*")):
        opener = gzip.open if p.suffix == ".gz" else open
        with opener(p, "rt", newline="") as f:
            rows += list(csv.DictReader(f))
    return rows


async def until_caught_up(ex, rec, timeout=8.0):
    ex.paused = True
    await T.wait_for(lambda: rec.trades.last_id == ex.next_trade, timeout, "trades caught up")


def check_trades(ex, rows):
    ids = [int(r["agg_id"]) for r in rows]
    assert ids == sorted(ids), "trades out of order"
    assert len(ids) == len(set(ids)), "duplicate trades on disk"
    assert ids == list(range(ids[0], ids[-1] + 1)), "missing trade IDs on disk"
    truth = {t["a"]: t for t in ex.trades}
    for r in rows:
        t = truth[int(r["agg_id"])]
        assert Decimal(r["price"]) == Decimal(t["p"]) and Decimal(r["qty"]) == Decimal(t["q"])
        assert r["aggressor"] == ("sell" if t["m"] else "buy")
    assert ids[-1] == ex.next_trade


def check_book(rows, depth=10):
    assert rows, "no book rows"
    for r in rows:
        bids = [Decimal(r[f"bid_px_{i}"]) for i in range(1, depth + 1) if r[f"bid_px_{i}"]]
        asks = [Decimal(r[f"ask_px_{i}"]) for i in range(1, depth + 1) if r[f"ask_px_{i}"]]
        assert bids == sorted(bids, reverse=True) and asks == sorted(asks)
        assert bids[0] < asks[0], "crossed book recorded"
        assert int(r["update_id"]) > 0 and r["event_ms"]


# =========================================================================== tests

def test_records_book_and_every_trade(tmp_path):
    async def t():
        async with exchange() as (ex, port), running_recorder(port, tmp_path) as rec:
            await asyncio.sleep(1.5)
            await until_caught_up(ex, rec)
        check_trades(ex, read_rows(tmp_path, "trades"))
        check_book(read_rows(tmp_path, "book"))
        assert all(p.suffix == ".gz" for p in tmp_path.rglob("book_*")), "files gzipped on close"
    T.run(t())


def test_dropped_trades_are_backfilled(tmp_path):
    async def t():
        async with exchange() as (ex, port), running_recorder(port, tmp_path) as rec:
            await T.wait_for(lambda: rec.trades.stats["trades"] > 50, what="trades flowing")
            ex.drop_trades = 25
            ex.trade_garbage_next = True
            await T.wait_for(lambda: rec.trades.stats["backfilled"] >= 25, what="backfill")
            await until_caught_up(ex, rec)
            assert rec.trades.stats["lost"] == 0
        rows = read_rows(tmp_path, "trades")
        check_trades(ex, rows)
        assert sum(r["source"] == "rest" for r in rows) >= 25
        assert "trades_backfilled" in (tmp_path / T.SYMBOL / "events.csv").read_text()
    T.run(t())


def test_disconnect_loses_nothing(tmp_path):
    async def t():
        async with exchange() as (ex, port), running_recorder(port, tmp_path) as rec:
            await T.wait_for(lambda: rec.trades.stats["trades"] > 50, what="trades flowing")
            for _ in range(3):
                ex.kill_connections()
                await asyncio.sleep(0.4)
            await until_caught_up(ex, rec)
            assert rec.trades.stats["reconnects"] >= 3 and rec.trades.stats["lost"] == 0
        check_trades(ex, read_rows(tmp_path, "trades"))
        check_book(read_rows(tmp_path, "book"))
    T.run(t())


def test_too_big_gap_is_logged_not_hidden(tmp_path):
    async def t():
        async with exchange() as (ex, port), running_recorder(port, tmp_path, max_backfill=3) as rec:
            await T.wait_for(lambda: rec.trades.stats["trades"] > 20, what="trades flowing")
            ex.drop_trades = 10
            await T.wait_for(lambda: rec.trades.stats["lost"] >= 10, what="loss recorded")
        assert "trades_lost" in (tmp_path / T.SYMBOL / "events.csv").read_text()
    T.run(t())


def test_hourly_rotation_splits_without_gaps_or_duplicates(tmp_path):
    async def t():
        clock = {"n": 0}

        def fast_hours():                       # a new "hour" every 0.3 s
            return "2026-10-19", f"{int(time.monotonic() / 0.3) % 24:02d}"
        async with exchange() as (ex, port), running_recorder(port, tmp_path, hour_key=fast_hours) as rec:
            await asyncio.sleep(2.0)
            await until_caught_up(ex, rec)
        files = list(tmp_path.rglob("trades_*"))
        assert len(files) >= 4, "should have rotated several times"
        assert all(p.name.endswith(".csv.gz") for p in files)
        check_trades(ex, read_rows(tmp_path, "trades"))
    T.run(t())


def test_book_rows_only_while_synced(tmp_path):
    async def t():
        async with exchange() as (ex, port), running_recorder(port, tmp_path) as rec:
            await T.wait_for(lambda: rec.book_file.rows > 5, what="book rows")
            ex.paused = True                    # feed goes silent -> not ready
            await T.wait_for(lambda: not rec.sync.ready, timeout=3, what="not ready")
            n = rec.book_file.rows
            await asyncio.sleep(0.6)
            assert rec.book_file.rows == n, "must not record a stale book"
            ex.paused = False
        assert "STALE" in (tmp_path / T.SYMBOL / "events.csv").read_text() or \
               "book_stale" in (tmp_path / T.SYMBOL / "events.csv").read_text()
    T.run(t())


def test_leftovers_from_crash_are_compressed_and_never_overwritten(tmp_path):
    day = tmp_path / T.SYMBOL / "2026-10-19"
    day.mkdir(parents=True)
    (day / "trades_03.csv").write_text("recv_ms,agg_id\n1,1\n")
    (day / "trades_02.csv.gz.tmp").write_bytes(b"half written")
    assert recorder.compress_leftovers(tmp_path) == 1
    assert not (day / "trades_02.csv.gz.tmp").exists()
    with gzip.open(day / "trades_03.csv.gz", "rt") as f:
        assert f.read() == "recv_ms,agg_id\n1,1\n"
    # a restart in the same hour must start a new part, not clobber the old one
    w = recorder.HourlyCsv(tmp_path / T.SYMBOL, "trades", ["a"], lambda: ("2026-10-19", "03"))
    w.write([1])
    assert w.path.name == "trades_03-2.csv"
    w.close(); w.join()
    assert (day / "trades_03.csv.gz").exists() and (day / "trades_03-2.csv.gz").exists()
