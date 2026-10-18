"""
recorder.py - record the live order book and every trade to disk, for days.

    python recorder.py BTCUSDT ETHUSDT
    caffeinate -i python recorder.py BTCUSDT      # macOS: keep the Mac awake while recording

What it saves (per symbol, one file per UTC hour, gzipped when the hour ends):

    data/BTCUSDT/2026-10-19/book_03.csv.gz     top N levels, every 0.25 s
    data/BTCUSDT/2026-10-19/trades_03.csv.gz   every aggregated trade
    data/BTCUSDT/events.csv                    sync changes, gaps, backfills

book columns:   recv_ms, event_ms, update_id, bid_px_1, bid_qty_1, ... bid_px_N, bid_qty_N,
                ask_px_1, ask_qty_1, ... ask_px_N, ask_qty_N
trades columns: recv_ms, trade_ms, agg_id, price, qty, aggressor (buy/sell), source (ws/rest)

  * recv_ms is your clock, event_ms / trade_ms are Binance's (all ms since 1970, UTC).
  * Book rows are written only while the book is synced and fresh. Missing time = no
    trustworthy book, and events.csv says why.
  * Trades are checked by ID. If any are missed (disconnect, dropped message) they are
    fetched back from REST, so the trade record has no holes. Those rows have
    source=rest and an empty recv_ms.
  * The current hour is a plain .csv (so a crash loses at most ~1 s). Leftover .csv files
    from a previous run are compressed on startup.

Options: --depth 10  --interval 0.25  --out data  --ws-base ...  --rest-base ...
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import gzip
import logging
import os
import random
import shutil
import signal
import threading
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Optional

import aiohttp
import websockets

import lob

log = logging.getLogger("recorder")


# =========================================================================== files

def utc_hour_key() -> tuple[str, str]:
    g = time.gmtime()
    return time.strftime("%Y-%m-%d", g), time.strftime("%H", g)


def compress(path: Path) -> None:
    """foo.csv -> foo.csv.gz, atomically (a crash never leaves a broken .gz)."""
    gz = path.with_name(path.name + ".gz")
    tmp = gz.with_name(gz.name + ".tmp")
    with open(path, "rb") as src, gzip.open(tmp, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    os.replace(tmp, gz)
    path.unlink()


def _safe_compress(path: Path) -> None:
    try:
        compress(path)
    except Exception:
        log.exception("could not compress %s (left as .csv)", path)


def compress_leftovers(root: Path) -> int:
    """Compress .csv files left by a previous run; remove half-written .tmp files."""
    if not root.exists():
        return 0
    for tmp in root.rglob("*.gz.tmp"):
        tmp.unlink(missing_ok=True)
    n = 0
    for p in root.rglob("*.csv"):
        if p.name == "events.csv":
            continue
        _safe_compress(p)
        n += 1
    return n


class HourlyCsv:
    """Writes rows to <root>/<day>/<kind>_<HH>.csv and gzips each file when its hour ends."""

    def __init__(self, root: Path, kind: str, header: list[str],
                 hour_key: Callable[[], tuple[str, str]] = utc_hour_key) -> None:
        self.root, self.kind, self.header, self.hour_key = root, kind, header, hour_key
        self.rows = 0
        self.path: Optional[Path] = None
        self._f = None
        self._w = None
        self._key = None
        self._threads: list[threading.Thread] = []

    def write(self, row: list) -> None:
        key = self.hour_key()
        if key != self._key:
            self._open(key)
        self._w.writerow(row)
        self.rows += 1

    def _open(self, key: tuple[str, str]) -> None:
        self.close()
        day, hour = key
        folder = self.root / day
        folder.mkdir(parents=True, exist_ok=True)
        n = 1
        while True:                      # never overwrite: restarts in the same hour get -2, -3...
            stem = f"{self.kind}_{hour}" + ("" if n == 1 else f"-{n}")
            if not (folder / f"{stem}.csv").exists() and not (folder / f"{stem}.csv.gz").exists():
                break
            n += 1
        self.path = folder / f"{stem}.csv"
        self._f = open(self.path, "w", newline="", buffering=1 << 16)
        self._w = csv.writer(self._f)
        self._w.writerow(self.header)
        self._key = key

    def flush(self) -> None:
        if self._f:
            self._f.flush()

    def close(self) -> None:
        if self._f:
            self._f.close()
            t = threading.Thread(target=_safe_compress, args=(self.path,), daemon=False)
            t.start()
            self._threads.append(t)
            self._f = self._w = self._key = None
        self._threads = [t for t in self._threads if t.is_alive()]

    def join(self) -> None:
        for t in self._threads:
            t.join()


class EventLog:
    """Small append-only log of what happened: state changes, gaps, backfills."""

    def __init__(self, folder: Path) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "events.csv"
        new = not path.exists()
        self._f = open(path, "a", newline="")
        self._w = csv.writer(self._f)
        if new:
            self._w.writerow(["utc_time", "event", "detail"])
        self.count = 0

    def write(self, event: str, detail: str = "") -> None:
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}Z"
        self._w.writerow([ts, event, detail])
        self._f.flush()
        self.count += 1

    def close(self) -> None:
        self._f.close()


def _num(d: Decimal) -> str:
    """Shortest exact text for a price/qty: 81493.03000000 -> 81493.03, 100.00 -> 100."""
    return f"{d.normalize():f}"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


# =========================================================================== book

class BookRecorder:
    """Samples the synced book every `interval` seconds and writes the top `depth` levels."""

    @staticmethod
    def header(depth: int) -> list[str]:
        cols = ["recv_ms", "event_ms", "update_id"]
        for side in ("bid", "ask"):
            for i in range(1, depth + 1):
                cols += [f"{side}_px_{i}", f"{side}_qty_{i}"]
        return cols

    def __init__(self, sync: lob.BookSync, writer: HourlyCsv, events: EventLog,
                 depth: int = 10, interval: float = 0.25) -> None:
        self.sync, self.writer, self.events = sync, writer, events
        self.depth, self.interval = depth, interval
        self.skipped = 0
        self._state = None
        self._counts: dict[str, int] = {}

    def _track_health(self) -> None:
        s = self.sync
        state = "LIVE" if s.ready else ("STALE" if s.state is lob.State.LIVE else s.state.name)
        if state != self._state:
            self.events.write("book_state", state)
            self._state = state
        for k in ("resyncs", "reconnects", "stale", "parse_errors"):
            v = s.stats.get(k, 0)
            if v != self._counts.get(k, 0):
                self.events.write(f"book_{k}", str(v))
                self._counts[k] = v

    def sample(self) -> bool:
        self._track_health()
        if not self.sync.ready:
            self.skipped += 1
            return False
        bids, asks = self.sync.book.top(self.depth)
        row: list = [_now_ms(), self.sync.last_event_time or "", self.sync.book.last_update_id]
        for levels in (bids, asks):
            for i in range(self.depth):
                if i < len(levels):
                    row += [_num(levels[i][0]), _num(levels[i][1])]
                else:
                    row += ["", ""]
        self.writer.write(row)
        return True

    async def run(self) -> None:
        nxt = time.monotonic()
        while True:
            nxt += self.interval
            delay = nxt - time.monotonic()
            if delay < -5 * self.interval:       # fell far behind (e.g. Mac slept): don't burst
                nxt = time.monotonic()
                delay = 0
            await asyncio.sleep(max(0.0, delay))
            try:
                self.sample()
            except Exception:
                log.exception("[%s] book sample failed", self.sync.symbol)


# =========================================================================== trades

def _valid_trade(m, symbol: Optional[str]) -> bool:
    if not isinstance(m, dict):
        return False
    if symbol is not None and (m.get("e") != "aggTrade" or m.get("s") != symbol):
        return False
    return (isinstance(m.get("a"), int) and isinstance(m.get("T"), int)
            and isinstance(m.get("p"), str) and isinstance(m.get("q"), str)
            and isinstance(m.get("m"), bool))


class TradeRecorder:
    """Streams <symbol>@aggTrade and writes every trade, filling any ID gaps from REST."""

    HEADER = ["recv_ms", "trade_ms", "agg_id", "price", "qty", "aggressor", "source"]
    AGG_TRADES_WEIGHT = 4      # Binance spot GET /api/v3/aggTrades weight (check docs if it changes)

    def __init__(self, cfg: lob.Config, rest: lob.RateLimitedRest, writer: HourlyCsv,
                 events: EventLog, max_backfill: int = 200_000, stale_after: float = 60.0) -> None:
        self.cfg, self.rest, self.writer, self.events = cfg, rest, writer, events
        self.symbol = cfg.symbol.upper()
        self.max_backfill = max_backfill
        self.stale_after = stale_after
        self.last_id: Optional[int] = None
        self.last_msg = 0.0
        self.stats = dict(trades=0, backfilled=0, gaps=0, lost=0, duplicates=0,
                          reconnects=0, rotations=0, parse_errors=0)

    async def run(self) -> None:
        backoff = self.cfg.backoff_base
        while True:
            self._got_data = False
            try:
                await self._connection()
                self.stats["rotations"] += 1
                continue
            except asyncio.CancelledError:
                raise
            except lob.RateLimited as e:
                await asyncio.sleep(e.retry_after)
                continue
            except (lob.StaleFeed, websockets.exceptions.WebSocketException, OSError,
                    aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.stats["reconnects"] += 1
                log.warning("[%s] trade stream lost: %r", self.symbol, e)
                self.events.write("trades_reconnect", repr(e)[:200])
            except Exception:
                self.stats["reconnects"] += 1
                log.exception("[%s] trade stream unexpected error", self.symbol)
                self.events.write("trades_reconnect", "unexpected error, see recorder.log")
            if self._got_data:
                backoff = self.cfg.backoff_base
            await asyncio.sleep(backoff * (1 + random.random()))
            backoff = min(backoff * 2, self.cfg.backoff_max)

    async def _connection(self) -> None:
        url = f"{self.cfg.ws_base.rstrip('/')}/ws/{self.symbol.lower()}@aggTrade"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=2,
                                      open_timeout=10, max_queue=4096) as ws:
            opened = time.monotonic()
            while True:
                if time.monotonic() - opened > self.cfg.rotate_after:
                    return
                try:
                    raw = await asyncio.wait_for(ws.recv(), self.stale_after)
                except asyncio.TimeoutError:
                    raise lob.StaleFeed(f"no trades for {self.stale_after}s") from None
                recv = _now_ms()
                self.last_msg = time.monotonic()
                try:
                    m = lob._loads(raw)
                except ValueError:
                    self.stats["parse_errors"] += 1
                    continue          # a lost trade becomes an ID gap, which gets backfilled
                if not _valid_trade(m, self.symbol):
                    continue
                self._got_data = True
                await self._handle(m, recv)

    async def _handle(self, m: dict, recv_ms) -> None:
        a = m["a"]
        if self.last_id is not None:
            if a <= self.last_id:
                self.stats["duplicates"] += 1
                return
            if a > self.last_id + 1:
                await self._backfill(self.last_id + 1, a - 1)
        self._write(m, "ws", recv_ms)

    def _write(self, t: dict, source: str, recv_ms) -> None:
        try:
            price, qty = Decimal(t["p"]), Decimal(t["q"])
        except (InvalidOperation, KeyError, TypeError):
            log.warning("[%s] unusable trade %s skipped", self.symbol, t.get("a"))
            return
        # m = "buyer is maker": the seller crossed the spread, so the aggressor sold
        self.writer.write([recv_ms, t["T"], t["a"], _num(price), _num(qty),
                           "sell" if t["m"] else "buy", source])
        self.last_id = t["a"]
        self.stats["trades"] += 1
        if source == "rest":
            self.stats["backfilled"] += 1

    async def _backfill(self, start: int, end: int) -> None:
        missing = end - start + 1
        self.stats["gaps"] += 1
        if missing > self.max_backfill:
            self._lost(start, end, "too many to backfill")
            return
        log.info("[%s] backfilling %d trades (%d-%d)", self.symbol, missing, start, end)
        nxt, failures = start, 0
        while nxt <= end:
            try:
                batch = await self.rest.get("/api/v3/aggTrades",
                                            {"symbol": self.symbol, "fromId": nxt, "limit": 1000},
                                            self.AGG_TRADES_WEIGHT)
            except lob.RateLimited as e:
                if e.retry_after > 60:
                    break
                await asyncio.sleep(e.retry_after)
                continue
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                failures += 1
                log.warning("[%s] backfill request failed (%r), attempt %d", self.symbol, e, failures)
                if failures >= 3:
                    break
                await asyncio.sleep(failures)
                continue
            before = nxt
            for t in batch if isinstance(batch, list) else []:
                if not _valid_trade(t, None) or t["a"] < nxt:
                    continue
                if t["a"] > end:
                    break
                if t["a"] > nxt:                 # exchange itself has a hole; note it, move on
                    self._lost(nxt, t["a"] - 1, "not returned by REST")
                self._write(t, "rest", "")
                nxt = t["a"] + 1
            if nxt == before:                    # no progress: stop rather than loop forever
                break
        if nxt <= end:
            self._lost(nxt, end, "backfill incomplete")
        else:
            self.events.write("trades_backfilled", f"{start}-{end} ({missing})")

    def _lost(self, start: int, end: int, why: str) -> None:
        n = end - start + 1
        self.stats["lost"] += n
        log.warning("[%s] %d trades lost (%d-%d): %s", self.symbol, n, start, end, why)
        self.events.write("trades_lost", f"{start}-{end} ({n}): {why}")
        self.last_id = max(self.last_id or 0, end)


# =========================================================================== wiring

class SymbolRecorder:
    """Everything recorded for one symbol."""

    def __init__(self, symbol: str, rest: lob.RateLimitedRest, root: Path, depth: int = 10,
                 interval: float = 0.25, cfg_overrides: Optional[dict] = None,
                 hour_key: Callable[[], tuple[str, str]] = utc_hour_key,
                 max_backfill: int = 200_000) -> None:
        cfg = lob.Config(symbol=symbol.upper())
        for k, v in (cfg_overrides or {}).items():
            setattr(cfg, k, v)
        self.symbol = cfg.symbol
        folder = root / self.symbol
        self.events = EventLog(folder)
        self.book_file = HourlyCsv(folder, "book", BookRecorder.header(depth), hour_key)
        self.trade_file = HourlyCsv(folder, "trades", TradeRecorder.HEADER, hour_key)
        self.sync = lob.BookSync(cfg, rest)
        self.book = BookRecorder(self.sync, self.book_file, self.events, depth, interval)
        self.trades = TradeRecorder(cfg, rest, self.trade_file, self.events, max_backfill)
        self.folder = folder

    def tasks(self) -> list[asyncio.Task]:
        return [asyncio.create_task(c) for c in (self.sync.run(), self.book.run(), self.trades.run())]

    def flush(self) -> None:
        self.book_file.flush()
        self.trade_file.flush()

    def close(self) -> None:
        for f in (self.book_file, self.trade_file):
            f.close()
            f.join()
        self.events.close()

    def status(self) -> str:
        size = sum(p.stat().st_size for p in self.folder.rglob("*") if p.is_file())
        t = self.trades.stats
        extra = " ".join(f"{k}={t[k]}" for k in ("backfilled", "lost", "reconnects") if t[k])
        return (f"[{self.symbol}] {'LIVE' if self.sync.ready else self.sync.state.name:<12} "
                f"book rows {self.book_file.rows:,}  trades {t['trades']:,}  "
                f"disk {size / 1e6:,.1f} MB  {extra}").rstrip()


async def _flusher(recs: list[SymbolRecorder]) -> None:
    while True:
        await asyncio.sleep(1)
        for r in recs:
            r.flush()


async def _status(recs: list[SymbolRecorder], every: float) -> None:
    while True:
        await asyncio.sleep(every)
        for r in recs:
            log.info(r.status())


async def main(args) -> None:
    root = Path(args.out)
    root.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(root / "recorder.log")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(fh)

    n = await asyncio.to_thread(compress_leftovers, root)
    if n:
        log.info("compressed %d file(s) left from a previous run", n)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)

    overrides = {k: v for k, v in (("ws_base", args.ws_base), ("rest_base", args.rest_base)) if v}
    async with aiohttp.ClientSession(trust_env=True) as session:
        rest = lob.RateLimitedRest(session, overrides.get("rest_base", lob.Config.rest_base))
        recs = [SymbolRecorder(s, rest, root, args.depth, args.interval, overrides)
                for s in args.symbols]
        tasks = [t for r in recs for t in r.tasks()]
        tasks += [asyncio.create_task(_flusher(recs)), asyncio.create_task(_status(recs, 30))]
        log.info("recording %s to %s/ (Ctrl+C to stop)", ", ".join(r.symbol for r in recs), root)
        await stop.wait()
        log.info("stopping, finishing files...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for r in recs:
            r.close()
            log.info(r.status())
    log.info("all files saved")


def cli() -> None:
    p = argparse.ArgumentParser(description="Record Binance order book + trades to disk")
    p.add_argument("symbols", nargs="*", default=["BTCUSDT"])
    p.add_argument("--depth", type=int, default=10, help="levels per side to save (default 10)")
    p.add_argument("--interval", type=float, default=0.25, help="seconds between book snapshots")
    p.add_argument("--out", default="data", help="output folder (default ./data)")
    p.add_argument("--ws-base", default=None)
    p.add_argument("--rest-base", default=None)
    args = p.parse_args()
    args.symbols = [s.upper() for s in args.symbols]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main(args))


if __name__ == "__main__":
    cli()
