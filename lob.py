"""
lob.py - full-depth Binance spot local limit order book, kept in memory and
designed to keep running through anything the network or exchange throws at it.

    pip install websockets aiohttp sortedcontainers      (orjson optional, faster)
    python lob.py BTCUSDT ETHUSDT

State machine (one per symbol):

    DISCONNECTED -> BUFFERING -> SNAPSHOT -> SYNCING -> LIVE
         ^              ^                                |
         |              +---- gap / crossed / bad data --+   (resync, same connection)
         +----- disconnect / stale feed / 24h rotation --+   (reconnect with backoff)

Sync procedure (Binance spot "how to manage a local order book correctly"):
  1. Open <symbol>@depth@100ms and buffer every event.
  2. GET /api/v3/depth?limit=5000 -> lastUpdateId.
  3. Drop buffered events with u <= lastUpdateId.
  4. First remaining event must have U <= lastUpdateId + 1, otherwise the snapshot
     is older than the buffer: fetch it again.
  5. For every event: u <= local_id -> ignore; U > local_id + 1 -> gap, resync;
     else apply (qty 0 = delete level) and set local_id = u.

What "without crashing" means here:
  * The run() task never dies. Every failure ends in resync or reconnect; unknown
    exceptions are logged with a traceback and treated as a reconnect.
  * The book is never half-updated: an event is fully parsed before anything is
    mutated, and any bad data triggers a resync instead of an exception.
  * Memory is bounded: at most `max_levels` per side, and a capped event buffer.
  * A silent connection (no data for `stale_after` s) is detected and replaced.
  * Connections are rotated before Binance's 24h hard disconnect.
  * REST weight is budgeted and 429/418 + Retry-After are obeyed.
  * Your on_update callback can raise; it is logged and the book keeps going.

Note on depth: a snapshot has at most 5000 levels per side. Levels further out
that never change will not appear in the diff stream, so they can never be known
locally. The book trims each side to `max_levels` so it only holds the region it
can actually keep correct.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import Callable, Optional

import aiohttp
import websockets
from sortedcontainers import SortedDict

try:  # optional, ~5x faster JSON
    import orjson
    _loads = orjson.loads
except ImportError:  # pragma: no cover
    _loads = json.loads

log = logging.getLogger("lob")


# =========================================================================== config

@dataclass
class Config:
    symbol: str = "BTCUSDT"
    ws_base: str = "wss://stream.binance.com:9443"
    rest_base: str = "https://api.binance.com"
    stream_speed: str = "100ms"          # "100ms", or "" for the 1000ms stream
    snapshot_limit: int = 5000           # levels per side in the REST snapshot
    max_levels: int = 5000               # levels kept per side (bounds memory)
    stale_after: float = 10.0            # seconds with no message -> reconnect
    max_buffer: int = 50_000             # queued events before declaring consumer too slow
    rotate_after: float = 23 * 3600      # Binance drops connections at 24h
    min_snapshot_interval: float = 1.0   # never hammer the snapshot endpoint
    max_resyncs_per_connection: int = 5  # then reconnect instead
    backoff_base: float = 1.0
    backoff_max: float = 60.0


# =========================================================================== errors

class ResyncRequired(Exception):
    """Local book can't be trusted. Rebuild it (same connection)."""


class StaleFeed(Exception):
    """Connection is open but nothing is arriving."""


class ConsumerTooSlow(Exception):
    """Events piled up faster than they were processed."""


class StreamEnded(Exception):
    """The WebSocket closed."""


class RateLimited(Exception):
    def __init__(self, status: int, retry_after: float):
        super().__init__(f"HTTP {status}, retry after {retry_after}s")
        self.status = status
        self.retry_after = retry_after


_BAD_DATA = (KeyError, ValueError, TypeError, InvalidOperation, IndexError)


# =========================================================================== order book

def _parse_levels(levels) -> list[tuple[Decimal, Decimal]]:
    out = []
    for price_s, qty_s in levels:
        price, qty = Decimal(price_s), Decimal(qty_s)
        if not price.is_finite() or not qty.is_finite() or price <= 0 or qty < 0:
            raise ValueError(f"bad level {price_s!r} {qty_s!r}")
        out.append((price, qty))
    return out


class OrderBook:
    """Full-depth book. Price -> quantity, sorted, best price at index 0."""

    def __init__(self, max_levels: int = 5000) -> None:
        self.max_levels = max_levels
        self.bids: SortedDict = SortedDict(lambda p: -p)  # highest first
        self.asks: SortedDict = SortedDict()              # lowest first
        self.last_update_id = 0

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0

    def load_snapshot(self, snap: dict) -> None:
        bids = _parse_levels(snap["bids"])   # parse everything before touching state
        asks = _parse_levels(snap["asks"])
        uid = int(snap["lastUpdateId"])
        self.clear()
        self._apply(bids, asks, uid)

    def _apply(self, bids, asks, update_id: int) -> None:
        for side, levels in ((self.bids, bids), (self.asks, asks)):
            for price, qty in levels:
                if qty == 0:
                    side.pop(price, None)
                else:
                    side[price] = qty
            while len(side) > self.max_levels:   # drop the worst (furthest) level
                side.popitem(-1)
        self.last_update_id = update_id

    # ---- reads (safe: asyncio is single-threaded, nothing changes mid-call)

    def best_bid(self) -> Optional[tuple[Decimal, Decimal]]:
        return self.bids.peekitem(0) if self.bids else None

    def best_ask(self) -> Optional[tuple[Decimal, Decimal]]:
        return self.asks.peekitem(0) if self.asks else None

    def mid(self) -> Optional[Decimal]:
        bb, ba = self.best_bid(), self.best_ask()
        return (bb[0] + ba[0]) / 2 if bb and ba else None

    def spread(self) -> Optional[Decimal]:
        bb, ba = self.best_bid(), self.best_ask()
        return ba[0] - bb[0] if bb and ba else None

    def is_crossed(self) -> bool:
        bb, ba = self.best_bid(), self.best_ask()
        return bool(bb and ba and bb[0] >= ba[0])

    def top(self, n: int = 10) -> tuple[list, list]:
        """Copies of the best n levels on each side: ([(price, qty)...], [...])."""
        return list(self.bids.items()[:n]), list(self.asks.items()[:n])

    def depth_within(self, pct: Decimal) -> tuple[Decimal, Decimal]:
        """Total bid / ask quantity within pct (e.g. Decimal('0.01') = 1%) of mid."""
        m = self.mid()
        if m is None:
            return Decimal(0), Decimal(0)
        lo, hi = m * (1 - pct), m * (1 + pct)
        bq = sum((q for p, q in self.bids.items() if p >= lo), Decimal(0)) if self.bids else Decimal(0)
        aq = sum((q for p, q in self.asks.items() if p <= hi), Decimal(0)) if self.asks else Decimal(0)
        return bq, aq


# =========================================================================== REST + rate limits

def _depth_weight(limit: int) -> int:
    # Binance spot GET /api/v3/depth weight by limit (check docs if it changes).
    if limit <= 100:
        return 5
    if limit <= 500:
        return 25
    if limit <= 1000:
        return 50
    return 250


class RateLimitedRest:
    """
    One per IP; share it between all symbols. Binance limits request weight per
    IP per minute (spot: 6000 at time of writing, see /api/v3/exchangeInfo).
      * Spends at most `safety` x limit per minute, waiting for the next minute
        rather than going over.
      * Trusts the server's X-MBX-USED-WEIGHT-1M header over its own count.
      * On 429 / 418 blocks EVERY caller until Retry-After passes. Retrying
        through a 429 is how an IP gets banned (418).
    """

    def __init__(self, session: aiohttp.ClientSession, base_url: str,
                 weight_limit: int = 6000, safety: float = 0.8) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.budget = int(weight_limit * safety)
        self.used = 0
        self.window = self._minute()
        self.blocked_until = 0.0
        self._lock = asyncio.Lock()

    @staticmethod
    def _minute() -> int:
        return int(time.time() // 60)

    async def _reserve(self, weight: int) -> None:
        if self._minute() != self.window:
            self.window, self.used = self._minute(), 0
        if self.used + weight > self.budget:
            wait = 60 - (time.time() % 60) + 0.25
            log.warning("REST weight %d/%d used, waiting %.1fs", self.used, self.budget, wait)
            await asyncio.sleep(wait)
            self.window, self.used = self._minute(), 0
        self.used += weight

    async def get(self, path: str, params: dict, weight: int):
        async with self._lock:
            pause = self.blocked_until - time.monotonic()
            if pause > 0:
                raise RateLimited(429, pause)
            await self._reserve(weight)
            async with self.session.get(self.base_url + path, params=params,
                                        timeout=aiohttp.ClientTimeout(total=10)) as r:
                with contextlib.suppress(TypeError, ValueError):
                    self.used = max(self.used, int(r.headers.get("X-MBX-USED-WEIGHT-1M")))
                if r.status in (418, 429):
                    try:
                        retry = float(r.headers.get("Retry-After", 60))
                    except ValueError:
                        retry = 60.0
                    self.blocked_until = time.monotonic() + retry
                    log.log(logging.ERROR if r.status == 418 else logging.WARNING,
                            "HTTP %d from Binance (%s), pausing REST for %.0fs",
                            r.status, "IP BANNED" if r.status == 418 else "rate limited", retry)
                    raise RateLimited(r.status, retry)
                r.raise_for_status()
                return await r.json(loads=_loads)

    async def depth(self, symbol: str, limit: int) -> dict:
        return await self.get("/api/v3/depth", {"symbol": symbol, "limit": limit},
                              _depth_weight(limit))


# =========================================================================== state machine

class State(Enum):
    DISCONNECTED = auto()
    BUFFERING = auto()
    SNAPSHOT = auto()
    SYNCING = auto()
    LIVE = auto()


class _Closed:
    """Sentinel the reader leaves on the queue when it stops."""
    __slots__ = ("exc",)

    def __init__(self, exc: Optional[BaseException]) -> None:
        self.exc = exc


def _valid_event(m) -> bool:
    return (isinstance(m, dict) and m.get("e") == "depthUpdate"
            and isinstance(m.get("U"), int) and isinstance(m.get("u"), int)
            and m["U"] <= m["u"]
            and isinstance(m.get("b"), list) and isinstance(m.get("a"), list))


class BookSync:
    MAX_SNAPSHOT_ATTEMPTS = 5

    def __init__(self, cfg: Config, rest: RateLimitedRest,
                 on_update: Optional[Callable[["BookSync"], None]] = None) -> None:
        self.cfg = cfg
        self.symbol = cfg.symbol.upper()
        self.rest = rest
        self.on_update = on_update
        self.book = OrderBook(cfg.max_levels)
        self.state = State.DISCONNECTED
        self.last_msg = 0.0
        self._last_snapshot = 0.0
        self.stats = dict(events=0, resyncs=0, reconnects=0, rotations=0,
                          stale=0, parse_errors=0, ignored=0, callback_errors=0)

    # ---- public

    @property
    def ready(self) -> bool:
        """True only while the book is synced AND data is fresh. Gate all reads on this."""
        return (self.state is State.LIVE
                and time.monotonic() - self.last_msg < self.cfg.stale_after)

    async def run(self) -> None:
        """Runs until cancelled. Never returns, never raises (except CancelledError)."""
        backoff = self.cfg.backoff_base
        while True:
            self._was_live = False
            try:
                await self._connection()
                self.stats["rotations"] += 1
                continue                                   # planned rotation: reconnect now
            except asyncio.CancelledError:
                raise
            except RateLimited as e:
                self.stats["reconnects"] += 1
                await self._sleep_logged(e.retry_after, f"rate limited: {e}")
                continue
            except (StaleFeed, ConsumerTooSlow, StreamEnded, ResyncRequired,
                    websockets.exceptions.WebSocketException, OSError,
                    aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.stats["reconnects"] += 1
                log.warning("[%s] connection lost: %r", self.symbol, e)
            except Exception:                              # bug or surprise: log it, keep going
                self.stats["reconnects"] += 1
                log.exception("[%s] unexpected error, reconnecting", self.symbol)
            finally:
                self._set_state(State.DISCONNECTED)
            if self._was_live:
                backoff = self.cfg.backoff_base
            delay = backoff * (1 + random.random())        # jitter avoids reconnect storms
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, self.cfg.backoff_max)

    # ---- internals

    def _set_state(self, s: State) -> None:
        if s is not self.state:
            log.info("[%s] %s -> %s", self.symbol, self.state.name, s.name)
            self.state = s

    async def _sleep_logged(self, seconds: float, why: str) -> None:
        log.warning("[%s] %s; waiting %.1fs", self.symbol, why, seconds)
        await asyncio.sleep(seconds)

    async def _connection(self) -> None:
        stream = f"{self.symbol.lower()}@depth" + (f"@{self.cfg.stream_speed}" if self.cfg.stream_speed else "")
        url = f"{self.cfg.ws_base.rstrip('/')}/ws/{stream}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                      close_timeout=2, open_timeout=10) as ws:
            self.last_msg = time.monotonic()
            queue: asyncio.Queue = asyncio.Queue()
            reader = asyncio.create_task(self._reader(ws, queue))
            opened = time.monotonic()
            resyncs_here = 0
            try:
                while True:
                    self.book.clear()
                    self._set_state(State.BUFFERING)
                    try:
                        await self._sync(queue)
                        self._set_state(State.LIVE)
                        self._was_live = True
                        applied = 0
                        while True:
                            if time.monotonic() - opened > self.cfg.rotate_after:
                                log.info("[%s] rotating connection before 24h limit", self.symbol)
                                return
                            self._apply_event(await self._next(queue))
                            applied += 1
                            if applied == 1000:
                                resyncs_here = 0       # healthy again
                            self._notify()
                    except ResyncRequired as e:
                        self.stats["resyncs"] += 1
                        resyncs_here += 1
                        log.warning("[%s] resync #%d: %s", self.symbol, self.stats["resyncs"], e)
                        if resyncs_here > self.cfg.max_resyncs_per_connection:
                            raise ResyncRequired("too many resyncs on one connection") from e
                        # keep the same connection; events since the failure are already queued
            finally:
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await reader

    async def _reader(self, ws, queue: asyncio.Queue) -> None:
        exc: Optional[BaseException] = None
        try:
            async for raw in ws:
                self.last_msg = time.monotonic()
                try:
                    msg = _loads(raw)
                except ValueError:
                    # A lost depth event becomes a sequence gap, which the
                    # consumer detects. Nothing else to do here.
                    self.stats["parse_errors"] += 1
                    log.warning("[%s] unparseable message skipped", self.symbol)
                    continue
                if not _valid_event(msg) or msg.get("s") != self.symbol:
                    self.stats["ignored"] += 1
                    continue
                if queue.qsize() >= self.cfg.max_buffer:
                    exc = ConsumerTooSlow(f"{queue.qsize()} events queued")
                    return
                queue.put_nowait(msg)
            exc = StreamEnded("server closed the stream")
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            exc = e
        finally:
            queue.put_nowait(_Closed(exc))     # unbounded queue: this never fails

    async def _next(self, queue: asyncio.Queue) -> dict:
        try:
            item = await asyncio.wait_for(queue.get(), self.cfg.stale_after)
        except asyncio.TimeoutError:
            self.stats["stale"] += 1
            raise StaleFeed(f"no data for {self.cfg.stale_after}s") from None
        if isinstance(item, _Closed):
            raise item.exc or StreamEnded("stream closed")
        return item

    async def _sync(self, queue: asyncio.Queue) -> None:
        buffered = [await self._next(queue)]           # make sure events are flowing
        for attempt in range(1, self.MAX_SNAPSHOT_ATTEMPTS + 1):
            wait = self._last_snapshot + self.cfg.min_snapshot_interval - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._set_state(State.SNAPSHOT)
            try:
                snap = await self.rest.depth(self.symbol, self.cfg.snapshot_limit)
            except RateLimited as e:
                if e.retry_after > 30:
                    raise                               # long ban: disconnect and wait it out
                await self._sleep_logged(e.retry_after, "snapshot rate limited")
                continue
            finally:
                self._last_snapshot = time.monotonic()

            while not queue.empty():
                buffered.append(await self._next(queue))

            try:
                self.book.load_snapshot(snap)
            except _BAD_DATA as e:
                raise ResyncRequired(f"malformed snapshot: {e!r}") from e
            sid = self.book.last_update_id
            buffered = [ev for ev in buffered if ev["u"] > sid]

            if buffered and buffered[0]["U"] > sid + 1:
                log.info("[%s] snapshot %d older than buffer (U=%d), refetching (%d/%d)",
                         self.symbol, sid, buffered[0]["U"], attempt, self.MAX_SNAPSHOT_ATTEMPTS)
                continue

            self._set_state(State.SYNCING)
            for ev in buffered:
                self._apply_event(ev)
            return
        raise ResyncRequired("could not line up snapshot with stream")

    def _apply_event(self, e: dict) -> None:
        local = self.book.last_update_id
        if e["u"] <= local:
            return                                      # already in the book
        if e["U"] > local + 1:
            raise ResyncRequired(f"gap: local={local}, event U={e['U']}")
        try:
            bids, asks = _parse_levels(e["b"]), _parse_levels(e["a"])
        except _BAD_DATA as err:
            raise ResyncRequired(f"malformed event {e['u']}: {err!r}") from err
        self.book._apply(bids, asks, e["u"])            # all-or-nothing: parsed first
        self.stats["events"] += 1
        if self.book.is_crossed():
            raise ResyncRequired(f"crossed book at {e['u']}: "
                                 f"{self.book.best_bid()} >= {self.book.best_ask()}")

    def _notify(self) -> None:
        if self.on_update is None:
            return
        try:
            self.on_update(self)
        except Exception:
            self.stats["callback_errors"] += 1
            log.exception("[%s] on_update callback raised (book unaffected)", self.symbol)


# =========================================================================== demo

async def _printer(syncs: list[BookSync]) -> None:
    while True:
        await asyncio.sleep(2)
        for s in syncs:
            if not s.ready:
                log.info("[%s] not ready (%s)", s.symbol, s.state.name)
                continue
            (bp, bq), (ap, aq) = s.book.best_bid(), s.book.best_ask()
            log.info("[%s] id=%d bid %s x %s | ask %s x %s | spread %s | levels %d/%d | %s",
                     s.symbol, s.book.last_update_id, bp, bq, ap, aq, s.book.spread(),
                     len(s.book.bids), len(s.book.asks),
                     " ".join(f"{k}={v}" for k, v in s.stats.items() if v))


async def main(symbols: list[str]) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):  # Windows
            loop.add_signal_handler(sig, stop.set)

    async with aiohttp.ClientSession(trust_env=True) as session:
        rest = RateLimitedRest(session, Config.rest_base)
        syncs = [BookSync(Config(symbol=s), rest) for s in symbols]
        tasks = [asyncio.create_task(s.run(), name=s.symbol) for s in syncs]
        tasks.append(asyncio.create_task(_printer(syncs)))
        await stop.wait()
        log.info("shutting down")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main([s.upper() for s in sys.argv[1:]] or ["BTCUSDT"]))
