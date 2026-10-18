"""
view.py - live full-screen order book in your terminal.

    pip install rich
    python view.py BTCUSDT
    python view.py ETHUSDT --levels 20 --group 0.5

Shows the best asks (red) above the best bids (green), each with size, running
total, and a depth bar, plus spread, mid price and connection health. Built on
lob.py, so the book behind it is the same fully synced, self-healing one.

Options:
  --levels N     price rows per side (default: as many as fit the window)
  --group G      merge prices into buckets of G (e.g. 1 = whole dollars)
  --fps F        screen refreshes per second (default 10)
  --ws-base / --rest-base   alternative endpoints, e.g.
                 wss://data-stream.binance.vision  https://data-api.binance.vision
Logs go to lob.log in this folder. Press Ctrl+C to quit.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Iterable, Optional

import aiohttp
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import lob

BAR_WIDTH = 28
_EIGHTHS = " ▏▎▍▌▋▊▉"
STATE_STYLE = {
    lob.State.LIVE: "bold green",
    lob.State.SYNCING: "yellow",
    lob.State.SNAPSHOT: "yellow",
    lob.State.BUFFERING: "yellow",
    lob.State.DISCONNECTED: "bold red",
}


# =========================================================================== helpers

def aggregate(levels: Iterable[tuple[Decimal, Decimal]], n: int,
              group: Optional[Decimal], is_bid: bool) -> list[tuple[Decimal, Decimal]]:
    """Best-first levels -> up to n (price, qty) rows, merged into `group`-sized buckets.
    Bids round down, asks round up, so a bucket never claims a better price than it has."""
    out: list[tuple[Decimal, Decimal]] = []
    if n <= 0:
        return out
    for price, qty in levels:
        if group:
            mode = ROUND_FLOOR if is_bid else ROUND_CEILING
            price = (price / group).to_integral_value(rounding=mode) * group
        if out and out[-1][0] == price:
            out[-1] = (price, out[-1][1] + qty)
        elif len(out) == n:
            break
        else:
            out.append((price, qty))
    return out


def bar(value: Decimal, maximum: Decimal, width: int = BAR_WIDTH) -> str:
    if maximum <= 0:
        return ""
    cells = float(value / maximum) * width
    full = int(cells)
    part = int((cells - full) * 8)
    return ("█" * full + (_EIGHTHS[part] if part and full < width else "")).ljust(width)


def price_decimals(group: Optional[Decimal], prices: list[Decimal]) -> int:
    if group:
        return max(0, -group.normalize().as_tuple().exponent)
    exps = [-p.normalize().as_tuple().exponent for p in prices[:50]]
    return min(8, max([0, *exps]))


def fmt_qty(q: Decimal) -> str:
    if q >= 1000:
        return f"{q:,.1f}"
    if q >= 1:
        return f"{q:,.3f}"
    return f"{q:.5f}"


class LastWarning(logging.Handler):
    """Keeps the most recent warning/error so it can be shown in the footer."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.message = ""
        self.at = 0.0

    def emit(self, record: logging.LogRecord) -> None:
        self.message = record.getMessage()
        self.at = time.monotonic()


# =========================================================================== rendering

class View:
    def __init__(self, sync: lob.BookSync, levels: Optional[int], group: Optional[Decimal],
                 warnings: Optional[LastWarning] = None) -> None:
        self.sync = sync
        self.levels = levels
        self.group = group
        self.warnings = warnings
        self._prev_mid: Optional[Decimal] = None
        self._mid_dir = ""
        self._rate_t = time.monotonic()
        self._rate_events = 0
        self.rate = 0.0

    def _update_rate(self) -> None:
        now = time.monotonic()
        if now - self._rate_t >= 1.0:
            ev = self.sync.stats["events"]
            self.rate = (ev - self._rate_events) / (now - self._rate_t)
            self._rate_t, self._rate_events = now, ev

    def render(self, height: int = 40):
        s, book = self.sync, self.sync.book
        self._update_rate()
        rows = self.levels or max(3, (height - 9) // 2)

        # ---- header
        header = Text()
        header.append(f" {s.symbol} ", style="bold reverse")
        header.append("  ")
        if s.ready:
            header.append("● LIVE", style="bold green")
        elif s.state is lob.State.LIVE:
            header.append("● STALE", style="bold red")        # synced but no fresh data
        else:
            header.append(f"● {s.state.name}", style=STATE_STYLE.get(s.state, "yellow"))
        mid, spread = book.mid(), book.spread()
        tick_pd = 2
        if mid is not None:
            tick_pd = price_decimals(None, [book.best_bid()[0], book.best_ask()[0]])
            if self._prev_mid is not None and mid != self._prev_mid:
                self._mid_dir = "▲" if mid > self._prev_mid else "▼"
            self._prev_mid = mid
            header.append(f"   mid {mid:,.{tick_pd + 1}f} ", style="bold")
            header.append(self._mid_dir, style="green" if self._mid_dir == "▲" else "red")
            bps = spread / mid * 10_000 if mid else Decimal(0)
            header.append(f"   spread {spread:,.{tick_pd}f} ({bps:.2f} bps)", style="cyan")
        header.append(f"   {self.rate:,.0f} updates/s", style="dim")

        if not book.bids or not book.asks:
            body = Text(f"\n  Waiting for order book… ({s.state.name})\n", style="yellow")
            return Panel(Group(header, body), border_style="yellow", padding=(0, 1))

        bids = aggregate(book.bids.items(), rows, self.group, True)
        asks = aggregate(book.asks.items(), rows, self.group, False)
        biggest = max(q for _, q in bids + asks)
        pd = price_decimals(self.group, [p for p, _ in bids + asks])

        table = Table(box=None, pad_edge=False, show_edge=False, expand=False,
                      header_style="dim", padding=(0, 1))
        table.add_column("price", justify="right", min_width=12)
        table.add_column("size", justify="right", min_width=10)
        table.add_column("total", justify="right", min_width=10)
        table.add_column("", justify="left", width=BAR_WIDTH, no_wrap=True)

        dim = not s.ready
        ask_rows, total = [], Decimal(0)
        for price, qty in asks:
            total += qty
            ask_rows.append((price, qty, total))
        for price, qty, cum in reversed(ask_rows):
            table.add_row(Text(f"{price:,.{pd}f}", style="red" + (" dim" if dim else "")),
                          fmt_qty(qty), Text(fmt_qty(cum), style="dim"),
                          Text(bar(qty, biggest), style="red3" + (" dim" if dim else "")))

        mid_line = Text(f"{spread:,.{tick_pd}f} spread", style="bold cyan")
        table.add_row(Text("─" * 12, style="dim"), Text("─" * 10, style="dim"),
                      Text("─" * 10, style="dim"), mid_line)

        total = Decimal(0)
        for price, qty in bids:
            total += qty
            table.add_row(Text(f"{price:,.{pd}f}", style="green" + (" dim" if dim else "")),
                          fmt_qty(qty), Text(fmt_qty(total), style="dim"),
                          Text(bar(qty, biggest), style="green3" + (" dim" if dim else "")))

        # ---- footer
        footer = Text()
        footer.append(f" update #{book.last_update_id}   levels {len(book.bids)}/{len(book.asks)}", style="dim")
        if self.group:
            footer.append(f"   grouped by {self.group}", style="dim")
        problems = {k: v for k, v in s.stats.items() if v and k not in ("events",)}
        if problems:
            footer.append("   " + " ".join(f"{k}={v}" for k, v in problems.items()), style="yellow")
        if self.warnings and self.warnings.message and time.monotonic() - self.warnings.at < 30:
            footer.append(f"\n last warning: {self.warnings.message[:120]}", style="yellow")
        footer.append("\n Ctrl+C to quit", style="dim")

        border = "green" if s.ready else "yellow"
        return Panel(Group(header, Text(""), table, Text(""), footer),
                     border_style=border, padding=(0, 1), expand=False)


# =========================================================================== app

async def run(args) -> None:
    cfg = lob.Config(symbol=args.symbol.upper())
    if args.ws_base:
        cfg.ws_base = args.ws_base
    if args.rest_base:
        cfg.rest_base = args.rest_base

    warnings = LastWarning()
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.FileHandler("lob.log")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(fh)
    root.addHandler(warnings)

    console = Console()
    async with aiohttp.ClientSession(trust_env=True) as session:
        sync = lob.BookSync(cfg, lob.RateLimitedRest(session, cfg.rest_base))
        task = asyncio.create_task(sync.run())
        view = View(sync, args.levels, Decimal(str(args.group)) if args.group else None, warnings)
        try:
            with Live(console=console, screen=True, auto_refresh=False) as live:
                while True:
                    live.update(view.render(console.size.height), refresh=True)
                    await asyncio.sleep(1 / args.fps)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def main() -> None:
    p = argparse.ArgumentParser(description="Live terminal order book")
    p.add_argument("symbol", nargs="?", default="BTCUSDT")
    p.add_argument("--levels", type=int, default=None, help="rows per side (default: fit window)")
    p.add_argument("--group", type=float, default=None, help="price bucket size, e.g. 1 or 0.5")
    p.add_argument("--fps", type=float, default=10.0, help="screen refreshes per second")
    p.add_argument("--ws-base", default=None)
    p.add_argument("--rest-base", default=None)
    args = p.parse_args()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
