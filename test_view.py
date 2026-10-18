"""Tests for view.py: grouping maths, bars, and rendering in every state."""

import asyncio
from decimal import Decimal as D

from rich.console import Console

import lob
import test_lob as T
import view


def text_of(renderable, width=100):
    c = Console(width=width, record=True, color_system=None)
    c.print(renderable)
    return c.export_text()


def test_aggregate_no_group_keeps_levels_and_stops_at_n():
    lv = [(D("10.02"), D(1)), (D("10.01"), D(2)), (D("10.00"), D(3))]
    assert view.aggregate(lv, 2, None, True) == [(D("10.02"), D(1)), (D("10.01"), D(2))]


def test_aggregate_groups_bids_down_and_asks_up():
    bids = [(D("100.9"), D(1)), (D("100.1"), D(2)), (D("99.5"), D(4))]
    assert view.aggregate(bids, 5, D(1), True) == [(D(100), D(3)), (D(99), D(4))]
    asks = [(D("100.1"), D(1)), (D("100.9"), D(2)), (D("101.5"), D(4))]
    assert view.aggregate(asks, 5, D(1), False) == [(D(101), D(3)), (D(102), D(4))]


def test_bar_scales_and_has_fixed_width():
    assert view.bar(D(1), D(1), 10) == "█" * 10
    assert view.bar(D(0), D(1), 10) == " " * 10
    assert len(view.bar(D("0.33"), D(1), 10)) == 10


def test_renders_waiting_state_without_data():
    sync = lob.BookSync(lob.Config(symbol="BTCUSDT"), rest=None)
    out = text_of(view.View(sync, 5, None).render())
    assert "Waiting for order book" in out and "DISCONNECTED" in out


def test_renders_live_book_against_fake_exchange():
    async def t():
        async with T.exchange() as (ex, port), T.client(port) as sync:
            await T.assert_matches(ex, sync)
            v = view.View(sync, 6, None)
            out = text_of(v.render())
            best_bid, best_ask = sync.book.best_bid()[0], sync.book.best_ask()[0]
            assert "LIVE" in out and "spread" in out
            assert f"{best_bid:,.2f}" in out and f"{best_ask:,.2f}" in out
            lines = out.splitlines()
            ask_line = next(i for i, l in enumerate(lines) if f"{best_ask:,.2f}" in l)
            bid_line = next(i for i, l in enumerate(lines) if f"{best_bid:,.2f}" in l)
            assert ask_line < bid_line, "asks must be drawn above bids"
            grouped = text_of(view.View(sync, 6, D(1)).render())
            assert "grouped by 1" in grouped
            sync.state = lob.State.BUFFERING        # mid-resync must still render
            assert "BUFFERING" in text_of(v.render())
    T.run(t())
