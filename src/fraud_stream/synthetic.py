"""Synthetic card transaction stream with injected fraud patterns (no real card data)."""
from __future__ import annotations

import random
from collections.abc import Iterator

from .models import Transaction

MCC = ["GROCERY", "FUEL", "RESTAURANT", "ONLINE_RETAIL", "TRAVEL", "GIFT_CARDS", "CRYPTO", "WIRE"]
NORMAL_MCC = MCC[:5]


def stream(n: int = 5000, cards: int = 300, seed: int = 11, fraud_rate: float = 0.01,
           start_ts: float = 1_735_689_600.0) -> Iterator[Transaction]:
    rng = random.Random(seed)
    home = {f"card_{i:05d}": rng.choice(["US", "US", "US", "CA", "GB"]) for i in range(cards)}
    typical = {c: rng.uniform(20, 120) for c in home}
    ts = start_ts
    i = 0
    while i < n:
        ts += rng.expovariate(1 / 20)
        card = rng.choice(list(home))
        if rng.random() < fraud_rate:
            pattern = rng.choice(["burst", "big", "geo", "risky"])
            if pattern == "burst":
                for _ in range(7):
                    ts += rng.uniform(5, 40)
                    yield Transaction(f"t{i:07d}", card, ts, round(rng.uniform(10, 60), 2), "ONLINE_RETAIL",
                                      home[card], True)
                    i += 1
                continue
            if pattern == "big":
                amt, mcc, ctry = typical[card] * rng.uniform(15, 30), "ONLINE_RETAIL", home[card]
            elif pattern == "geo":
                amt, mcc, ctry = typical[card], "RESTAURANT", "RO" if home[card] != "RO" else "BR"
            else:
                amt, mcc, ctry = rng.uniform(950, 3000), rng.choice(["GIFT_CARDS", "CRYPTO", "WIRE"]), home[card]
            yield Transaction(f"t{i:07d}", card, ts, round(amt, 2), mcc, ctry, True)
        else:
            amt = max(1.0, rng.gauss(typical[card], typical[card] * 0.3))
            yield Transaction(f"t{i:07d}", card, ts, round(amt, 2), rng.choice(NORMAL_MCC), home[card], False)
        i += 1
