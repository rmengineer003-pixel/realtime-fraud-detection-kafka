"""Stateful, per-card fraud rules. Pure Python so the logic can be unit tested without a broker.

State per card is bounded (a deque of recent timestamps and running amount statistics), which
mirrors how you would keep keyed state in Kafka Streams, Flink, or Spark Structured Streaming."""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import ClassVar

from .models import Alert, Transaction


@dataclass
class RuleConfig:
    velocity_window_s: int = 600
    velocity_max_txns: int = 3
    amount_zscore: float = 4.0
    amount_min_history: int = 5
    geo_window_s: int = 3600
    high_risk_mcc: tuple[str, ...] = ("GIFT_CARDS", "CRYPTO", "WIRE")
    high_risk_amount: float = 900.0
    alert_threshold: float = 0.5


@dataclass
class CardState:
    recent: deque = field(default_factory=deque)
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0
    last_country: str | None = None
    last_ts: float | None = None

    def update_amount(self, x: float) -> None:  # Welford's online algorithm
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else 0.0


class FraudScorer:
    WEIGHTS: ClassVar[dict[str, float]] = {
        "velocity": 0.6, "amount_outlier": 0.6, "geo_jump": 0.6, "high_risk_merchant": 0.5,
    }

    def __init__(self, cfg: RuleConfig | None = None):
        self.cfg = cfg or RuleConfig()
        self.state: dict[str, CardState] = defaultdict(CardState)

    def score(self, t: Transaction) -> Alert | None:
        c, s = self.cfg, self.state[t.card_id]
        reasons: list[str] = []

        while s.recent and t.ts - s.recent[0] > c.velocity_window_s:
            s.recent.popleft()
        s.recent.append(t.ts)
        if len(s.recent) > c.velocity_max_txns:
            reasons.append("velocity")

        if s.n >= c.amount_min_history and s.std > 0 and (t.amount - s.mean) / s.std > c.amount_zscore:
            reasons.append("amount_outlier")

        if (s.last_country and s.last_country != t.country and s.last_ts is not None
                and t.ts - s.last_ts < c.geo_window_s):
            reasons.append("geo_jump")

        if t.merchant_category in c.high_risk_mcc and t.amount >= c.high_risk_amount:
            reasons.append("high_risk_merchant")

        # update state after scoring so a transaction never explains itself
        if "amount_outlier" not in reasons:
            s.update_amount(t.amount)
        s.last_country, s.last_ts = t.country, t.ts

        score = min(1.0, sum(self.WEIGHTS[r] for r in reasons))
        if score >= c.alert_threshold:
            return Alert(t.txn_id, t.card_id, t.ts, round(score, 2), reasons)
        return None
