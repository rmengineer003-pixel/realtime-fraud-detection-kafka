from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class Transaction:
    txn_id: str
    card_id: str
    ts: float              # epoch seconds
    amount: float
    merchant_category: str
    country: str
    label_fraud: bool = False  # synthetic ground truth, used only for evaluation

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @staticmethod
    def from_json(raw: bytes | str) -> Transaction:
        d = json.loads(raw)
        return Transaction(**d)


@dataclass
class Alert:
    txn_id: str
    card_id: str
    ts: float
    score: float
    reasons: list[str] = field(default_factory=list)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()
