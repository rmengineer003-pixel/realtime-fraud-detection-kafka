"""Replay the synthetic stream through the scorer without Kafka and report precision/recall."""
from __future__ import annotations

import json
import sys
from collections import Counter

from .rules import FraudScorer
from .synthetic import stream


def evaluate(n: int = 20000, seed: int = 11) -> dict:
    scorer = FraudScorer()
    tp = fp = fn = tn = 0
    reasons: Counter = Counter()
    for t in stream(n=n, seed=seed):
        alert = scorer.score(t)
        if alert:
            reasons.update(alert.reasons)
        if alert and t.label_fraud:
            tp += 1
        elif alert:
            fp += 1
        elif t.label_fraud:
            fn += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"transactions": tp + fp + fn + tn, "alerts": tp + fp, "true_positives": tp, "false_positives": fp,
            "missed": fn, "precision": round(precision, 3), "recall": round(recall, 3),
            "alerts_by_reason": dict(reasons)}


if __name__ == "__main__":
    print(json.dumps(evaluate(int(sys.argv[1]) if len(sys.argv) > 1 else 20000), indent=2))
