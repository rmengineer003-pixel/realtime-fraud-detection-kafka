"""Kafka producer and consumer wrappers (kafka-python). Broker settings come from environment variables."""
from __future__ import annotations

import logging
import os
import signal

from .models import Transaction
from .rules import FraudScorer
from .synthetic import stream

log = logging.getLogger("fraud_stream")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TXN_TOPIC = os.getenv("TXN_TOPIC", "transactions")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "fraud-alerts")


def produce(n: int = 5000) -> None:
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", linger_ms=20)
    for t in stream(n=n):
        p.send(TXN_TOPIC, key=t.card_id.encode(), value=t.to_json())  # key by card keeps per-card ordering
    p.flush()
    log.info("produced %d transactions to %s", n, TXN_TOPIC)


def consume() -> None:
    from kafka import KafkaConsumer, KafkaProducer
    consumer = KafkaConsumer(TXN_TOPIC, bootstrap_servers=BOOTSTRAP, group_id="fraud-scorer",
                             enable_auto_commit=False, auto_offset_reset="earliest")
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all")
    scorer, running = FraudScorer(), True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while running:
        batch = consumer.poll(timeout_ms=1000)
        for records in batch.values():
            for rec in records:
                try:
                    alert = scorer.score(Transaction.from_json(rec.value))
                except (ValueError, TypeError) as exc:  # poison message: log and skip
                    log.warning("skipping bad record at offset %s: %s", rec.offset, exc)
                    continue
                if alert:
                    producer.send(ALERT_TOPIC, key=alert.card_id.encode(), value=alert.to_json())
        producer.flush()
        consumer.commit()  # commit only after alerts are flushed (at-least-once)
    consumer.close()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    {"produce": produce, "consume": consume}[sys.argv[1]]()
