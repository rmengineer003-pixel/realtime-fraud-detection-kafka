from fraud_stream.evaluate import evaluate
from fraud_stream.models import Transaction
from fraud_stream.rules import FraudScorer, RuleConfig


def t(i, ts, amt=50.0, mcc="GROCERY", country="US", card="c1"):
    return Transaction(f"t{i}", card, ts, amt, mcc, country)


def test_velocity_burst_alerts():
    s = FraudScorer(RuleConfig(velocity_max_txns=3, alert_threshold=0.4))
    alerts = [s.score(t(i, 1000 + i * 10)) for i in range(5)]
    assert alerts[:3] == [None, None, None]
    assert alerts[3] and "velocity" in alerts[3].reasons


def test_amount_outlier_needs_history():
    s = FraudScorer(RuleConfig(alert_threshold=0.4))
    assert s.score(t(0, 0, amt=5000)) is None  # no history yet
    s = FraudScorer(RuleConfig(alert_threshold=0.4))
    for i, a in enumerate([40, 45, 50, 55, 60, 48]):
        assert s.score(t(i, i * 3600, amt=a)) is None
    a = s.score(t(99, 99 * 3600, amt=900))
    assert a and a.reasons == ["amount_outlier"]


def test_outlier_does_not_poison_baseline():
    s = FraudScorer()
    for i, a in enumerate([40, 45, 50, 55, 60, 48]):
        s.score(t(i, i * 3600, amt=a))
    before = s.state["c1"].mean
    s.score(t(50, 50 * 3600, amt=5000))
    assert s.state["c1"].mean == before


def test_geo_jump_within_window():
    s = FraudScorer()
    assert s.score(t(0, 0, country="US")) is None
    a = s.score(t(1, 600, country="RO"))
    assert a and "geo_jump" in a.reasons
    assert s.score(t(2, 600 + 7200, country="US")) is None  # outside window


def test_cards_are_isolated():
    s = FraudScorer(RuleConfig(velocity_max_txns=2, alert_threshold=0.4))
    for i in range(6):
        assert s.score(t(i, i, card=f"card{i}")) is None


def test_bad_json_raises():
    import pytest
    with pytest.raises(ValueError):
        Transaction.from_json(b"not json")


def test_replay_quality_floor():
    r = evaluate(n=20000)
    assert r["precision"] >= 0.8 and r["recall"] >= 0.6
