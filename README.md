# VoltTrade Core v0.6 — Pre-Momentum

VoltTrade v0.6 shifts the scanner from **detecting a move** to **detecting conditions that can precede a move**.

## Crypto architecture

- **Binance** — leading activity discovery: 1m candles, 5m volume acceleration, trade-count acceleration, taker-buy ratio, compression and lead/lag.
- **Kraken** — EUR price, spread, liquidity and market structure validation.
- **Coinbase** — independent cross-market confirmation.
- **Catalyst Engine** — configurable RSS/Atom feeds, freshness, source tier, positive/negative catalyst classification and token matching.

## New leading metrics

- Pre-Momentum Score (0–100)
- 5m volume acceleration
- 5m trade-count acceleration
- 5m taker-buy ratio
- volatility compression
- pressure below resistance
- Binance → Kraken lead/lag
- catalyst score + age + source
- negative catalyst veto/penalty

## Status flow

`PRE-MOMENTUM → SETUP → ENTRY WINDOW → MOMENTUM → LATE`

PRE-MOMENTUM is intentionally allowed while price is still flat. A large already-realized move is not rewarded as an early signal.

## Run locally

```bash
python -m pip install -r requirements.txt
python app/server.py
```

Open http://localhost:8080

## Tests

```bash
python -m unittest discover -s tests -v
```

The included tests validate that:
1. rising activity + flat price + catalyst produces a high pre-momentum score;
2. a quiet market without activity stays low;
3. a negative catalyst penalizes the score;
4. an already-moving price is flagged as late-risk rather than treated as early.

## Deployment

The repository contains `Dockerfile` and `railway.json` for Railway deployment.

## Important

No model can know with certainty that a price will rise before the market reacts. Pre-Momentum is a probabilistic leading-signal engine and must be validated against logged outcomes before position sizes are increased.
