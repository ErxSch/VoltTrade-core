# VoltTrade Core v0.5-consensus

Multi-exchange early-momentum prototype.

## Crypto engine
- Binance: discovery layer (USDT 24h universe + direct 1m kline history for early acceleration/volume)
- Kraken: EUR execution/validation layer (live ticker, spread, EUR liquidity, OHLC structure)
- Coinbase: cross-market direction confirmation via public Advanced Trade ticker feed
- Breakout age: rejects stale breakouts and prioritizes fresh ignition
- Consensus score: separate from the single-exchange technical score
- ENTRY requires both technical quality and multi-exchange confirmation

## UI
- ET / EN
- Crypto + Stocks
- Stocks split into USA / EUROPE placeholders for separate future feeds
- Candlestick + volume detail chart, support/resistance, exchange comparison
- Small legal/risk footer

## Railway
Deploy from GitHub. Dockerfile binds to `$PORT` (default 8080).

## Notes
This prototype does not place trades. Exchange availability and API reachability can vary by hosting region. If Binance or Coinbase is unreachable, the source health indicator turns red and ENTRY becomes harder rather than inventing data.
