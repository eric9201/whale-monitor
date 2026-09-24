# WhaleScope

24/7 BTC/ETH public derivatives large-trade monitor.

Initial sources:
- Binance Futures
- Bybit linear perpetuals
- OKX perpetual swaps
- Hyperliquid

## Railway
Start command: `npm start`
Health check: `/health`

The server binds to `0.0.0.0` and `process.env.PORT`.

Important: CEX public trade feeds show aggressor buy/sell flow. They do not reveal trader identity and do not prove whether a participant opened or closed a position.
