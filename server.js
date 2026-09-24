import express from "express";
import WebSocket, { WebSocketServer } from "ws";

const app = express();
const PORT = process.env.PORT || 3000;
const clients = new Set();
const trades = [];
const seen = new Set();
const health = {};
const LIMIT = 30000;

function publish(x) {
  const key = `${x.exchange}:${x.id}`;
  if (seen.has(key)) return;
  seen.add(key);
  if (seen.size > 60000) seen.clear();
  trades.push(x);
  if (trades.length > LIMIT) trades.shift();
  const msg = JSON.stringify({ type: "trade", data: x });
  for (const c of clients) if (c.readyState === WebSocket.OPEN) c.send(msg);
}

function normalize(exchange, symbol, side, price, qty, id, ts, extra={}) {
  price = Number(price); qty = Number(qty);
  const asset = String(symbol).toUpperCase().includes("ETH") ? "ETH" : "BTC";
  const usd = price * qty;
  if (!Number.isFinite(usd) || usd <= 0) return;
  publish({exchange, asset, side, price, qty, usd, id:String(id), ts:Number(ts), ...extra});
}

function reconnect(name, make) {
  let delay = 1000;
  const connect = () => {
    health[name] = "connecting";
    try {
      const ws = make();
      ws.on("open", () => { health[name] = "online"; delay = 1000; });
      ws.on("close", () => {
        health[name] = "reconnecting";
        setTimeout(connect, delay);
        delay = Math.min(delay * 2, 30000);
      });
      ws.on("error", () => ws.close());
    } catch {
      health[name] = "error";
      setTimeout(connect, delay);
    }
  };
  connect();
}

reconnect("Binance", () => {
  const ws = new WebSocket("wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade");
  ws.on("message", b => {
    const x = JSON.parse(b).data;
    normalize("Binance", x.s, x.m ? "SELL" : "BUY", x.p, x.q, x.a, x.T);
  });
  return ws;
});

reconnect("Bybit", () => {
  const ws = new WebSocket("wss://stream.bybit.com/v5/public/linear");
  ws.on("open", () => ws.send(JSON.stringify({
    op:"subscribe", args:["publicTrade.BTCUSDT","publicTrade.ETHUSDT"]
  })));
  ws.on("message", b => {
    const j=JSON.parse(b);
    for (const x of j.data || [])
      normalize("Bybit", x.s, String(x.S).toUpperCase(), x.p, x.v, x.i, x.T, {block:x.BT===true});
  });
  return ws;
});

reconnect("OKX", () => {
  const ws = new WebSocket("wss://ws.okx.com:8443/ws/v5/public");
  ws.on("open", () => ws.send(JSON.stringify({
    op:"subscribe",
    args:[
      {channel:"trades",instId:"BTC-USDT-SWAP"},
      {channel:"trades",instId:"ETH-USDT-SWAP"}
    ]
  })));
  ws.on("message", b => {
    const j=JSON.parse(b);
    for (const x of j.data || [])
      normalize("OKX", x.instId, x.side==="buy"?"BUY":"SELL", x.px, x.sz, x.tradeId, x.ts);
  });
  return ws;
});

reconnect("Hyperliquid", () => {
  const ws = new WebSocket("wss://api.hyperliquid.xyz/ws");
  ws.on("open", () => ["BTC","ETH"].forEach(coin =>
    ws.send(JSON.stringify({method:"subscribe",subscription:{type:"trades",coin}}))
  ));
  ws.on("message", b => {
    const j=JSON.parse(b);
    for (const x of (Array.isArray(j.data) ? j.data : []))
      normalize("Hyperliquid", x.coin, x.side==="B"?"BUY":"SELL", x.px, x.sz, x.hash||x.tid||x.time, x.time);
  });
  return ws;
});

app.use(express.static("public"));
app.get("/health", (_, res) => res.json({
  ok:true, uptime:process.uptime(), sources:health, buffer:trades.length
}));
app.get("/api/snapshot", (_, res) => {
  const cutoff=Date.now()-86400000;
  res.json({trades:trades.filter(x=>x.ts>=cutoff).slice(-8000), health});
});

const server=app.listen(PORT, "0.0.0.0", () => console.log(`WhaleScope listening on ${PORT}`));
const wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection", ws => {
  clients.add(ws);
  ws.on("close",()=>clients.delete(ws));
});

setInterval(()=>{
  const cutoff=Date.now()-86400000;
  while(trades.length && trades[0].ts < cutoff) trades.shift();
},60000);
