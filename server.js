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

app.get("/", (_, res) => res.type("html").send("<!doctype html><html lang=\"zh-Hant\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>WhaleScope</title><style>\n*{box-sizing:border-box}body{margin:0;background:#070a12;color:#edf2ff;font:14px system-ui}main{max-width:1450px;margin:auto;padding:20px}.top{display:flex;justify-content:space-between;align-items:center;gap:12px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{background:#0e1422;border:1px solid #202b42;border-radius:14px;padding:16px}.v{font-size:25px;font-weight:800}.buy{color:#42e6a4}.sell{color:#ff6680}.pill{display:inline-block;padding:4px 9px;margin:3px;border:1px solid #33415f;border-radius:12px}select{background:#111a2b;color:#fff;padding:9px;border:1px solid #33415f;border-radius:9px}table{width:100%;border-collapse:collapse}td,th{padding:9px;border-bottom:1px solid #1d2638;text-align:right}td:first-child,th:first-child{text-align:left}.note{opacity:.72;line-height:1.6}@media(max-width:800px){.grid{grid-template-columns:1fr 1fr}.opt{display:none}}\n</style></head><body><main>\n<div class=\"top\"><div><h1>WhaleScope</h1><div>BTC / ETH · 24H 大額合約成交雷達</div></div><select id=\"threshold\"><option value=\"100000\">$100K+</option><option value=\"500000\" selected>$500K+</option><option value=\"1000000\">$1M+</option></select></div>\n<p id=\"health\"></p><div class=\"grid\">\n<div class=\"card\"><small>24H 主動買</small><div class=\"v buy\" id=\"buy\">$0</div></div>\n<div class=\"card\"><small>24H 主動賣</small><div class=\"v sell\" id=\"sell\">$0</div></div>\n<div class=\"card\"><small>NET FLOW</small><div class=\"v\" id=\"net\">$0</div></div>\n<div class=\"card\"><small>BUY / SELL</small><div class=\"v\" id=\"ratio\">—</div></div></div>\n<div class=\"card\" style=\"margin-top:12px\"><h3>即時大額成交</h3><table><thead><tr><th>來源</th><th>幣</th><th>方向</th><th>名目金額</th><th class=\"opt\">價格</th><th>時間</th></tr></thead><tbody id=\"rows\"></tbody></table></div>\n<p class=\"note\">CEX 公開成交只能辨識主動買/賣，無法辨識交易帳戶，也不能直接判定新開多或新開空。本服務僅做公開市場監控與統計。</p>\n</main><script>\nlet trades=[];const $=id=>document.getElementById(id),money=n=>\"$\"+Intl.NumberFormat(\"en\",{notation:\"compact\",maximumFractionDigits:2}).format(n);\nfunction draw(){const threshold=+$(\"threshold\").value,cutoff=Date.now()-864e5,x=trades.filter(v=>v.ts>cutoff&&v.usd>=threshold),buy=x.filter(v=>v.side===\"BUY\").reduce((s,v)=>s+v.usd,0),sell=x.filter(v=>v.side===\"SELL\").reduce((s,v)=>s+v.usd,0);$(\"buy\").textContent=money(buy);$(\"sell\").textContent=money(sell);$(\"net\").textContent=money(buy-sell);$(\"ratio\").textContent=sell?(buy/sell).toFixed(2):\"—\";$(\"rows\").innerHTML=x.slice(-120).reverse().map(v=>'<tr><td>'+v.exchange+'</td><td>'+v.asset+'</td><td class=\"'+(v.side===\"BUY\"?\"buy\":\"sell\")+'\">'+v.side+'</td><td>'+money(v.usd)+'</td><td class=\"opt\">'+Intl.NumberFormat().format(v.price)+'</td><td>'+new Date(v.ts).toLocaleTimeString()+'</td></tr>').join(\"\")}\nfetch(\"/api/snapshot\").then(r=>r.json()).then(j=>{trades=j.trades;$(\"health\").innerHTML=Object.entries(j.health).map(([k,v])=>'<span class=\"pill\">'+k+\" · \"+v+\"</span>\").join(\"\");draw()});\nconst ws=new WebSocket((location.protocol===\"https:\"?\"wss://\":\"ws://\")+location.host+\"/ws\");ws.onmessage=e=>{const j=JSON.parse(e.data);if(j.type===\"trade\"){trades.push(j.data);if(trades.length>12000)trades.shift();draw()}};\n$(\"threshold\").onchange=draw;setInterval(draw,5000);\n</script></body></html>"));
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
