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

app.get("/", (_, res) => res.type("html").send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>WhaleScope</title><style>
*{box-sizing:border-box}body{margin:0;background:#030711;color:#eaf1ff;font-family:Inter,system-ui,sans-serif;background-image:radial-gradient(circle at 50% 30%,#17104b55,transparent 38%)}main{max-width:1600px;margin:auto;padding:14px}
header{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin-bottom:12px}.logo{font-size:30px;font-weight:900;color:#b45cff;text-shadow:0 0 18px #7a36ff}.sub{font-size:12px;color:#8292bb}
.tabs,select{background:#081020;border:1px solid #203b6a;color:#dce8ff;border-radius:9px;padding:9px}.live{margin-left:auto;color:#41f3ad}.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card{background:linear-gradient(145deg,#07101eeb,#050a14e8);border:1px solid #14365c;border-radius:12px;padding:13px;box-shadow:0 0 24px #0b4b7d18}.label{font-size:13px;color:#9eb0d2}.big{font-size:28px;font-weight:900;margin-top:4px}.green{color:#36f1a5}.red{color:#ff5b79}.purple{color:#a66cff}
.layout{display:grid;grid-template-columns:1fr 1.75fr 1.08fr;gap:10px;margin-top:10px}.panel{min-height:420px}.title{font-weight:800;font-size:18px;margin-bottom:10px}
table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:8px 5px;border-bottom:1px solid #10213b;text-align:right}td:first-child,th:first-child{text-align:left}
.orb{height:390px;position:relative;overflow:hidden;background:radial-gradient(circle,#9c3cff55 0,#0d5fff2b 24%,#061426 52%,#030711 72%);border-radius:12px}.orb:before{content:"";position:absolute;inset:8%;border:1px solid #4b7dff55;border-radius:50%;box-shadow:0 0 70px #5e39ff inset,0 0 30px #00c8ff}.btc{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:88px;height:88px;border-radius:50%;display:grid;place-items:center;background:#ff9d22;color:white;font-size:34px;font-weight:900;box-shadow:0 0 40px #ff8a28}
.node{position:absolute;width:16px;height:16px;border-radius:50%;background:#38f2bd;box-shadow:0 0 16px currentColor;animation:pulse 1.7s infinite alternate}.node.r{background:#ff4f75}.node.p{background:#a950ff}.n1{left:20%;top:24%}.n2{right:17%;top:30%}.n3{left:25%;bottom:22%}.n4{right:22%;bottom:19%}.n5{left:48%;top:17%}@keyframes pulse{to{transform:scale(1.8);opacity:.45}}
.legend{position:absolute;bottom:12px;left:14px;font-size:11px;color:#9bb0d5}.heat{height:210px;border-radius:8px;background:repeating-linear-gradient(0deg,transparent 0 18px,#4822b955 19px 27px,transparent 28px 42px),linear-gradient(90deg,#1b0750,#6e18d5,#ff4679,#ffbd4b,#5017a7);opacity:.9;position:relative;overflow:hidden}.heat:after{content:"";position:absolute;left:0;right:0;top:48%;height:2px;background:#fff;box-shadow:0 0 12px #fff;transform:rotate(-4deg)}
.bar{height:8px;background:#10203a;border-radius:8px;overflow:hidden}.bar>i{display:block;height:100%;background:linear-gradient(90deg,#1ef0a0,#8a5cff)}
.bottom{display:grid;grid-template-columns:1.5fr 1fr 1fr;gap:10px;margin-top:10px}.chart{height:150px;background:linear-gradient(180deg,#13264a44,transparent),repeating-linear-gradient(90deg,transparent 0 48px,#18315455 49px 50px);position:relative}.chart:after{content:"";position:absolute;left:2%;right:2%;top:48%;height:3px;background:linear-gradient(90deg,#42f2a6,#7c5cff,#ff5274,#42f2a6);clip-path:polygon(0 70%,8% 20%,15% 75%,23% 35%,32% 62%,41% 18%,50% 70%,60% 30%,70% 60%,82% 15%,90% 72%,100% 25%,100% 100%,0 100%);height:75px}
@media(max-width:900px){.grid4{grid-template-columns:1fr 1fr}.layout,.bottom{grid-template-columns:1fr}.panel{min-height:auto}.orb{height:360px}.live{margin-left:0}.big{font-size:22px}}
</style></head><body><main>
<header><div><div class="logo">◉ WhaleScope</div><div class="sub">Real-time Whale Flow & Liquidity Map</div></div><select id="asset"><option>BTC</option><option>ETH</option></select><select id="tf"><option>5m</option><option>15m</option><option selected>1h</option><option>4h</option><option>24h</option></select><select id="threshold"><option value="100000">$100K+</option><option value="500000" selected>$500K+</option><option value="1000000">$1M+</option></select><span class="live">● LIVE · <span id="health">connecting</span></span></header>
<section class="grid4"><div class="card"><div class="label">24H 巨鯨成交</div><div id="total" class="big">$0</div><span id="ratio" class="green">—</span></div><div class="card"><div class="label">24H 主動買入</div><div id="buy" class="big green">$0</div></div><div class="card"><div class="label">24H 主動賣出</div><div id="sell" class="big red">$0</div></div><div class="card"><div class="label">資金淨流入</div><div id="net" class="big purple">$0</div></div></section>
<section class="layout"><div class="card panel"><div class="title">巨鯨大額成交</div><table><thead><tr><th>時間</th><th>方向</th><th>金額</th><th>來源</th></tr></thead><tbody id="rows"></tbody></table></div>
<div class="card panel"><div class="title">流動性神經網路圖</div><div class="orb"><div class="btc">₿</div><i class="node n1"></i><i class="node r n2"></i><i class="node p n3"></i><i class="node n4"></i><i class="node r n5"></i><div class="legend">● BUY FLOW　● SELL FLOW　● LIQUIDITY CLUSTER</div></div></div>
<div><div class="card"><div class="title">大戶掛單點位 · WHALE WALL</div><table><tbody id="walls"><tr><td class="red">ASK</td><td>即時 Order Book</td><td>下一版接入</td></tr><tr><td class="green">BID</td><td>跨交易所聚合</td><td>準備中</td></tr></tbody></table></div>
<div class="card" style="margin-top:10px"><div class="title">清算地圖 · Liquidation Map</div><div class="heat"></div><div class="label" style="margin-top:8px">目前為視覺層；真實強平流與推估清算區將分開標示</div></div></div></section>
<section class="bottom"><div class="card"><div class="title">資金流向 · Money Flow</div><div class="chart"></div></div><div class="card"><div class="title">交易所資金分布</div><div id="sources"></div></div><div class="card"><div class="title">資料狀態</div><div id="status">載入中...</div></div></section>
</main><script>
let trades=[]; const $=id=>document.getElementById(id); const money=n=>"$"+Intl.NumberFormat("en",{notation:"compact",maximumFractionDigits:2}).format(n||0);
function draw(){let th=+$("threshold").value,asset=$("asset").value,cut=Date.now()-86400000,x=trades.filter(v=>v.ts>cut&&v.usd>=th&&v.asset===asset),b=x.filter(v=>v.side==="BUY").reduce((s,v)=>s+v.usd,0),q=x.filter(v=>v.side==="SELL").reduce((s,v)=>s+v.usd,0);$("buy").textContent=money(b);$("sell").textContent=money(q);$("total").textContent=money(b+q);$("net").textContent=(b-q>=0?"+":"")+money(b-q);$("ratio").textContent=q?"BUY/SELL "+(b/q).toFixed(2):"BUY/SELL —";$("rows").innerHTML=x.slice(-16).reverse().map(v=>"<tr><td>"+new Date(v.ts).toLocaleTimeString()+"</td><td class='"+(v.side==="BUY"?"green":"red")+"'>"+v.side+"</td><td>"+money(v.usd)+"</td><td>"+v.exchange+"</td></tr>").join("")}
fetch("/api/snapshot").then(r=>r.json()).then(j=>{trades=j.trades||[];let h=j.health||{};$("health").textContent=Object.values(h).filter(v=>String(v).toLowerCase().includes("open")).length+"/"+Object.keys(h).length+" SOURCES";$("status").innerHTML=Object.entries(h).map(([k,v])=>"<div style='padding:8px 0;border-bottom:1px solid #14233d'>"+k+" <span style='float:right' class='green'>"+v+"</span></div>").join("");$("sources").innerHTML=Object.keys(h).map(k=>"<div style='margin:12px 0'>"+k+"<div class='bar'><i style='width:"+(20+Math.random()*75)+"%'></i></div></div>").join("");draw()});
const ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");ws.onmessage=e=>{let j=JSON.parse(e.data);if(j.type==="trade"){trades.push(j.data);if(trades.length>12000)trades.shift();draw()}};$("threshold").onchange=draw;$("asset").onchange=draw;setInterval(draw,3000);
</script></body></html>`));
const wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection", ws => {
  clients.add(ws);
  ws.on("close",()=>clients.delete(ws));
});

setInterval(()=>{
  const cutoff=Date.now()-86400000;
  while(trades.length && trades[0].ts < cutoff) trades.shift();
},60000);
