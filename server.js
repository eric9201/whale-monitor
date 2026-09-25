import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
const clients = new Set();
const trades = [];
const seen = new Set();
const health = {};
const liquidations = [];
const books = {BTC:{},ETH:{}};
const LIMIT = 30000;

function broadcast(type, data) {
  const msg = JSON.stringify({type, data});
  for (const c of clients) if (c.readyState === WebSocket.OPEN) c.send(msg);
}
function pushLiq(x) {
  liquidations.push(x);
  if (liquidations.length > 5000) liquidations.shift();
  broadcast("liquidation", x);
}
function pushBook(exchange, asset, bids, asks) {
  books[asset][exchange] = {bids, asks, ts:Date.now()};
  broadcast("book", {exchange, asset, bids, asks, ts:Date.now()});
}

function publish(x) {
  const key = `${x.exchange}:${x.id}`;
  if (seen.has(key)) return;
  seen.add(key);
  if (seen.size > 60000) seen.clear();
  trades.push(x);
  if (trades.length > LIMIT) trades.shift();
  broadcast("trade", x);
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
  const ws = new WebSocket("wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@forceOrder/ethusdt@forceOrder/btcusdt@depth20@500ms/ethusdt@depth20@500ms");
  ws.on("message", b => {
    const j = JSON.parse(b), x = j.data, st = j.stream || "";
    if (st.includes("@aggTrade")) {
      normalize("Binance", x.s, x.m ? "SELL" : "BUY", x.p, x.q, x.a, x.T);
    } else if (st.includes("@forceOrder")) {
      const o=x.o, p=Number(o.ap)||Number(o.p), q=Number(o.q);
      pushLiq({exchange:"Binance",asset:o.s.startsWith("ETH")?"ETH":"BTC",side:o.S,price:p,usd:p*q,ts:Number(o.T)});
    } else if (st.includes("@depth20")) {
      const asset=st.startsWith("eth")?"ETH":"BTC";
      pushBook("Binance",asset,(x.b||[]).map(v=>[+v[0],+v[1]]),(x.a||[]).map(v=>[+v[0],+v[1]]));
    }
  });
  return ws;
});

reconnect("Bybit", () => {
  const ws = new WebSocket("wss://stream.bybit.com/v5/public/linear");
  ws.on("open", () => ws.send(JSON.stringify({
    op:"subscribe", args:["publicTrade.BTCUSDT","publicTrade.ETHUSDT","allLiquidation.BTCUSDT","allLiquidation.ETHUSDT","orderbook.50.BTCUSDT","orderbook.50.ETHUSDT"]
  })));
  ws.on("message", b => {
    const j=JSON.parse(b);
    if ((j.topic||"").startsWith("publicTrade")) {
      for (const x of j.data || []) normalize("Bybit", x.s, String(x.S).toUpperCase(), x.p, x.v, x.i, x.T, {block:x.BT===true});
    } else if ((j.topic||"").startsWith("allLiquidation")) {
      for (const x of j.data || []) pushLiq({exchange:"Bybit",asset:x.s.startsWith("ETH")?"ETH":"BTC",side:x.S,price:+x.p,usd:(+x.p)*(+x.v),ts:+x.T});
    } else if ((j.topic||"").startsWith("orderbook") && j.type==="snapshot" && j.data) {
      const d=j.data, asset=d.s.startsWith("ETH")?"ETH":"BTC";
      pushBook("Bybit",asset,(d.b||[]).map(v=>[+v[0],+v[1]]),(d.a||[]).map(v=>[+v[0],+v[1]]));
    }
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

app.get("/health", (_, res) => res.json({ok:true, uptime:process.uptime(), sources:health, buffer:trades.length}));
app.get("/api/snapshot", (_, res) => {
  const cutoff = Date.now() - 86400000;
  res.json({trades: trades.filter(x => x.ts >= cutoff).slice(-8000), liquidations:liquidations.filter(x=>x.ts>=cutoff), books, health});
});

app.get("/", (_, res) => res.type("html").send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WhaleScope</title><style>
*{box-sizing:border-box}body{margin:0;background:#030711;color:#eaf1ff;font-family:Inter,system-ui,sans-serif;background-image:radial-gradient(circle at 50% 0,#17104b55,transparent 38%)}main{max-width:1500px;margin:auto;padding:14px}header{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:12px}.logo{font-size:29px;font-weight:900;color:#b45cff;text-shadow:0 0 18px #7a36ff}.sub,.muted{font-size:12px;color:#8292bb}select{background:#081020;border:1px solid #203b6a;color:#dce8ff;border-radius:9px;padding:9px}.live{margin-left:auto;color:#41f3ad}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card{background:linear-gradient(145deg,#07101eeb,#050a14e8);border:1px solid #14365c;border-radius:12px;padding:13px;box-shadow:0 0 24px #0b4b7d18}.label{font-size:13px;color:#9eb0d2}.big{font-size:27px;font-weight:900;margin-top:4px}.green{color:#36f1a5}.red{color:#ff5b79}.purple{color:#a66cff}.grid{display:grid;grid-template-columns:1.15fr 1fr;gap:10px;margin-top:10px}.title{font-weight:800;font-size:17px;margin-bottom:10px}table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:8px 5px;border-bottom:1px solid #10213b;text-align:right}td:first-child,th:first-child{text-align:left}.scroll{max-height:410px;overflow:auto}.barrow{margin:12px 0}.bar{height:10px;background:#10203a;border-radius:8px;overflow:hidden;margin-top:5px}.bar i{display:block;height:100%;background:linear-gradient(90deg,#1ef0a0,#8a5cff)}.flow{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.flowbox{background:#081020;border:1px solid #142d50;border-radius:9px;padding:12px}.heatrow{display:grid;grid-template-columns:72px 1fr 90px;gap:8px;align-items:center;margin:8px 0}.heatbar{height:12px;border-radius:7px;background:#151e34;overflow:hidden}.heatbar i{display:block;height:100%;background:linear-gradient(90deg,#6d36ff,#ff5274)}@media(max-width:800px){.cards,.grid,.flow{grid-template-columns:1fr 1fr}.live{margin-left:0}.big{font-size:21px}}@media(max-width:520px){.grid,.flow{grid-template-columns:1fr}}
</style></head><body><main>
<header><div><div class="logo">◉ WhaleScope</div><div class="sub">Real-time Whale Flow & Liquidity</div></div><select id="asset"><option>BTC</option><option>ETH</option></select><select id="threshold"><option value="100000">$100K+</option><option value="500000" selected>$500K+</option><option value="1000000">$1M+</option></select><span class="live">● LIVE · <span id="health">connecting</span></span></header>
<section class="cards"><div class="card"><div class="label">24H 巨鯨成交</div><div id="total" class="big">$0</div><span id="ratio" class="green">—</span></div><div class="card"><div class="label">24H 主動買入</div><div id="buy" class="big green">$0</div></div><div class="card"><div class="label">24H 主動賣出</div><div id="sell" class="big red">$0</div></div><div class="card"><div class="label">資金淨流入</div><div id="net" class="big purple">$0</div></div></section>
<section class="grid"><div class="card"><div class="title">巨鯨大額成交</div><div class="scroll"><table><thead><tr><th>時間</th><th>方向</th><th>金額</th><th>來源</th></tr></thead><tbody id="rows"></tbody></table></div></div><div class="card"><div class="title">大戶掛單點位 · WHALE WALL</div><div class="scroll"><table><thead><tr><th>類型</th><th>價格</th><th>金額</th><th>交易所</th></tr></thead><tbody id="walls"></tbody></table></div></div></section>
<section class="grid"><div class="card"><div class="title">清算地圖 · ACTUAL LIQUIDATIONS</div><div id="liqmap"></div><div class="muted">公開強平事件聚合；不是推估未來清算價位。</div></div><div class="card"><div class="title">交易所資金分布</div><div id="venues"></div></div></section>
<section class="card" style="margin-top:10px"><div class="title">資金流向 · MONEY FLOW</div><div id="flow" class="flow"></div></section>
</main><script>
let trades=[],liqs=[],books={BTC:{},ETH:{}};const $=x=>document.getElementById(x),money=n=>"$"+Intl.NumberFormat("en",{notation:"compact",maximumFractionDigits:2}).format(n||0);
function calc(cut,asset,th){let x=trades.filter(v=>v.ts>=cut&&v.asset===asset&&v.usd>=th),b=x.filter(v=>v.side==="BUY").reduce((s,v)=>s+v.usd,0),q=x.filter(v=>v.side==="SELL").reduce((s,v)=>s+v.usd,0);return{x,b,q,net:b-q}}
function draw(){const a=$("asset").value,th=+$("threshold").value,now=Date.now(),d=calc(now-86400000,a,th);$("buy").textContent=money(d.b);$("sell").textContent=money(d.q);$("total").textContent=money(d.b+d.q);$("net").textContent=(d.net>=0?"+":"")+money(d.net);$("ratio").textContent=d.q?"BUY/SELL "+(d.b/d.q).toFixed(2):"BUY/SELL —";$("rows").innerHTML=d.x.slice(-30).reverse().map(v=>"<tr><td>"+new Date(v.ts).toLocaleTimeString()+"</td><td class='"+(v.side==="BUY"?"green":"red")+"'>"+v.side+"</td><td>"+money(v.usd)+"</td><td>"+v.exchange+"</td></tr>").join("");
let wall=[];for(const [ex,b] of Object.entries(books[a]||{})){for(const [p,q] of b.bids||[])if(p*q>=th)wall.push({t:"BID",p,u:p*q,e:ex});for(const [p,q] of b.asks||[])if(p*q>=th)wall.push({t:"ASK",p,u:p*q,e:ex})}wall.sort((x,y)=>y.u-x.u);$("walls").innerHTML=wall.slice(0,30).map(w=>"<tr><td class='"+(w.t==="BID"?"green":"red")+"'>"+w.t+"</td><td>"+w.p.toLocaleString()+"</td><td>"+money(w.u)+"</td><td>"+w.e+"</td></tr>").join("")||"<tr><td colspan=4 class=muted>目前門檻沒有大型掛單</td></tr>";
const ls=liqs.filter(x=>x.asset===a&&x.ts>=now-86400000),mx=Math.max(1,...ls.map(x=>x.usd));$("liqmap").innerHTML=ls.slice(-14).reverse().map(x=>"<div class=heatrow><span class='"+(x.side==="BUY"?"green":"red")+"'>"+x.side+"</span><div class=heatbar><i style='width:"+Math.max(2,x.usd/mx*100)+"%'></i></div><span>"+money(x.usd)+"</span></div>").join("")||"<div class=muted>等待公開強平事件…</div>";
let by={};for(const v of d.x)by[v.exchange]=(by[v.exchange]||0)+v.usd;let max=Math.max(1,...Object.values(by));$("venues").innerHTML=Object.entries(by).sort((a,b)=>b[1]-a[1]).map(([k,v])=>"<div class=barrow>"+k+" <span style='float:right'>"+money(v)+"</span><div class=bar><i style='width:"+(v/max*100)+"%'></i></div></div>").join("")||"<div class=muted>等待成交資料…</div>";
$("flow").innerHTML=[[5,"5m"],[15,"15m"],[60,"1h"],[1440,"24h"]].map(([m,n])=>{let z=calc(now-m*60000,a,th);return"<div class=flowbox><div class=label>"+n+" NET FLOW</div><div class='big "+(z.net>=0?"green":"red")+"'>"+(z.net>=0?"+":"")+money(z.net)+"</div><div class=muted>BUY "+money(z.b)+" / SELL "+money(z.q)+"</div></div>"}).join("")}
fetch("/api/snapshot").then(r=>r.json()).then(j=>{trades=j.trades||[];liqs=j.liquidations||[];books=j.books||books;let h=j.health||{};$("health").textContent=Object.values(h).filter(v=>v==="online").length+"/"+Object.keys(h).length+" SOURCES";draw()});
const ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");ws.onmessage=e=>{let j=JSON.parse(e.data);if(j.type==="trade")trades.push(j.data);if(j.type==="liquidation")liqs.push(j.data);if(j.type==="book"){books[j.data.asset]??={};books[j.data.asset][j.data.exchange]=j.data}draw()};$("asset").onchange=draw;$("threshold").onchange=draw;setInterval(draw,3000);
</script></body></html>`));

const wss = new WebSocketServer({server, path:"/ws"});
wss.on("connection", ws => {
  clients.add(ws);
  ws.on("close",()=>clients.delete(ws));
});

setInterval(()=>{
  const cutoff=Date.now()-86400000;
  while(trades.length && trades[0].ts < cutoff) trades.shift();
},60000);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`WhaleScope listening on ${PORT}`);
});
