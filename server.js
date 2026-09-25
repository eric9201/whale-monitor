import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";

const app=express(), server=http.createServer(app), PORT=process.env.PORT||3000;
const clients=new Set(), trades=[], liquidations=[], seen=new Set(), health={};
const books={BTC:{},ETH:{}}, prices={BTC:0,ETH:0}, oi={BTC:{},ETH:{}};
const LIMIT=40000;

const send=(type,data)=>{const m=JSON.stringify({type,data});for(const c of clients)if(c.readyState===WebSocket.OPEN)c.send(m)};
function trade(exchange,asset,side,price,qty,id,ts){
  price=+price;qty=+qty;const usd=price*qty,key=exchange+":"+id;
  if(!Number.isFinite(usd)||usd<=0||seen.has(key))return; seen.add(key); if(seen.size>80000)seen.clear();
  prices[asset]=price; const x={exchange,asset,side,price,qty,usd,id:String(id),ts:+ts||Date.now()};
  trades.push(x);if(trades.length>LIMIT)trades.shift();send("trade",x);
}
function liq(exchange,asset,side,price,qty,ts){
  const x={exchange,asset,side,price:+price,usd:(+price)*(+qty),ts:+ts||Date.now()};
  if(x.usd>0){liquidations.push(x);if(liquidations.length>8000)liquidations.shift();send("liquidation",x)}
}
function book(exchange,asset,bids,asks){books[asset][exchange]={bids,asks,ts:Date.now()};send("book",{exchange,asset,bids,asks,ts:Date.now()})}
function reconnect(name,make){let delay=1000;const go=()=>{health[name]="connecting";try{const ws=make();ws.on("open",()=>{health[name]="online";delay=1000});ws.on("close",()=>{health[name]="reconnecting";setTimeout(go,delay);delay=Math.min(delay*2,30000)});ws.on("error",()=>ws.close())}catch(e){health[name]="error";setTimeout(go,delay)}};go()}

// Binance: trades + actual liquidations + L20 orderbook
reconnect("Binance",()=>{const ws=new WebSocket("wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@forceOrder/ethusdt@forceOrder/btcusdt@depth20@500ms/ethusdt@depth20@500ms");ws.on("message",b=>{const j=JSON.parse(b),x=j.data,st=j.stream||"",a=st.startsWith("eth")?"ETH":"BTC";if(st.includes("@aggTrade"))trade("Binance",a,x.m?"SELL":"BUY",x.p,x.q,x.a,x.T);else if(st.includes("@forceOrder")){const o=x.o,p=+o.ap||+o.p;liq("Binance",a,o.S,p,o.q,o.T)}else if(st.includes("@depth20"))book("Binance",a,(x.b||[]).map(v=>[+v[0],+v[1]]),(x.a||[]).map(v=>[+v[0],+v[1]]))});return ws});

// Bybit: trades + actual liquidations + L50 book with correct snapshot/delta merge
const bybitBook={BTC:{b:new Map(),a:new Map()},ETH:{b:new Map(),a:new Map()}};
reconnect("Bybit",()=>{const ws=new WebSocket("wss://stream.bybit.com/v5/public/linear");ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:["publicTrade.BTCUSDT","publicTrade.ETHUSDT","allLiquidation.BTCUSDT","allLiquidation.ETHUSDT","orderbook.50.BTCUSDT","orderbook.50.ETHUSDT"]})));ws.on("message",b=>{const j=JSON.parse(b),t=j.topic||"";if(t.startsWith("publicTrade"))for(const x of j.data||[])trade("Bybit",x.s.startsWith("ETH")?"ETH":"BTC",String(x.S).toUpperCase(),x.p,x.v,x.i,x.T);else if(t.startsWith("allLiquidation"))for(const x of j.data||[])liq("Bybit",x.s.startsWith("ETH")?"ETH":"BTC",x.S,x.p,x.v,x.T);else if(t.startsWith("orderbook")&&j.data){const d=j.data,A=d.s.startsWith("ETH")?"ETH":"BTC",ob=bybitBook[A];if(j.type==="snapshot"){ob.b.clear();ob.a.clear()}for(const [p,q] of d.b||[]){+q===0?ob.b.delete(p):ob.b.set(p,+q)}for(const [p,q] of d.a||[]){+q===0?ob.a.delete(p):ob.a.set(p,+q)}book("Bybit",A,[...ob.b].map(([p,q])=>[+p,q]).sort((x,y)=>y[0]-x[0]).slice(0,50),[...ob.a].map(([p,q])=>[+p,q]).sort((x,y)=>x[0]-y[0]).slice(0,50))}});return ws});

// OKX: contract sizes converted to base asset (BTC ctVal .01, ETH .1 for these USDT swaps)
const okxCt={BTC:.01,ETH:.1};
reconnect("OKX",()=>{const ws=new WebSocket("wss://ws.okx.com:8443/ws/v5/public");ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:["BTC-USDT-SWAP","ETH-USDT-SWAP"].flatMap(instId=>[{channel:"trades",instId},{channel:"books5",instId}])})));ws.on("message",b=>{const j=JSON.parse(b),arg=j.arg||{},A=(arg.instId||"").startsWith("ETH")?"ETH":"BTC";if(arg.channel==="trades")for(const x of j.data||[])trade("OKX",A,x.side==="buy"?"BUY":"SELL",x.px,(+x.sz)*okxCt[A],x.tradeId,x.ts);if(arg.channel==="books5"&&j.data?.[0]){const d=j.data[0],cv=okxCt[A];book("OKX",A,(d.bids||[]).map(v=>[+v[0],+v[1]*cv]),(d.asks||[]).map(v=>[+v[0],+v[1]*cv]))}});return ws});

// Hyperliquid on-chain perps: trades + L2 book
reconnect("Hyperliquid",()=>{const ws=new WebSocket("wss://api.hyperliquid.xyz/ws");ws.on("open",()=>["BTC","ETH"].forEach(coin=>{ws.send(JSON.stringify({method:"subscribe",subscription:{type:"trades",coin}}));ws.send(JSON.stringify({method:"subscribe",subscription:{type:"l2Book",coin}}))}));ws.on("message",b=>{const j=JSON.parse(b);if(j.channel==="trades")for(const x of j.data||[])trade("Hyperliquid",x.coin,x.side==="B"?"BUY":"SELL",x.px,x.sz,x.hash||x.tid||x.time,x.time);if(j.channel==="l2Book"&&j.data){const d=j.data,A=d.coin;if(A==="BTC"||A==="ETH"){const lv=d.levels||[[],[]];book("Hyperliquid",A,(lv[0]||[]).map(x=>[+x.px,+x.sz]),(lv[1]||[]).map(x=>[+x.px,+x.sz]))}}});return ws});

// Supplemental major-venue ticker flow. These are labelled "market coverage"; only venues with live public feeds contribute.
function simpleWS(name,url,subscribe,parse){reconnect(name,()=>{const ws=new WebSocket(url);if(subscribe)ws.on("open",()=>ws.send(JSON.stringify(subscribe)));ws.on("message",b=>{try{parse(JSON.parse(b))}catch{}});return ws})}
simpleWS("Bitget","wss://ws.bitget.com/v2/ws/public",{op:"subscribe",args:[{instType:"USDT-FUTURES",channel:"trade",instId:"BTCUSDT"},{instType:"USDT-FUTURES",channel:"trade",instId:"ETHUSDT"}]},j=>{for(const x of j.data||[]){const A=(j.arg?.instId||"").startsWith("ETH")?"ETH":"BTC";trade("Bitget",A,x.side==="buy"?"BUY":"SELL",x.price,x.size,x.tradeId||x.ts,x.ts)}});
simpleWS("Gate","wss://fx-ws.gateio.ws/v4/ws/usdt",null,j=>{}); // health coverage; REST/WS schema varies, excluded from flow until validated
simpleWS("MEXC","wss://contract.mexc.com/edge",null,j=>{}); // health coverage; excluded from flow until validated
simpleWS("BingX","wss://open-api-swap.bingx.com/swap-market",null,j=>{}); // health coverage; excluded from flow until validated
health["KuCoin"]="coverage-pending"; health["HTX"]="coverage-pending"; health["Deribit"]="coverage-pending";

// TradingView-style estimated liquidation field: OI/volume proxy + 10x/25x/50x/100x.
// It is explicitly an estimate, not scraped TradingView data or exchange position books.
function estimatedZones(asset){
  const p=prices[asset];if(!p)return[];
  const recent=trades.filter(x=>x.asset===asset&&x.ts>Date.now()-15*60*1000);
  const buy=recent.filter(x=>x.side==="BUY").reduce((s,x)=>s+x.usd,0),sell=recent.filter(x=>x.side==="SELL").reduce((s,x)=>s+x.usd,0);
  const fresh=buy+sell;if(!fresh)return[];
  const longShare=fresh?buy/fresh:.5, lev=[10,25,50,100], out=[];
  for(const L of lev){out.push({side:"LONG",lev:L,price:p*(1-1/L),weight:fresh*longShare/lev.length});out.push({side:"SHORT",lev:L,price:p*(1+1/L),weight:fresh*(1-longShare)/lev.length})}
  return out;
}

app.get("/health",(_,res)=>res.json({ok:true,uptime:process.uptime(),sources:health,buffer:trades.length}));
app.get("/api/snapshot",(_,res)=>{const c=Date.now()-86400000;res.json({trades:trades.filter(x=>x.ts>=c).slice(-10000),liquidations:liquidations.filter(x=>x.ts>=c),books,health,zones:{BTC:estimatedZones("BTC"),ETH:estimatedZones("ETH")}})});

app.get("/",(_,res)=>res.type("html").send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WhaleScope</title><style>
*{box-sizing:border-box}body{margin:0;background:#030711;color:#eaf1ff;font-family:Inter,system-ui;background-image:radial-gradient(circle at 50% 0,#1b0c4e55,transparent 38%)}main{max-width:1550px;margin:auto;padding:14px}header{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:12px}.logo{font-size:29px;font-weight:900;color:#b45cff;text-shadow:0 0 18px #7a36ff}.sub,.muted{font-size:12px;color:#8292bb}select{background:#081020;border:1px solid #203b6a;color:#dce8ff;border-radius:9px;padding:9px}.live{margin-left:auto;color:#41f3ad}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card{background:linear-gradient(145deg,#07101eeb,#050a14e8);border:1px solid #14365c;border-radius:12px;padding:13px;box-shadow:0 0 24px #0b4b7d18}.label{font-size:13px;color:#9eb0d2}.big{font-size:27px;font-weight:900;margin-top:4px}.green{color:#36f1a5}.red{color:#ff5b79}.purple{color:#a66cff}.grid{display:grid;grid-template-columns:1.15fr 1fr;gap:10px;margin-top:10px}.title{font-weight:800;font-size:17px;margin-bottom:10px}table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:8px 5px;border-bottom:1px solid #10213b;text-align:right}td:first-child,th:first-child{text-align:left}.scroll{max-height:380px;overflow:auto}.barrow{margin:10px 0}.bar{height:9px;background:#10203a;border-radius:8px;overflow:hidden;margin-top:4px}.bar i{display:block;height:100%;background:linear-gradient(90deg,#1ef0a0,#8a5cff)}.flow{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.flowbox{background:#081020;border:1px solid #142d50;border-radius:9px;padding:12px}.heat{position:relative;height:330px;background:linear-gradient(180deg,#07101d,#030711);border-radius:9px;overflow:hidden;border:1px solid #142d50}.zone{position:absolute;left:0;right:0;height:14px;transform:translateY(-50%);border-radius:4px;display:flex;align-items:center;padding-left:8px;font-size:10px}.long{background:linear-gradient(90deg,#19e6a055,#19e6a0dd,#19e6a022)}.short{background:linear-gradient(90deg,#ff557522,#ff5575dd,#ff557555)}.pxline{position:absolute;left:0;right:0;height:1px;background:#fff9;box-shadow:0 0 8px #fff}.pill{display:inline-block;padding:3px 7px;border-radius:12px;border:1px solid #264c75;margin:2px;font-size:11px}@media(max-width:800px){.cards,.grid,.flow{grid-template-columns:1fr 1fr}.live{margin-left:0}.big{font-size:21px}}@media(max-width:520px){.grid,.flow{grid-template-columns:1fr}}
</style></head><body><main><header><div><div class="logo">◉ WhaleScope</div><div class="sub">CEX + On-chain Whale Intelligence</div></div><select id="asset"><option>BTC</option><option>ETH</option></select><select id="threshold"><option value="100000">$100K+</option><option value="500000" selected>$500K+</option><option value="1000000">$1M+</option></select><span class="live">● LIVE · <span id="health">connecting</span></span></header>
<section class="cards"><div class="card"><div class="label">24H 巨鯨成交</div><div id="total" class="big">$0</div><span id="ratio" class="green">—</span></div><div class="card"><div class="label">24H 主動買入</div><div id="buy" class="big green">$0</div></div><div class="card"><div class="label">24H 主動賣出</div><div id="sell" class="big red">$0</div></div><div class="card"><div class="label">資金淨流入</div><div id="net" class="big purple">$0</div></div></section>
<section class="grid"><div class="card"><div class="title">巨鯨大額成交</div><div class="scroll"><table><thead><tr><th>時間</th><th>方向</th><th>金額</th><th>來源</th></tr></thead><tbody id="rows"></tbody></table></div></div><div class="card"><div class="title">大戶掛單點位 · CEX + ON-CHAIN</div><div class="scroll"><table><thead><tr><th>類型</th><th>價格</th><th>金額</th><th>來源</th></tr></thead><tbody id="walls"></tbody></table></div></div></section>
<section class="grid"><div class="card"><div class="title">推估清算熱圖 · 10x / 25x / 50x / 100x</div><div id="heat" class="heat"></div><div class="muted">TradingView 類型模型：由公開市場活動推估；不是實際持倉或保證清算價。</div></div><div class="card"><div class="title">實際強平事件</div><div id="actual" class="scroll"></div></div></section>
<section class="grid"><div class="card"><div class="title">交易所資金分布 · LIVE COVERAGE</div><div id="venues"></div><div class="muted">只統計目前已驗證且實際收到成交資料的來源；不以假數值補滿前十大。</div></div><div class="card"><div class="title">市場覆蓋</div><div id="coverage"></div></div></section>
<section class="card" style="margin-top:10px"><div class="title">資金流向 · MONEY FLOW</div><div id="flow" class="flow"></div></section>
</main><script>
let trades=[],liqs=[],books={BTC:{},ETH:{}},zones={BTC:[],ETH:[]},health={};const $=x=>document.getElementById(x),money=n=>"$"+Intl.NumberFormat("en",{notation:"compact",maximumFractionDigits:2}).format(n||0);
function calc(cut,a,th){let x=trades.filter(v=>v.ts>=cut&&v.asset===a&&v.usd>=th),b=x.filter(v=>v.side==="BUY").reduce((s,v)=>s+v.usd,0),q=x.filter(v=>v.side==="SELL").reduce((s,v)=>s+v.usd,0);return{x,b,q,net:b-q}}
function draw(){const a=$("asset").value,th=+$("threshold").value,now=Date.now(),d=calc(now-86400000,a,th);$("buy").textContent=money(d.b);$("sell").textContent=money(d.q);$("total").textContent=money(d.b+d.q);$("net").textContent=(d.net>=0?"+":"")+money(d.net);$("ratio").textContent=d.q?"BUY/SELL "+(d.b/d.q).toFixed(2):"BUY/SELL —";$("rows").innerHTML=d.x.slice(-30).reverse().map(v=>"<tr><td>"+new Date(v.ts).toLocaleTimeString()+"</td><td class='"+(v.side==="BUY"?"green":"red")+"'>"+v.side+"</td><td>"+money(v.usd)+"</td><td>"+v.exchange+"</td></tr>").join("");
let wall=[];for(const [ex,b] of Object.entries(books[a]||{})){for(const [p,q] of b.bids||[])if(p*q>=th)wall.push({t:"BID",p,u:p*q,e:ex});for(const [p,q] of b.asks||[])if(p*q>=th)wall.push({t:"ASK",p,u:p*q,e:ex})}wall.sort((x,y)=>y.u-x.u);$("walls").innerHTML=wall.slice(0,40).map(w=>"<tr><td class='"+(w.t==="BID"?"green":"red")+"'>"+w.t+"</td><td>"+w.p.toLocaleString()+"</td><td>"+money(w.u)+"</td><td>"+w.e+"</td></tr>").join("")||"<tr><td colspan=4 class=muted>目前門檻沒有大型掛單</td></tr>";
const z=zones[a]||[], ps=z.map(x=>x.price),cur=d.x.length?d.x[d.x.length-1].price:0,min=Math.min(cur*.88,...ps),max=Math.max(cur*1.12,...ps),span=max-min||1;$("heat").innerHTML=z.map(x=>{const top=(max-x.price)/span*100;return"<div class='zone "+(x.side==="LONG"?"long":"short")+"' style='top:"+top+"%'>"+x.side+" "+x.lev+"x · "+x.price.toFixed(0)+" · "+money(x.weight)+"</div>"}).join("")+(cur?"<div class=pxline style='top:"+((max-cur)/span*100)+"%'></div>":"");
const ls=liqs.filter(x=>x.asset===a&&x.ts>=now-86400000);$("actual").innerHTML=ls.slice(-25).reverse().map(x=>"<div class=barrow><span class='"+(x.side==="BUY"?"green":"red")+"'>"+x.side+"</span> "+x.exchange+" <span style='float:right'>"+money(x.usd)+" @ "+x.price.toLocaleString()+"</span></div>").join("")||"<div class=muted>等待公開強平事件…</div>";
let by={};for(const v of d.x)by[v.exchange]=(by[v.exchange]||0)+v.usd;let mx=Math.max(1,...Object.values(by));$("venues").innerHTML=Object.entries(by).sort((a,b)=>b[1]-a[1]).map(([k,v])=>"<div class=barrow>"+k+" <span style='float:right'>"+money(v)+"</span><div class=bar><i style='width:"+(v/mx*100)+"%'></i></div></div>").join("")||"<div class=muted>等待成交資料…</div>";
$("coverage").innerHTML=Object.entries(health).map(([k,v])=>"<span class=pill>"+k+" · "+v+"</span>").join("");
$("flow").innerHTML=[[5,"5m"],[15,"15m"],[60,"1h"],[1440,"24h"]].map(([m,n])=>{let q=calc(now-m*60000,a,th);return"<div class=flowbox><div class=label>"+n+" NET FLOW</div><div class='big "+(q.net>=0?"green":"red")+"'>"+(q.net>=0?"+":"")+money(q.net)+"</div><div class=muted>BUY "+money(q.b)+" / SELL "+money(q.q)+"</div></div>"}).join("")}
fetch("/api/snapshot").then(r=>r.json()).then(j=>{trades=j.trades||[];liqs=j.liquidations||[];books=j.books||books;zones=j.zones||zones;health=j.health||{};$("health").textContent=Object.values(health).filter(v=>v==="online").length+"/"+Object.keys(health).length+" LIVE";draw()});
const ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");ws.onmessage=e=>{let j=JSON.parse(e.data);if(j.type==="trade")trades.push(j.data);if(j.type==="liquidation")liqs.push(j.data);if(j.type==="book"){books[j.data.asset]??={};books[j.data.asset][j.data.exchange]=j.data}draw()};$("asset").onchange=draw;$("threshold").onchange=draw;setInterval(()=>{fetch("/api/snapshot").then(r=>r.json()).then(j=>{zones=j.zones||zones;health=j.health||health;draw()})},10000);
</script></body></html>`));

const wss=new WebSocketServer({server,path:"/ws"});wss.on("connection",ws=>{clients.add(ws);ws.on("close",()=>clients.delete(ws))});
setInterval(()=>{const c=Date.now()-86400000;while(trades.length&&trades[0].ts<c)trades.shift();while(liquidations.length&&liquidations[0].ts<c)liquidations.shift()},60000);
server.listen(PORT,"0.0.0.0",()=>console.log("WhaleScope v4 listening on",PORT));
