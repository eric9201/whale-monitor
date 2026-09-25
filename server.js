import express from "express";
import http from "http";
import WebSocket,{WebSocketServer} from "ws";
const app=express(),server=http.createServer(app),PORT=process.env.PORT||3000,clients=new Set();
const trades=[],liqs=[],books={BTC:{},ETH:{}},seen=new Set(),LIMIT=50000;
const state={}; const now=()=>Date.now();
const wallTracks=new Map();
const WALL_TTL=5000, WALL_BUCKET_PCT=0.0005;
function wallBucket(price){ return Math.round(Math.log(+price)/Math.log(1+WALL_BUCKET_PCT)); }
function updateWalls(exchange,asset,bids,asks){
  const t=now(), seenNow=new Set();
  for(const [side,levels] of [["BID",bids],["ASK",asks]]){
    for(const [price,qty] of levels||[]){
      const notional=(+price)*(+qty); if(!(notional>0)) continue;
      const bucket=wallBucket(price), venueKey=`${asset}:${side}:${bucket}:${exchange}`;
      seenNow.add(venueKey);
      let w=wallTracks.get(venueKey);
      if(!w) w={id:venueKey,asset,side,bucket,exchange,firstSeen:t,lastSeen:t,price:+price,current:notional,max:notional};
      else {w.lastSeen=t;w.price=+price;w.current=notional;w.max=Math.max(w.max,notional);}
      wallTracks.set(venueKey,w);
    }
  }
  for(const [k,w] of wallTracks) if(w.exchange===exchange&&w.asset===asset&&!seenNow.has(k)&&t-w.lastSeen>WALL_TTL) wallTracks.delete(k);
}
function aggregateWalls(asset,minNotional=0){
  const t=now(), groups=new Map();
  for(const w of wallTracks.values()){
    if(w.asset!==asset||t-w.lastSeen>WALL_TTL) continue;
    const k=`${w.asset}:${w.side}:${w.bucket}`; let g=groups.get(k);
    if(!g) g={id:k,asset:w.asset,side:w.side,priceNum:0,weight:0,current:0,max:0,firstSeen:w.firstSeen,lastSeen:w.lastSeen,venues:[]};
    g.priceNum+=w.price*w.current;g.weight+=w.current;g.current+=w.current;g.max+=w.max;g.firstSeen=Math.min(g.firstSeen,w.firstSeen);g.lastSeen=Math.max(g.lastSeen,w.lastSeen);g.venues.push({exchange:w.exchange,current:w.current,max:w.max});groups.set(k,g);
  }
  return [...groups.values()].map(g=>({...g,price:g.weight?g.priceNum/g.weight:0,durationMs:t-g.firstSeen,venueCount:g.venues.length}))
    .filter(g=>{let m=mid(asset);return g.current>=minNotional&&(!m||(g.side==="ASK"?g.price>=m:g.price<=m))}).sort((a,b)=>b.current-a.current);
}
const oiState={BTC:{},ETH:{}},liqHeat={BTC:new Map(),ETH:new Map()},LEV=[10,25,50,100],HEAT_TTL=86400000;
async function jget(u){let r=await fetch(u,{headers:{"user-agent":"WhaleScope/1.0"}});if(!r.ok)throw Error(r.status);return r.json()}
function mid(a){let p=[];for(let b of Object.values(books[a]||{})){if(b.bids?.[0]?.[0])p.push(+b.bids[0][0]);if(b.asks?.[0]?.[0])p.push(+b.asks[0][0])}return p.length?p.reduce((x,y)=>x+y,0)/p.length:0}
function addHeat(a,side,px,usd,lev,src,ts){if(!(px>0&&usd>0))return;let step=a==="BTC"?50:5,b=Math.round(px/step)*step,k=side+":"+b,z=liqHeat[a].get(k)||{id:k,asset:a,side,price:b,estimatedUsd:0,firstSeen:ts,lastSeen:ts,sources:{},leverages:{}};z.estimatedUsd+=usd;z.lastSeen=ts;z.sources[src]=(z.sources[src]||0)+usd;z.leverages[lev]=(z.leverages[lev]||0)+usd;liqHeat[a].set(k,z)}
function ingestOI(src,a,oi,px,ts=now()){let q=oiState[a][src];oiState[a][src]={oiUsd:oi,price:px,ts};if(!q||!(q.oiUsd>0)||ts-q.ts<1000)return;let d=oi-q.oiUsd;if(!(d>0))return;let side=px>=q.price?"LONG":"SHORT",w={10:.15,25:.30,50:.35,100:.20};for(let l of LEV){let lp=side==="LONG"?px*(1-1/l):px*(1+1/l);addHeat(a,side,lp,d*w[l],l,src,ts)}}
function heat(a){let t=now();for(let[k,z]of liqHeat[a])if(t-z.lastSeen>HEAT_TTL)liqHeat[a].delete(k);return[...liqHeat[a].values()].sort((x,y)=>y.estimatedUsd-x.estimatedUsd).slice(0,100)}
async function pollOI(){for(let a of["BTC","ETH"]){let sym=a+"USDT",px=mid(a);if(!px)continue;
try{let x=await jget("https://fapi.binance.com/fapi/v1/openInterest?symbol="+sym);ingestOI("Binance",a,+x.openInterest*px,px,+x.time||now())}catch(e){}
try{let x=await jget("https://api.bybit.com/v5/market/open-interest?category=linear&symbol="+sym+"&intervalTime=5min&limit=1"),v=x?.result?.list?.[0];if(v)ingestOI("Bybit",a,+v.openInterest*px,px,+v.timestamp||now())}catch(e){}
try{let x=await jget("https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId="+a+"-USDT-SWAP"),v=x?.data?.[0];if(v){let u=+(v.oiUsd||0);if(!(u>0))u=+(v.oiCcy||0)*px;if(u>0)ingestOI("OKX",a,u,px,+v.ts||now())}}catch(e){}
}}setInterval(pollOI,15000);setTimeout(pollOI,5000);

const flowLab={BTC:[],ETH:[]},FLOW_SAMPLE_MS=5000,FLOW_KEEP_MS=7*86400000,FLOW_WINDOWS=[15000,30000,60000,180000,300000];
function lastPrice(a){for(let i=trades.length-1;i>=0;i--)if(trades[i].asset===a)return trades[i].price;return 0}
function flowWindow(a,ms,t=now()){let b=0,se=0,c=t-ms;for(let i=trades.length-1;i>=0;i--){let x=trades[i];if(x.ts<c)break;if(x.asset!==a||x.ts>t)continue;if(x.side==="BUY")b+=x.usd;else if(x.side==="SELL")se+=x.usd}let tot=b+se;return{buy:b,sell:se,net:b-se,ratio:tot?(b-se)/tot:0,total:tot}}
function sampleFlowLab(){let t=now();for(let a of["BTC","ETH"]){let px=lastPrice(a);if(!px)continue;let row={ts:t,price:px,flow5:flowWindow(a,5000,t),flow15:flowWindow(a,15000,t),flow30:flowWindow(a,30000,t),flow60:flowWindow(a,60000,t),outcomes:{}};flowLab[a].push(row);let cut=t-FLOW_KEEP_MS;while(flowLab[a].length&&flowLab[a][0].ts<cut)flowLab[a].shift();for(let r of flowLab[a])for(let h of FLOW_WINDOWS)if(r.outcomes[h]==null&&t-r.ts>=h){let target=trades.find(x=>x.asset===a&&x.ts>=r.ts+h);if(target)r.outcomes[h]=(target.price-r.price)/r.price}}}
setInterval(sampleFlowLab,FLOW_SAMPLE_MS);setTimeout(sampleFlowLab,FLOW_SAMPLE_MS);
function flowStats(a,field="flow15"){let rows=flowLab[a],bins=[[-1,-.5,"< -50%"],[-.5,-.25,"-50~-25%"],[-.25,-.1,"-25~-10%"],[-.1,.1,"-10~+10%"],[.1,.25,"+10~25%"],[.25,.5,"+25~50%"],[.5,1.000001,"> +50%"]];return bins.map(([lo,hi,label])=>{let q=rows.filter(r=>r[field]&&r[field].ratio>=lo&&r[field].ratio<hi),o={label,n:q.length};for(let h of FLOW_WINDOWS){let v=q.map(r=>r.outcomes[h]).filter(Number.isFinite);o["n"+h]=v.length;o["mean"+h]=v.length?v.reduce((x,y)=>x+y,0)/v.length:null;o["win"+h]=v.length?v.filter(x=>x>0).length/v.length:null}return o})}


const liqPaper={BTC:{open:null,trades:[]},ETH:{open:null,trades:[]}},LIQ_PAPER_SAMPLE=5000;
// Entry: dominant SHORT liquidation concentration => LONG; dominant LONG => SHORT.
// Exit: hold until price reaches the nearest opposite-side liquidation price beyond entry, locked at entry.
// No fixed holding time. Paper only.
function strongestOppositeTarget(a,entrySide,px){
 let want=entrySide==="LONG"?"LONG":"SHORT"; // LONG trade targets LONG-liq zone; SHORT trade targets SHORT-liq zone
 let candidates=heat(a).filter(x=>x.side===want && (entrySide==="LONG"?x.price>px:x.price<px));
 if(!candidates.length)return null;
 candidates.sort((x,y)=>Math.abs(x.price-px)-Math.abs(y.price-px));
 return candidates[0];
}
function liqPaperTick(){
 let t=now();
 for(let a of ["BTC","ETH"]){
  let px=lastPrice(a),st=liqPaper[a]; if(!px)continue;
  if(st.open){
   let hit=st.open.side==="LONG"?px>=st.open.targetPrice:px<=st.open.targetPrice;
   if(hit){
    let raw=(px-st.open.price)/st.open.price*(st.open.side==="LONG"?1:-1);
    st.trades.push({...st.open,exitTs:t,exitPrice:px,rawReturn:raw,exitReason:"OPPOSITE_LIQ_TARGET_HIT"});
    if(st.trades.length>5000)st.trades.shift(); st.open=null;
   }
  }
  if(st.open)continue;
  let hm=heat(a),near=hm.filter(x=>Math.abs(x.price-px)/px<=.12);
  let longUsd=near.filter(x=>x.side==="LONG").reduce((z,x)=>z+x.estimatedUsd,0);
  let shortUsd=near.filter(x=>x.side==="SHORT").reduce((z,x)=>z+x.estimatedUsd,0);
  if(!(longUsd>0||shortUsd>0))continue;
  let side=longUsd>shortUsd?"SHORT":shortUsd>longUsd?"LONG":null;
  if(!side)continue;
  let target=strongestOppositeTarget(a,side,px);
  if(target)st.open={ts:t,price:px,side,longLiqUsd:longUsd,shortLiqUsd:shortUsd,targetPrice:target.price,targetUsd:target.estimatedUsd,targetLiqSide:target.side};
 }
}
setInterval(liqPaperTick,LIQ_PAPER_SAMPLE);setTimeout(liqPaperTick,10000);
function liqPaperStats(a){
 let st=liqPaper[a],q=st.trades,n=q.length,w=q.filter(x=>x.rawReturn>0).length,
 avg=n?q.reduce((z,x)=>z+x.rawReturn,0)/n:null,
 grossWin=q.filter(x=>x.rawReturn>0).reduce((z,x)=>z+x.rawReturn,0),
 grossLoss=-q.filter(x=>x.rawReturn<0).reduce((z,x)=>z+x.rawReturn,0);
 return{mode:"PAPER_ONLY",rule:"SHORT_LIQ=>LONG / LONG_LIQ=>SHORT; EXIT_AT_ENTRY_OPPOSITE_LIQ_TARGET",n,winRate:n?w/n:null,avgReturn:avg,profitFactor:grossLoss?grossWin/grossLoss:null,open:st.open,recent:q.slice(-20).reverse()};
}

const burstBins=new Map(), BURST_MS=3000;
function whaleBursts(asset,threshold,cutoff){
  const bins=new Map();
  for(const x of trades){ if(x.asset!==asset||x.ts<cutoff) continue;
    const bin=Math.floor(x.ts/BURST_MS), k=`${x.exchange}:${x.side}:${bin}`;
    let b=bins.get(k); if(!b)b={exchange:x.exchange,asset:x.asset,side:x.side,start:bin*BURST_MS,end:(bin+1)*BURST_MS,usd:0,count:0,priceNum:0};
    b.usd+=x.usd;b.count++;b.priceNum+=x.price*x.usd;bins.set(k,b);
  }
  return [...bins.values()].filter(b=>b.usd>=threshold).map(b=>({...b,price:b.usd?b.priceNum/b.usd:0})).sort((a,b)=>a.start-b.start);
}
const init=n=>state[n]??={socket:"connecting",lastTrade:0,lastBook:0,lastLiq:0,error:""};
["Binance","Bybit","OKX","Hyperliquid","Bitget"].forEach(init);
function emit(type,data){const s=JSON.stringify({type,data});for(const c of clients)if(c.readyState===WebSocket.OPEN)c.send(s)}
function putTrade(exchange,asset,side,price,qty,id,ts){price=+price;qty=+qty;const usd=price*qty,key=exchange+":"+id;if(!Number.isFinite(usd)||usd<=0||seen.has(key))return;seen.add(key);if(seen.size>100000)seen.clear();const x={exchange,asset,side,price,qty,usd,id:String(id),ts:+ts||now()};trades.push(x);if(trades.length>LIMIT)trades.shift();state[exchange].lastTrade=now();emit("trade",x)}
function putLiq(exchange,asset,side,price,qty,ts){price=+price;qty=+qty;const usd=price*qty;if(!(usd>0))return;const x={exchange,asset,side,price,qty,usd,ts:+ts||now()};liqs.push(x);if(liqs.length>10000)liqs.shift();state[exchange].lastLiq=now();emit("liquidation",x)}
function putBook(exchange,asset,bids,asks){books[asset][exchange]={bids,asks,ts:now()};updateWalls(exchange,asset,bids,asks);state[exchange].lastBook=now();emit("book",{exchange,asset,bids,asks,ts:now()})}
function connect(name,make){let delay=1000;const go=()=>{state[name].socket="connecting";try{const ws=make();ws.on("open",()=>{state[name].socket="connected";delay=1000});ws.on("error",e=>{state[name].error=String(e.message||e).slice(0,120);try{ws.close()}catch{}});ws.on("close",()=>{state[name].socket="reconnecting";setTimeout(go,delay);delay=Math.min(30000,delay*2)})}catch(e){state[name].error=String(e);setTimeout(go,delay)}};go()}
function health(){let o={};for(const [k,v] of Object.entries(state)){const age=v.lastTrade?now()-v.lastTrade:null;o[k]={...v,tradeAgeMs:age,status:v.socket!=="connected"?"OFFLINE":age!==null&&age<60000?"LIVE":"NO_DATA"}}return o}

// Binance: isolate high-frequency feeds so a book stream cannot mask trade-feed failure.
// Binance 2026 /public endpoint, raw path-based streams.
function binanceRaw(label,stream,onData){
  let delay=1000;
  const go=()=>{
    const url="wss://fstream.binance.com/public/ws/"+stream;
    const ws=new WebSocket(url);
    state.Binance.feeds??={};
    const d=state.Binance.feeds[label]??={};
    Object.assign(d,{stream,url,socket:"connecting",error:""});
    ws.on("open",()=>{d.socket="connected";d.openedAt=now();delay=1000;console.log(`[Binance-${label}] OPEN ${stream}`)});
    ws.on("message",b=>{try{
      const x=JSON.parse(b); d.messageCount=(d.messageCount||0)+1;d.lastMessage=now();
      if(d.messageCount===1)console.log(`[Binance-${label}] FIRST_MESSAGE ${stream}`);
      if(x.code!=null){d.error=`WS ${x.code}: ${x.msg}`;console.error(`[Binance-${label}]`,d.error);return}
      onData(x);
    }catch(e){d.error=String(e.message||e);console.error(`[Binance-${label}] PARSE`,d.error)}});
    ws.on("error",e=>{d.error=String(e.message||e);console.error(`[Binance-${label}] ERROR`,d.error)});
    ws.on("close",(code,reason)=>{d.socket="reconnecting";d.closeCode=code;d.closeReason=String(reason);console.log(`[Binance-${label}] CLOSE`,code,String(reason));setTimeout(go,delay);delay=Math.min(30000,delay*2)});
  };go();
}
for(const A of ["BTC","ETH"]){
  const sym=A.toLowerCase()+"usdt";
  binanceRaw(`TRADE-${A}`,`${sym}@aggTrade`,x=>{
    putTrade("Binance",A,x.m?"SELL":"BUY",x.p,x.q,x.a,x.T);
    state.Binance.lastTradeStream=A;
  });
  binanceRaw(`BOOK-${A}`,`${sym}@depth20@100ms`,x=>{
    putBook("Binance",A,(x.b||[]).map(v=>[+v[0],+v[1]]),(x.a||[]).map(v=>[+v[0],+v[1]]));
  });
  binanceRaw(`LIQ-${A}`,`${sym}@forceOrder`,x=>{
    const o=x.o;if(o)putLiq("Binance",A,o.S,+o.ap||+o.p,o.q,o.T);
  });
}
// Aggregate socket state for the existing UI. LIVE still requires recent trades via health().
setInterval(()=>{
 const f=Object.values(state.Binance.feeds||{});
 const tradeFeeds=f.filter(x=>x.stream?.includes("@aggTrade"));
 state.Binance.socket=tradeFeeds.length&&tradeFeeds.every(x=>x.socket==="connected")?"connected":"reconnecting";
 state.Binance.messageCount=f.reduce((n,x)=>n+(x.messageCount||0),0);
 state.Binance.lastMessage=Math.max(0,...f.map(x=>x.lastMessage||0));
 state.Binance.error=f.map(x=>x.error).filter(Boolean).join(" | ").slice(0,240);
},1000);

// Bybit with orderbook delta merge
const bb={BTC:{b:new Map(),a:new Map()},ETH:{b:new Map(),a:new Map()}};
connect("Bybit",()=>{const ws=new WebSocket("wss://stream.bybit.com/v5/public/linear");ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:["publicTrade.BTCUSDT","publicTrade.ETHUSDT","allLiquidation.BTCUSDT","allLiquidation.ETHUSDT","orderbook.50.BTCUSDT","orderbook.50.ETHUSDT"]})));ws.on("message",b=>{try{const j=JSON.parse(b),t=j.topic||"";if(t.startsWith("publicTrade"))for(const x of j.data||[])putTrade("Bybit",x.s.startsWith("ETH")?"ETH":"BTC",String(x.S).toUpperCase(),x.p,x.v,x.i,x.T);else if(t.startsWith("allLiquidation"))for(const x of j.data||[])putLiq("Bybit",x.s.startsWith("ETH")?"ETH":"BTC",x.S,x.p,x.v,x.T);else if(t.startsWith("orderbook")&&j.data){const A=j.data.s.startsWith("ETH")?"ETH":"BTC",ob=bb[A];if(j.type==="snapshot"){ob.b.clear();ob.a.clear()}for(const [p,q] of j.data.b||[]){+q===0?ob.b.delete(p):ob.b.set(p,+q)}for(const [p,q] of j.data.a||[]){+q===0?ob.a.delete(p):ob.a.set(p,+q)}putBook("Bybit",A,[...ob.b].map(([p,q])=>[+p,q]).sort((a,b)=>b[0]-a[0]).slice(0,50),[...ob.a].map(([p,q])=>[+p,q]).sort((a,b)=>a[0]-b[0]).slice(0,50))}}catch(e){state.Bybit.error=String(e)}});return ws});

// OKX: correct USDT swap contract value
const cv={BTC:.01,ETH:.1};
connect("OKX",()=>{const ws=new WebSocket("wss://ws.okx.com:8443/ws/v5/public");ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:["BTC-USDT-SWAP","ETH-USDT-SWAP"].flatMap(instId=>[{channel:"trades",instId},{channel:"books5",instId}])})));ws.on("message",b=>{try{const j=JSON.parse(b),A=(j.arg?.instId||"").startsWith("ETH")?"ETH":"BTC";if(j.arg?.channel==="trades")for(const x of j.data||[])putTrade("OKX",A,x.side==="buy"?"BUY":"SELL",x.px,+x.sz*cv[A],x.tradeId,x.ts);if(j.arg?.channel==="books5"&&j.data?.[0]){const d=j.data[0];putBook("OKX",A,(d.bids||[]).map(v=>[+v[0],+v[1]*cv[A]]),(d.asks||[]).map(v=>[+v[0],+v[1]*cv[A]]))}}catch(e){state.OKX.error=String(e)}});return ws});

// Hyperliquid
connect("Hyperliquid",()=>{const ws=new WebSocket("wss://api.hyperliquid.xyz/ws");ws.on("open",()=>["BTC","ETH"].forEach(coin=>{ws.send(JSON.stringify({method:"subscribe",subscription:{type:"trades",coin}}));ws.send(JSON.stringify({method:"subscribe",subscription:{type:"l2Book",coin}}))}));ws.on("message",b=>{try{const j=JSON.parse(b);if(j.channel==="trades"){const arr=Array.isArray(j.data)?j.data:(Array.isArray(j.data?.trades)?j.data.trades:[]);for(const x of arr)putTrade("Hyperliquid",x.coin,x.side==="B"?"BUY":"SELL",x.px,x.sz,x.hash||x.tid||x.time,x.time)}if(j.channel==="l2Book"&&j.data){const A=j.data.coin,lv=j.data.levels||[[],[]];if(A==="BTC"||A==="ETH")putBook("Hyperliquid",A,(lv[0]||[]).map(x=>[+x.px,+x.sz]),(lv[1]||[]).map(x=>[+x.px,+x.sz]))}}catch(e){state.Hyperliquid.error=String(e)}});return ws});

// Bitget validated public futures trade feed
connect("Bitget",()=>{const ws=new WebSocket("wss://ws.bitget.com/v2/ws/public");ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:[{instType:"USDT-FUTURES",channel:"trade",instId:"BTCUSDT"},{instType:"USDT-FUTURES",channel:"trade",instId:"ETHUSDT"}]})));ws.on("message",b=>{try{const j=JSON.parse(b);if(j.arg?.channel!=="trade")return;const A=j.arg.instId.startsWith("ETH")?"ETH":"BTC";for(const x of j.data||[])putTrade("Bitget",A,x.side==="buy"?"BUY":"SELL",x.price,x.size,x.tradeId||x.ts,x.ts)}catch(e){state.Bitget.error=String(e)}});return ws});

function zones(asset){
 const px=(()=>{for(let i=trades.length-1;i>=0;i--)if(trades[i].asset===asset)return trades[i].price;return 0})();if(!px)return[];
 // Model-only levels: no fake dollar liquidity labels.
 return [10,25,50,100].flatMap(L=>[{side:"LONG",lev:L,price:px*(1-1/L)},{side:"SHORT",lev:L,price:px*(1+1/L)}]);
}
app.get("/health",(_,r)=>r.json({ok:true,uptime:process.uptime(),sources:health(),buffer:trades.length}));
app.get("/api/liquidation-paper",(_,r)=>r.json({BTC:liqPaperStats("BTC"),ETH:liqPaperStats("ETH")}));
app.get("/api/flow-validation",(_,r)=>r.json({mode:"OOS_FORWARD_ONLY",sampleMs:FLOW_SAMPLE_MS,retentionMs:FLOW_KEEP_MS,horizonsMs:FLOW_WINDOWS,BTC:{samples:flowLab.BTC.length,stats:flowStats("BTC")},ETH:{samples:flowLab.ETH.length,stats:flowStats("ETH")}}));
app.get("/api/snapshot",(_,r)=>{const c=now()-86400000, recent=trades.filter(x=>x.ts>=c);
 const marketDistribution={}; for(const x of recent){marketDistribution[x.asset]??={};marketDistribution[x.asset][x.exchange]=(marketDistribution[x.asset][x.exchange]||0)+x.usd;}
 r.json({trades:recent.slice(-30000),liquidations:liqs.filter(x=>x.ts>=c),books,health:health(),
 walls:{BTC:aggregateWalls("BTC"),ETH:aggregateWalls("ETH")},
 bursts:{BTC:{100000:whaleBursts("BTC",100000,c),500000:whaleBursts("BTC",500000,c),1000000:whaleBursts("BTC",1000000,c)},ETH:{100000:whaleBursts("ETH",100000,c),500000:whaleBursts("ETH",500000,c),1000000:whaleBursts("ETH",1000000,c)}},
 marketDistribution,liquidationMap:{status:"ESTIMATED_OI_MODEL",BTC:heat("BTC"),ETH:heat("ETH")}})});

app.get("/",(_,r)=>r.type("html").send(`<!doctype html><html lang="zh-Hant"><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>WhaleScope</title><style>
*{box-sizing:border-box}body{margin:0;background:#030711;color:#eaf1ff;font-family:system-ui;background-image:radial-gradient(circle at 50% 0,#1b0c4e55,transparent 38%)}main{max-width:1500px;margin:auto;padding:14px}.head{display:flex;gap:12px;align-items:center;flex-wrap:wrap}.logo{font-size:30px;font-weight:900;color:#b45cff}select{background:#081020;border:1px solid #24466d;color:white;padding:10px;border-radius:9px}.cards,.grid,.flow{display:grid;gap:10px;margin-top:10px}.cards{grid-template-columns:repeat(4,1fr)}.grid{grid-template-columns:1.15fr 1fr}.flow{grid-template-columns:repeat(4,1fr)}.card{background:#06101de8;border:1px solid #173c63;border-radius:13px;padding:14px}.title{font-size:18px;font-weight:800;margin-bottom:10px}.label,.muted{color:#8fa1c4;font-size:12px}.big{font-size:27px;font-weight:900}.g{color:#35efa6}.r{color:#ff5a78}.p{color:#a86cff}.scroll{max-height:390px;overflow:auto}table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:8px 5px;border-bottom:1px solid #12233d;text-align:right}td:first-child,th:first-child{text-align:left}.pill{display:inline-block;padding:5px 8px;border:1px solid #274e77;border-radius:20px;margin:3px;font-size:11px}.heat{height:360px;position:relative;border:1px solid #17304f;border-radius:9px;overflow:hidden;background:linear-gradient(180deg,#06111d,#071522)}.z{position:absolute;left:0;right:0;height:12px;padding-left:7px;font-size:10px;transform:translateY(-50%)}.zl{background:linear-gradient(90deg,#16e99b22,#16e99bcc,#16e99b22)}.zs{background:linear-gradient(90deg,#ff557522,#ff5575cc,#ff557522)}.bar{height:8px;background:#10213a;border-radius:8px;overflow:hidden}.bar i{display:block;height:100%;background:linear-gradient(90deg,#24e4a3,#8c62ff)}@media(max-width:750px){.cards,.grid,.flow{grid-template-columns:1fr 1fr}}@media(max-width:520px){.grid,.flow{grid-template-columns:1fr}}
.lf{background:#09182a;color:#9fb0cb;border:1px solid #24466f;border-radius:7px;padding:7px 10px;font-weight:700}.lf.on{color:#fff;border-color:#8c62ff;box-shadow:0 0 10px #8c62ff55}</style></head><body><main><div class=head><div><div class=logo>◉ WhaleScope</div><div class=muted>Verified CEX + On-chain Intelligence</div></div><select id=a><option>BTC</option><option>ETH</option></select><span class=pill>INTEGRATED WHALE FLOW</span><b class=g>● <span id=live>CHECKING</span></b></div>
<div class=cards><div class=card><div class=label>24H 巨鯨成交</div><div id=total class=big>$0</div></div><div class=card><div class=label>24H 主動買入</div><div id=buy class="big g">$0</div></div><div class=card><div class=label>24H 主動賣出</div><div id=sell class="big r">$0</div></div><div class=card><div class=label>資金淨流入</div><div id=net class="big p">$0</div></div></div>
<div class=grid><div class=card><div class=title>巨鯨大額成交</div><div class=scroll><table><thead><tr><th>時間</th><th>方向</th><th>金額</th><th>來源</th></tr></thead><tbody id=tr></tbody></table></div></div><div class=card><div class=title>大戶掛單點位</div><div class=scroll><table><thead><tr><th>類型</th><th>價格</th><th>金額</th><th>來源</th></tr></thead><tbody id=wa></tbody></table></div></div></div>
<div class=grid><div class=card><div class=title>預估清算資金熱圖</div><div class=liqfilters style="display:flex;gap:6px;flex-wrap:wrap;margin:0 0 10px"><button class="lf on" data-f="all">全部</button><button class=lf data-f="lt500">&lt; $500K</button><button class=lf data-f="ge500">≥ $500K</button><button class=lf data-f="lt1m">&lt; $1M</button><button class=lf data-f="ge1m">≥ $1M</button></div><div id=heat class=heat></div><div class=muted>ESTIMATED LIQUIDATION HEATMAP：Binance / Bybit / OKX 公開 OI Δ 建模；槓桿僅在模型內計算，畫面整合為價格區間與預估清算資金強度。</div></div><div class=card><div class=title>實際公開強平</div><div id=liq class=scroll></div></div></div>
<div class=grid><div class=card><div class=title>交易所成交資金分布</div><div id=ven></div><div class=muted>只計入真正收到且成功解析的公開成交資料。</div></div><div class=card><div class=title>資料來源驗證</div><div id=src></div></div></div>
<div class=card style="margin-top:10px"><div class=title>資金流向 · MONEY FLOW</div><div id=flow class=flow></div></div><div class=card style="margin-top:10px"><div class=title>FLOW LEAD/LAG · OOS 驗證</div><div class=muted>每 5 秒凍結當下 Flow，之後才補 15s / 30s / 1m / 3m / 5m forward return；不使用未來資料。</div><div id=flab class=scroll style="margin-top:8px"></div></div><div class=card style="margin-top:10px"><div class=title>清算反向 · PAPER TEST</div><div class=muted>規則：SHORT 清算資金較強 → 開多；LONG 清算資金較強 → 開空。進場時鎖定價格方向上最近的第一個反向清算價，價格觸及該目標才平倉；持倉期間不重複開倉。僅模擬、不送真實訂單。</div><div id=lpaper style="margin-top:8px"></div></div>
</main><script>
let D={trades:[],liquidations:[],books:{BTC:{},ETH:{}},health:{},walls:{BTC:[],ETH:[]},bursts:{BTC:{},ETH:{}},marketDistribution:{},liquidationMap:{}};let LIQ_FILTER="all";function liqFilterOk(x){let u=x.estimatedUsd||0;return LIQ_FILTER==="all"||(LIQ_FILTER==="lt500"&&u<5e5)||(LIQ_FILTER==="ge500"&&u>=5e5)||(LIQ_FILTER==="lt1m"&&u<1e6)||(LIQ_FILTER==="ge1m"&&u>=1e6)}function bindLiqFilters(){document.querySelectorAll(".lf").forEach(b=>b.onclick=()=>{LIQ_FILTER=b.dataset.f;document.querySelectorAll(".lf").forEach(x=>x.classList.toggle("on",x===b));draw()})}const $=x=>document.getElementById(x),M=n=>"$"+Intl.NumberFormat("en",{notation:"compact",maximumFractionDigits:2}).format(n||0);
function C(min,A){let x=D.trades.filter(v=>v.asset===A&&v.ts>Date.now()-min*60000),b=x.filter(v=>v.side==="BUY").reduce((s,v)=>s+v.usd,0),s=x.filter(v=>v.side==="SELL").reduce((q,v)=>q+v.usd,0);return{x,b,s,n:b-s}}
function B(A,T,min=1440){return ((D.bursts?.[A]?.[String(T)])||[]).filter(x=>x.start>Date.now()-min*60000)}
function dur(ms){let q=Math.max(0,Math.floor(ms/1000)),h=Math.floor(q/3600),m=Math.floor(q%3600/60),s=q%60;return(h?h+"h ":"")+(m?m+"m ":"")+s+"s"}
function draw(){let A=$("a").value,T=100000,q=C(1440,A),bur=B(A,T);let wb=bur.filter(v=>v.side==="BUY").reduce((z,v)=>z+v.usd,0),ws=bur.filter(v=>v.side==="SELL").reduce((z,v)=>z+v.usd,0);$("total").textContent=M(wb+ws);$("buy").textContent=M(wb);$("sell").textContent=M(ws);$("net").textContent=(wb-ws>=0?"+":"")+M(wb-ws);$("tr").innerHTML=bur.slice(-40).reverse().map(v=>"<tr><td>"+new Date(v.start).toLocaleTimeString()+"</td><td class="+(v.side==="BUY"?"g":"r")+">"+v.side+"</td><td>"+M(v.usd)+" <span class=muted>("+v.count+" fills)</span></td><td>"+v.exchange+"</td></tr>").join("")||"<tr><td colspan=4 class=muted>等待 3 秒聚合後符合門檻的巨鯨成交…</td></tr>";
let w=(D.walls?.[A]||[]).filter(x=>x.current>=100000);$("wa").innerHTML=w.slice(0,50).map(x=>"<tr><td class="+(x.side==="BID"?"g":"r")+">"+x.side+"</td><td>"+x.price.toLocaleString(undefined,{maximumFractionDigits:1})+"</td><td>"+M(x.current)+"<div class=muted>"+x.venueCount+" venues · "+dur(x.durationMs)+" · max "+M(x.max)+"</div></td><td>"+x.venues.map(v=>v.exchange).join(", ")+"</td></tr>").join("")||"<tr><td colspan=4 class=muted>目前沒有符合門檻的持續掛單</td></tr>";
let hm=(D.liquidationMap?.[A]||[]).slice(),last=[...(D.trades||[])].reverse().find(x=>x.asset===A)?.price||null;
if(hm.length){
  let step=A==="BTC"?100:10,bins=new Map();
  for(let x of hm){
    let p=Math.round(x.price/step)*step,k=x.side+":"+p,
        q=bins.get(k)||{side:x.side,price:p,estimatedUsd:0};
    q.estimatedUsd+=x.estimatedUsd||0; bins.set(k,q);
  }
  let rows=[...bins.values()].filter(liqFilterOk);
  if(last) rows=rows.filter(x=>Math.abs(x.price-last)/last<=.15);
  rows.sort((a,b)=>b.price-a.price);
  if(!rows.length){$("heat").innerHTML="<div class=muted style='padding:18px'>目前此資金門檻沒有清算區。</div>";return;}
  let maxH=Math.max(1,...rows.map(x=>x.estimatedUsd)),H=330,top=14,minGap=25;
  // Preserve price ordering while enforcing enough vertical separation for mobile labels.
  let pmax=Math.max(...rows.map(x=>x.price)),pmin=Math.min(...rows.map(x=>x.price));
  let placed=[];
  for(let x of rows){
    let y=pmax===pmin?H/2:top+(pmax-x.price)/(pmax-pmin)*(H-2*top);
    if(placed.length && y-placed[placed.length-1].y<minGap)y=placed[placed.length-1].y+minGap;
    if(y>H-16)continue;
    placed.push({...x,y});
  }
  $("heat").innerHTML=placed.map(x=>{
    let w=Math.max(10,Math.min(94,94*x.estimatedUsd/maxH)),
        cls=x.side==="LONG"?"z zl":"z zs";
    return "<div class='"+cls+"' style='position:absolute;left:0;top:"+x.y+"px;width:"+w+"%;height:22px;line-height:22px;padding-left:12px;white-space:nowrap;overflow:visible;z-index:2;font-size:14px;transform:none'>"+
      x.side+" LIQ · $"+Math.round(x.price).toLocaleString()+" · "+M(x.estimatedUsd)+"</div>";
  }).join("")+(last?"<div style='position:absolute;left:0;right:0;top:50%;border-top:1px dashed #8fa7c7;opacity:.45;z-index:1'></div>":"");
}else $("heat").innerHTML="<div class=muted style='padding:18px'>正在累積 OI Δ；至少需要兩次 OI snapshot 才會產生清算資金強度。</div>";
$("liq").innerHTML=D.liquidations.filter(x=>x.asset===A).slice(-30).reverse().map(x=>"<div style='padding:7px;border-bottom:1px solid #13243d'><b class="+(x.side==="BUY"?"g":"r")+">"+x.side+"</b> "+x.exchange+" <span style=float:right>"+M(x.usd)+" @ "+x.price.toLocaleString()+"</span></div>").join("")||"<div class=muted>等待 Binance / Bybit 公開強平事件…</div>";
let by=D.marketDistribution?.[A]||{};let mx=Math.max(1,...Object.values(by));$("ven").innerHTML=Object.entries(by).sort((a,b)=>b[1]-a[1]).map(([e,v])=>"<div style='margin:9px 0'>"+e+" <span style=float:right>"+M(v)+"</span><div class=bar><i style='width:"+(v/mx*100)+"%'></i></div></div>").join("")||"<div class=muted>等待符合門檻成交…</div>";
let hs=Object.entries(D.health),live=hs.filter(([e,v])=>v.status==="LIVE").length;$("live").textContent=live+"/"+hs.length+" VERIFIED LIVE";$("src").innerHTML=hs.map(([e,v])=>"<span class=pill>"+e+" · "+v.status+(v.tradeAgeMs!=null?" · "+Math.round(v.tradeAgeMs/1000)+"s":"")+"</span>").join("");
$("flow").innerHTML=[[5,"5m"],[15,"15m"],[60,"1h"],[1440,"24h"]].map(([m,n])=>{let x=C(m,A);return"<div class=card><div class=label>"+n+" NET</div><div class='big "+(x.n>=0?"g":"r")+"'>"+(x.n>=0?"+":"")+M(x.n)+"</div><div class=muted>BUY "+M(x.b)+" / SELL "+M(x.s)+"</div></div>"}).join("")}
async function snap(){try{let [d,v,p]=await Promise.all([fetch("/api/snapshot",{cache:"no-store"}).then(r=>r.json()),fetch("/api/flow-validation",{cache:"no-store"}).then(r=>r.json()),fetch("/api/liquidation-paper",{cache:"no-store"}).then(r=>r.json())]);D=d;draw();let A=$("a").value,st=v[A]?.stats||[];$("flab").innerHTML="<table><thead><tr><th>15s FlowRatio</th><th>N</th><th>+15s</th><th>+1m</th><th>+5m</th></tr></thead><tbody>"+st.map(x=>"<tr><td>"+x.label+"</td><td>"+x.n+"</td><td>"+(x.mean15000==null?"—":(x.mean15000*100).toFixed(3)+"% / "+(x.win15000*100).toFixed(0)+"% win")+"</td><td>"+(x.mean60000==null?"—":(x.mean60000*100).toFixed(3)+"% / "+(x.win60000*100).toFixed(0)+"% win")+"</td><td>"+(x.mean300000==null?"—":(x.mean300000*100).toFixed(3)+"% / "+(x.win300000*100).toFixed(0)+"% win")+"</td></tr>").join("")+"</tbody></table><div class=muted>Samples: "+(v[A]?.samples||0)+" · OOS_FORWARD_ONLY</div>";let lp=p[A]||{};$("lpaper").innerHTML="<div class=flow><div><div class=label>獨立交易</div><div class=big>"+(lp.n||0)+"</div></div><div><div class=label>勝率</div><div class=big>"+(lp.winRate==null?"—":(lp.winRate*100).toFixed(1)+"%")+"</div></div><div><div class=label>平均每筆報酬</div><div class=big>"+(lp.avgReturn==null?"—":(lp.avgReturn*100).toFixed(3)+"%")+"</div></div><div><div class=label>PF</div><div class=big>"+(lp.profitFactor==null?"—":lp.profitFactor.toFixed(2))+"</div></div></div><div class=muted>"+(lp.open?("OPEN "+lp.open.side+" @ "+Math.round(lp.open.price).toLocaleString()+" → TARGET "+Math.round(lp.open.targetPrice).toLocaleString()+" ("+lp.open.targetLiqSide+" LIQ "+M(lp.open.targetUsd)+") · LONG LIQ "+M(lp.open.longLiqUsd)+" / SHORT LIQ "+M(lp.open.shortLiqUsd)):"目前無持倉")+"</div>"}catch(e){$("live").textContent="API ERROR"}}bindLiqFilters();snap();setInterval(snap,5000);$("a").onchange=draw;
const ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");ws.onmessage=e=>{let j=JSON.parse(e.data);if(j.type==="trade"){D.trades.push(j.data);if(D.trades.length>15000)D.trades.shift()}else if(j.type==="liquidation")D.liquidations.push(j.data);else if(j.type==="book"){D.books[j.data.asset]??={};D.books[j.data.asset][j.data.exchange]=j.data}draw()};
</script></body></html>`));
const wss=new WebSocketServer({server,path:"/ws"});wss.on("connection",ws=>{clients.add(ws);ws.on("close",()=>clients.delete(ws))});
setInterval(()=>{const c=now()-86400000;while(trades.length&&trades[0].ts<c)trades.shift();while(liqs.length&&liqs[0].ts<c)liqs.shift()},60000);
server.listen(PORT,"0.0.0.0",()=>console.log("WhaleScope v9.8 Binance isolated raw feeds on",PORT));
