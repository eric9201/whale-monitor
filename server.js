import express from "express";
import http from "http";
import WebSocket,{WebSocketServer} from "ws";

const app=express(), server=http.createServer(app), PORT=process.env.PORT||3000;
const clients=new Set(), trades=[], liquidations=[], walls=new Map(), health={};
const MAX_TRADES=30000, MAX_LIQ=10000;
const now=()=>Date.now();
const num=x=>Number(x);
const assetOf=s=>String(s).toUpperCase().includes("ETH")?"ETH":"BTC";
function send(type,data){const m=JSON.stringify({type,data});for(const c of clients)if(c.readyState===WebSocket.OPEN)c.send(m)}
function pushTrade(x){if(!Number.isFinite(x.usd)||x.usd<=0)return;trades.push(x);if(trades.length>MAX_TRADES)trades.shift();send("trade",x)}
function pushLiq(x){if(!Number.isFinite(x.usd)||x.usd<=0)return;liquidations.push(x);if(liquidations.length>MAX_LIQ)liquidations.shift();send("liquidation",x)}
function setWalls(exchange,asset,bids,asks){
  const spot=Math.max(num(bids?.[0]?.[0]||0),num(asks?.[0]?.[0]||0));
  const pack=(a,side)=>a.map(v=>({exchange,asset,side,price:num(v[0]),qty:num(v[1]),usd:num(v[0])*num(v[1]),spot,ts:now()}))
    .filter(v=>v.usd>=100000).sort((a,b)=>b.usd-a.usd).slice(0,20);
  walls.set(exchange+":"+asset,[...pack(bids,"BID"),...pack(asks,"ASK")]); send("walls",[...walls.values()].flat())
}
function reconnect(name,make){
 let delay=1000,timer=null;
 const go=()=>{health[name]="connecting";send("health",health);try{
   const ws=make();
   ws.on("open",()=>{health[name]="online";delay=1000;send("health",health)});
   ws.on("close",()=>{health[name]="reconnecting";send("health",health);timer=setTimeout(go,delay);delay=Math.min(delay*2,30000)});
   ws.on("error",()=>{try{ws.close()}catch{}});
 }catch(e){health[name]="error";send("health",health);timer=setTimeout(go,delay)}};
 go();
}

// Binance trades + liquidation
reconnect("Binance",()=>{
 const ws=new WebSocket("wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@forceOrder/ethusdt@forceOrder/btcusdt@depth20@500ms/ethusdt@depth20@500ms");
 ws.on("message",b=>{const j=JSON.parse(b),x=j.data||{},stream=j.stream||"";
  if(stream.includes("@aggTrade")){const p=num(x.p),q=num(x.q);pushTrade({exchange:"Binance",asset:assetOf(x.s),side:x.m?"SELL":"BUY",price:p,qty:q,usd:p*q,id:String(x.a),ts:num(x.T)})}
  else if(stream.includes("@forceOrder")){const o=x.o||{},p=num(o.ap||o.p),q=num(o.z||o.q);pushLiq({exchange:"Binance",asset:assetOf(o.s),side:o.S==="SELL"?"LONG":"SHORT",price:p,qty:q,usd:p*q,ts:num(o.T||x.E)})}
  else if(stream.includes("@depth20"))setWalls("Binance",assetOf(stream),x.b||[],x.a||[]);
 });return ws
});

// Bybit trades + liquidation + orderbook
reconnect("Bybit",()=>{
 const ws=new WebSocket("wss://stream.bybit.com/v5/public/linear");
 ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:[
  "publicTrade.BTCUSDT","publicTrade.ETHUSDT","allLiquidation.BTCUSDT","allLiquidation.ETHUSDT","orderbook.50.BTCUSDT","orderbook.50.ETHUSDT"
 ]})));
 ws.on("message",b=>{const j=JSON.parse(b),topic=j.topic||"",d=j.data;
  if(topic.startsWith("publicTrade."))for(const x of d||[]){const p=num(x.p),q=num(x.v);pushTrade({exchange:"Bybit",asset:assetOf(x.s),side:String(x.S).toUpperCase(),price:p,qty:q,usd:p*q,id:String(x.i||x.T),ts:num(x.T)})}
  else if(topic.startsWith("allLiquidation."))for(const x of (Array.isArray(d)?d:[d]).filter(Boolean)){const p=num(x.p),q=num(x.v);pushLiq({exchange:"Bybit",asset:assetOf(x.s||topic),side:String(x.S).toUpperCase()==="SELL"?"LONG":"SHORT",price:p,qty:q,usd:p*q,ts:num(x.T||j.ts||now())})}
  else if(topic.startsWith("orderbook."))setWalls("Bybit",assetOf(topic),d?.b||[],d?.a||[]);
 });return ws
});

// OKX public trades + books5. Contract sizes are converted for BTC/ETH USDT swaps.
const OKX_CT={BTC:0.01,ETH:0.1};
reconnect("OKX",()=>{
 const ws=new WebSocket("wss://ws.okx.com:8443/ws/v5/public");
 ws.on("open",()=>ws.send(JSON.stringify({op:"subscribe",args:["BTC-USDT-SWAP","ETH-USDT-SWAP"].flatMap(inst=>[
  {channel:"trades",instId:inst},{channel:"books5",instId:inst}
 ])})));
 ws.on("message",b=>{const j=JSON.parse(b),ch=j.arg?.channel,inst=j.arg?.instId,asset=assetOf(inst),ct=OKX_CT[asset];
  if(ch==="trades")for(const x of j.data||[]){const p=num(x.px),q=num(x.sz)*ct;pushTrade({exchange:"OKX",asset,side:x.side==="buy"?"BUY":"SELL",price:p,qty:q,usd:p*q,id:String(x.tradeId),ts:num(x.ts)})}
  if(ch==="books5")for(const x of j.data||[]){const cv=a=>(a||[]).map(v=>[v[0],num(v[1])*ct]);setWalls("OKX",asset,cv(x.bids),cv(x.asks))}
 });return ws
});

// Hyperliquid trades + L2 book
reconnect("Hyperliquid",()=>{
 const ws=new WebSocket("wss://api.hyperliquid.xyz/ws");
 ws.on("open",()=>["BTC","ETH"].forEach(coin=>{ws.send(JSON.stringify({method:"subscribe",subscription:{type:"trades",coin}}));ws.send(JSON.stringify({method:"subscribe",subscription:{type:"l2Book",coin}}))}));
 ws.on("message",b=>{const j=JSON.parse(b);
  if(j.channel==="trades")for(const x of j.data||[]){const p=num(x.px),q=num(x.sz);pushTrade({exchange:"Hyperliquid",asset:x.coin,side:x.side==="B"?"BUY":"SELL",price:p,qty:q,usd:p*q,id:String(x.hash||x.tid||x.time),ts:num(x.time)})}
  if(j.channel==="l2Book"&&j.data){const cv=a=>(a||[]).map(v=>[v.px,v.sz]);setWalls("Hyperliquid",j.data.coin,cv(j.data.levels?.[0]),cv(j.data.levels?.[1]))}
 });return ws
});

app.get("/health",(_,res)=>res.json({ok:true,uptime:process.uptime(),sources:health,buffer:trades.length}));
app.get("/api/snapshot",(_,res)=>{const cut=now()-86400000;res.json({trades:trades.filter(x=>x.ts>=cut).slice(-10000),liquidations:liquidations.filter(x=>x.ts>=cut).slice(-5000),walls:[...walls.values()].flat(),health,serverTime:now()})});
app.use(express.static("public"));
const wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection",ws=>{clients.add(ws);ws.send(JSON.stringify({type:"health",data:health}));ws.on("close",()=>clients.delete(ws))});
setInterval(()=>{const cut=now()-86400000;while(trades.length&&trades[0].ts<cut)trades.shift();while(liquidations.length&&liquidations[0].ts<cut)liquidations.shift()},60000);
server.listen(PORT,"0.0.0.0",()=>console.log(`WhaleScope FULL listening on ${PORT}`));
