import { createServer } from 'node:http';
import { timingSafeEqual, createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { Ledger, Invalid, roles, sign, MAX_PACKET } from './core.mjs';
const hash=x=>createHash('sha256').update(x).digest();
export function createReceiptServer({ledger,keys}) {
  if(!roles.every(r=>typeof keys[r]==='string'&&keys[r].length>=32)||new Set(roles.map(r=>keys[r])).size!==roles.length)throw new Error('three distinct identity keys required');
  const reply=(res,status,data)=>{res.writeHead(status,{'content-type':'application/json','cache-control':'no-store','x-content-type-options':'nosniff'});res.end(JSON.stringify(data));};
  return createServer({requestTimeout:10000,headersTimeout:5000,maxHeaderSize:8192},async(req,res)=>{
    try {
      // Dedicated machine-only endpoint. No session/cookie/browser trust; no platform turn API.
      if(req.headers.origin||req.headers['sec-fetch-site']||req.headers['sec-fetch-dest'])return reply(res,403,{error:'machine clients only'});
      const actor=req.headers['x-receipt-actor'];const auth=req.headers.authorization;
      if(!roles.includes(actor)||typeof auth!=='string'||!auth.startsWith('Bearer ')||!timingSafeEqual(hash(auth.slice(7)),hash(keys[actor])))return reply(res,401,{error:'unauthorized'});
      const path=req.url;
      if(req.method==='POST'&&['/v1/events','/v1/ack'].includes(path)){
        if(req.headers['content-type']!=='application/json')return reply(res,415,{error:'JSON required'});
        let length=0;const chunks=[];
        for await(const chunk of req){length+=chunk.length;if(length>MAX_PACKET){reply(res,413,{error:'packet too large'});return;}chunks.push(chunk);}
        let packet;try{packet=JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{return reply(res,400,{error:'invalid JSON'});}
        if(packet?.actor!==actor)return reply(res,403,{error:'identity mismatch'});
        return reply(res,200,path==='/v1/events'?ledger.submit(packet,keys):ledger.ack(packet,keys));
      }
      if(req.method==='POST'&&path==='/v1/poll'){ledger.claim(actor);return reply(res,200,{deliveries:ledger.active(actor).filter(d=>d.ackBy>ledger.now()).map(d=>sign(actor,{schemaVersion:1,type:'delivery',...d},keys[actor]))});}
      if(req.method==='GET'&&path==='/v1/status')return reply(res,200,{deliveries:ledger.status(actor)});
      if(req.method==='GET'&&path==='/v1/audit')return reply(res,200,{audit:ledger.history(actor)});
      return reply(res,404,{error:'not found'});
    }catch(e){if(!res.headersSent)reply(res,e instanceof Invalid?400:503,{error:e instanceof Invalid?e.message:'receipt service unavailable'});}
  });
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  const [db,keyFile,portText]=process.argv.slice(2);const port=Number(portText);if(!db||!keyFile||!Number.isInteger(port)||port<1024||port>65535)throw new Error('usage: native.mjs PRIVATE_DB PRIVATE_KEY_MAP.json LOOPBACK_PORT');
  const keys=JSON.parse(readFileSync(keyFile,'utf8'));const ledger=new Ledger(db);const server=createReceiptServer({ledger,keys});
  // Independent opt-in listener; never bind publicly. Reverse proxy/TLS is a separate deployment decision.
  const timer=setInterval(()=>{try{for(const role of roles)ledger.claim(role);}catch{console.error('receipt sweep failed');}},30000);timer.unref();
  server.listen(port,'127.0.0.1',()=>console.log(`receipt service on 127.0.0.1:${port}`));
  const shutdown=()=>{clearInterval(timer);server.close(()=>ledger.close());};process.once('SIGTERM',shutdown);process.once('SIGINT',shutdown);
}
