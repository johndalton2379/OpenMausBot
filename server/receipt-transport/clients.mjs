import { join } from 'node:path';
import { readdirSync, readFileSync } from 'node:fs';
import { receive } from './receiver.mjs';
import { publish } from './spool.mjs';
import { Invalid, roles, sign, verify } from './core.mjs';
export function drainFiles(root,actor,key,dbPath,now=Date.now()) {
  if(!roles.includes(actor))throw new Invalid('invalid actor');
  const dir=join(root,'outbox',actor);let names;try{names=readdirSync(dir);}catch(e){if(e.code==='ENOENT')return {received:0,replayed:0,expired:0,rejected:0};throw e;}
  const result={received:0,replayed:0,expired:0,rejected:0};
  for(const name of names.sort()){
    if(!/^[0-9a-f-]{36}\.[1-9][0-9]?\.json$/.test(name))continue;
    try{const packet=JSON.parse(readFileSync(join(dir,name),'utf8'));const {body}=verify(packet,{[actor]:key});if(body.ackBy<=now||body.event?.expiresAt<=now){result.expired++;continue;}
      const receipt=receive(packet,actor,key,dbPath,now);publish(root,actor,receipt.ack,key);result[receipt.replay?'replayed':'received']++;
    }catch(e){if(e.code==='ENOENT')continue;if(e instanceof Invalid||e instanceof SyntaxError){result.rejected++;continue;}throw e;}
  }return result;
}
export async function drainNative(base,actor,key,dbPath){
  if(!roles.includes(actor)||typeof key!=='string'||key.length<32)throw new Invalid('invalid actor/key');
  const url=new URL(base);if(url.protocol!=='http:'||url.hostname!=='127.0.0.1'||!url.port||url.pathname!=='/'||url.search||url.hash||url.username||url.password)throw new Invalid('native client requires an explicit loopback service URL');
  const request=async(path,body)=>{const r=await fetch(new URL(path,url),{method:'POST',headers:{'x-receipt-actor':actor,authorization:`Bearer ${key}`,'content-type':'application/json'},redirect:'error',signal:AbortSignal.timeout(10000),...(body?{body:JSON.stringify(body)}:{})});if(!r.ok)throw new Error(`receipt endpoint returned ${r.status}`);return r.json();};
  const {deliveries}=await request('/v1/poll');if(!Array.isArray(deliveries))throw new Invalid('invalid poll response');const result={received:0,replayed:0};
  for(const packet of deliveries){const receipt=receive(packet,actor,key,dbPath);await request('/v1/ack',sign(actor,receipt.ack,key));result[receipt.replay?'replayed':'received']++;}return result;
}
