import { randomUUID, createHash } from 'node:crypto';
import { mkdirSync, openSync, closeSync, writeFileSync, fchmodSync, fsyncSync, renameSync, unlinkSync, readdirSync, readSync, lstatSync, constants } from 'node:fs';
import { join, dirname } from 'node:path';
import { Ledger, Invalid, sign, verify, roles, MAX_PACKET } from './core.mjs';
export function atomicJson(file,value,mode=0o600){
  const parent=dirname(file);mkdirSync(parent,{recursive:true,mode:0o700});const tmp=join(parent,`.${randomUUID()}.tmp`);let fd;
  try {fd=openSync(tmp,'wx',0o600);fchmodSync(fd,mode);writeFileSync(fd,JSON.stringify(value)+'\n');fsyncSync(fd);closeSync(fd);fd=undefined;renameSync(tmp,file);const dir=openSync(parent,'r');try{fsyncSync(dir);}finally{closeSync(dir);}}
  finally {if(fd!==undefined)closeSync(fd);try{unlinkSync(tmp);}catch(e){if(e.code!=='ENOENT')throw e;}}
}
export function publish(root,actor,body,key){ const name=`${randomUUID()}.json`;const path=join(root,'inbox',actor,name);atomicJson(path,sign(actor,body,key),0o640);return path; }
function readPacket(file){const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const buf=Buffer.alloc(MAX_PACKET+1);let n=0,r;while(n<buf.length&&(r=readSync(fd,buf,n,buf.length-n,null))>0)n+=r;if(n>MAX_PACKET)throw new Invalid('oversize packet');return JSON.parse(buf.subarray(0,n).toString('utf8'));}finally{closeSync(fd);}}
function remove(file){try{unlinkSync(file);}catch(e){if(e.code!=='ENOENT')throw e;}}
export function tick(root,ledger,keys){
  const result={accepted:0,replayed:0,acked:0,rejected:0,offered:0};
  for(const actor of roles){const inbox=join(root,'inbox',actor);mkdirSync(inbox,{recursive:true,mode:0o700});
    for(const name of readdirSync(inbox).sort()){
      if(!/^[0-9a-f-]{36}\.json$/.test(name))continue;const file=join(inbox,name);
      try{const st=lstatSync(file);if(!st.isFile()||st.size>MAX_PACKET)throw new Invalid('invalid input file');const packet=readPacket(file);if(packet.actor!==actor)throw new Invalid('wrong inbox identity');const {body}=verify(packet,keys);
        const out=body?.type==='ack'?ledger.ack(packet,keys):ledger.submit(packet,keys);
        result[out.replay?'replayed':body?.type==='ack'?'acked':'accepted']++;remove(file);
      }catch(e){if(e.code==='ENOENT')continue;if(!(e instanceof Invalid||e instanceof SyntaxError||e.code==='ELOOP'))throw e;
        // Never preserve untrusted payloads, filenames or signatures in the audit record.
        const id=createHash('sha256').update(`${actor}:${name}`).digest('hex');ledger.tx(()=>ledger.audit(null,actor,'input-rejected',id+':'+(e instanceof Invalid?e.message:'invalid JSON/file')));atomicJson(join(root,'rejected',`${id}.json`),{actor,receiptId:id,reason:e instanceof Invalid?e.message:'invalid JSON/file'});remove(file);result.rejected++;
      }
    }
  }
  for(const receiver of roles){
    const outbox=join(root,'outbox',receiver);mkdirSync(outbox,{recursive:true,mode:0o700});result.offered+=ledger.claim(receiver).length;
    // Recover after a crash between database commit and atomic file publication.
    for(const delivery of ledger.active(receiver))if(delivery.ackBy>ledger.now())atomicJson(join(outbox,`${delivery.event.eventId}.${delivery.attempt}.json`),sign(receiver,{schemaVersion:1,type:'delivery',...delivery},keys[receiver]),0o640);
  }
  return result;
}
