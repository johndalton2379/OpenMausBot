import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { Invalid, verify, validateEvent } from './core.mjs';
export function receive(packet,actor,key,dbPath,now=Date.now()){
  const {body:d,actor:sender}=verify(packet,{[actor]:key});
  if(sender!==actor||d?.receiver!==actor||d.type!=='delivery'||d.schemaVersion!==1||!Number.isSafeInteger(d.attempt)||d.attempt<1||!Number.isSafeInteger(d.ackBy)||d.ackBy<=now||d.event?.expiresAt<=now)throw new Invalid('not an active delivery');
  validateEvent(d.event,d.event.producer,now);
  const allowed=d.event.type==='deployment-readiness'?['paper-operator','mausbot']:['runtime-keeper','mausbot'];if(!allowed.includes(actor))throw new Invalid('receiver not subscribed');
  mkdirSync(dirname(dbPath),{recursive:true,mode:0o700});const db=new DatabaseSync(dbPath);
  try{db.exec('PRAGMA busy_timeout=10000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS receipts(receiver TEXT NOT NULL,id TEXT NOT NULL,body TEXT NOT NULL,receivedAt INTEGER NOT NULL,PRIMARY KEY(receiver,id));');
    const result=db.prepare('INSERT OR IGNORE INTO receipts VALUES(?,?,?,?)').run(actor,d.event.eventId,JSON.stringify(d.event),now);
    const saved=db.prepare('SELECT body FROM receipts WHERE receiver=? AND id=?').get(actor,d.event.eventId);
    // An acknowledgement is emitted only after a durable receipt, never a trading/startup action.
    if(saved.body!==JSON.stringify(d.event))throw new Invalid('receiver event ID conflict');
    return {replay:result.changes===0,ack:{schemaVersion:1,type:'ack',eventId:d.event.eventId,attempt:d.attempt}};
  }finally{db.close();}
}
