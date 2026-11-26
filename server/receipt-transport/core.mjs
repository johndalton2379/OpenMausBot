import { DatabaseSync } from 'node:sqlite';
import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
export const roles = ['runtime-keeper','paper-operator','mausbot'];
const producers = { 'deployment-readiness':'runtime-keeper', 'paper-session-closeout':'paper-operator' };
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const maxAge = 7*86400000;
export const MAX_PACKET = 32768;
export class Invalid extends Error {}
const check = (condition, message) => { if (!condition) throw new Invalid(message); };
const exact = (x, keys) => x && typeof x==='object' && !Array.isArray(x) && Object.keys(x).sort().join(',')===keys.sort().join(',');
const integer = x => Number.isSafeInteger(x) && x>=0;
const short = x => typeof x==='string' && x.length>0 && x.length<=512 && !/[\x00-\x1f]/.test(x);
const digest = x => createHash('sha256').update(x).digest('hex');
const canonical = x => Array.isArray(x)?x.map(canonical):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,canonical(x[k])])):x;
const mac = (key, actor, payload) => createHmac('sha256',key).update(`omb-receipt-v1:${actor}:${payload}`).digest('hex');
export function sign(actor, body, key) {
  check(roles.includes(actor) && typeof key==='string' && key.length>=32,'invalid signing identity');
  const payload=Buffer.from(JSON.stringify(body)).toString('base64url');
  return {actor,payload,signature:mac(key,actor,payload)};
}
export function verify(packet, keys) {
  check(exact(packet,['actor','payload','signature']) && roles.includes(packet.actor) && typeof packet.payload==='string' && /^[A-Za-z0-9_-]+$/.test(packet.payload) && packet.payload.length<=MAX_PACKET && typeof packet.signature==='string' && /^[0-9a-f]{64}$/.test(packet.signature),'invalid packet');
  const key=keys[packet.actor];
  check(typeof key==='string' && key.length>=32,'identity unavailable');
  check(timingSafeEqual(Buffer.from(packet.signature,'hex'),Buffer.from(mac(key,packet.actor,packet.payload),'hex')),'authentication failed');
  let body; try { body=JSON.parse(Buffer.from(packet.payload,'base64url').toString('utf8')); } catch { throw new Invalid('invalid payload'); }
  return {actor:packet.actor,body,hash:digest(packet.payload)};
}
export function validateEvent(e, actor, now) {
  check(exact(e,['schemaVersion','eventId','type','producer','createdAt','expiresAt','synthetic','data']) && e.schemaVersion===1 && typeof e.eventId==='string' && uuid.test(e.eventId) && Object.hasOwn(producers,e.type) && producers[e.type]===actor && e.producer===actor && integer(e.createdAt) && integer(e.expiresAt) && e.createdAt<=now+60000 && e.expiresAt>e.createdAt && e.expiresAt-e.createdAt<=maxAge && typeof e.synthetic==='boolean','invalid event schema or producer');
  const d=e.data;
  if(e.type==='deployment-readiness') check(exact(d,['status','startupAuthorized','revision','evidence']) && ['ready','blocked'].includes(d.status) && d.startupAuthorized===false && short(d.revision) && short(d.evidence),'invalid readiness data');
  else check(exact(d,['status','simulatedBankrollUsd','skylitCreditCeiling','skylitCreditsUsed','evidence']) && ['closed','incomplete'].includes(d.status) && d.simulatedBankrollUsd===10000 && d.skylitCreditCeiling===4000 && integer(d.skylitCreditsUsed) && short(d.evidence),'invalid closeout data');
}
export class Ledger {
  constructor(file, {maxAttempts=4,baseMs=30000,now=Date.now}={}) {
    check(integer(maxAttempts)&&maxAttempts>=1&&maxAttempts<=10&&integer(baseMs)&&baseMs>=1&&baseMs<=3600000,'invalid retry policy');
    this.now=now; this.maxAttempts=maxAttempts;this.baseMs=baseMs;
    mkdirSync(dirname(file),{recursive:true,mode:0o700});
    this.db=new DatabaseSync(file); this.db.exec(`PRAGMA busy_timeout=10000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, hash TEXT NOT NULL, body TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS delivery(id TEXT NOT NULL REFERENCES events(id), receiver TEXT NOT NULL, state TEXT NOT NULL, attempt INTEGER NOT NULL, due INTEGER NOT NULL, reason TEXT, PRIMARY KEY(id,receiver));
      CREATE TABLE IF NOT EXISTS audit(seq INTEGER PRIMARY KEY, at INTEGER NOT NULL, id TEXT, receiver TEXT, action TEXT NOT NULL, detail TEXT);
      CREATE INDEX IF NOT EXISTS delivery_due ON delivery(receiver,state,due);`);
  }
  tx(fn) { this.db.exec('BEGIN IMMEDIATE'); try {const r=fn();this.db.exec('COMMIT');return r;} catch(e){this.db.exec('ROLLBACK');throw e;} }
  audit(id, receiver, action, detail=null) {this.db.prepare('INSERT INTO audit(at,id,receiver,action,detail) VALUES(?,?,?,?,?)').run(this.now(),id,receiver,action,detail);}
  submit(packet,keys) {
    const {actor,body:e}=verify(packet,keys); validateEvent(e,actor,this.now());const hash=digest(JSON.stringify(canonical(e)));
    return this.tx(()=>{const old=this.db.prepare('SELECT hash FROM events WHERE id=?').get(e.eventId);
      if(old){check(old.hash===hash,'event ID conflict');return {eventId:e.eventId,replay:true};}
      check(e.expiresAt>this.now(),'stale event');
      this.db.prepare('INSERT INTO events VALUES(?,?,?)').run(e.eventId,hash,JSON.stringify(e));
      const receivers=e.type==='deployment-readiness'?['paper-operator','mausbot']:['runtime-keeper','mausbot'];
      for(const r of receivers)this.db.prepare('INSERT INTO delivery VALUES(?,?,\'pending\',0,?,NULL)').run(e.eventId,r,this.now());
      this.audit(e.eventId,null,'accepted',actor);return {eventId:e.eventId,replay:false};});
  }
  sweep() {
    const now=this.now(); const rows=this.db.prepare("SELECT d.*,e.body FROM delivery d JOIN events e ON e.id=d.id WHERE d.state IN ('pending','leased')").all();
    for(const row of rows){const event=JSON.parse(row.body);const reason=event.expiresAt<=now?'expired':row.state==='leased'&&row.due<=now&&row.attempt>=this.maxAttempts?'retry-exhausted':null;
      if(reason){this.db.prepare("UPDATE delivery SET state='dead',reason=? WHERE id=? AND receiver=?").run(reason,row.id,row.receiver);this.audit(row.id,row.receiver,'dead',reason);}}
  }
  claim(receiver,limit=100) {
    check(roles.includes(receiver)&&integer(limit)&&limit>=1&&limit<=100,'invalid receiver');
    return this.tx(()=>{this.sweep();const now=this.now();const rows=this.db.prepare("SELECT d.*,e.body FROM delivery d JOIN events e ON e.id=d.id WHERE receiver=? AND state IN ('pending','leased') AND due<=? ORDER BY due,id LIMIT ?").all(receiver,now,limit);
      return rows.map(row=>{const attempt=row.attempt+1;const due=now+this.baseMs*2**(attempt-1);this.db.prepare("UPDATE delivery SET state='leased',attempt=?,due=? WHERE id=? AND receiver=?").run(attempt,due,row.id,receiver);this.audit(row.id,receiver,'offered',String(attempt));return {event:JSON.parse(row.body),receiver,attempt,ackBy:due};});});
  }
  active(receiver){return this.db.prepare("SELECT d.*,e.body FROM delivery d JOIN events e ON e.id=d.id WHERE receiver=? AND state='leased'").all(receiver).map(row=>({event:JSON.parse(row.body),receiver,attempt:row.attempt,ackBy:row.due}));}
  ack(packet,keys) {
    const {actor,body:a}=verify(packet,keys);check(exact(a,['schemaVersion','eventId','attempt','type'])&&a.schemaVersion===1&&a.type==='ack'&&typeof a.eventId==='string'&&uuid.test(a.eventId)&&integer(a.attempt)&&a.attempt>0,'invalid acknowledgement');
    return this.tx(()=>{this.sweep();const row=this.db.prepare('SELECT * FROM delivery WHERE id=? AND receiver=?').get(a.eventId,actor);check(row&&row.attempt===a.attempt,'unknown or superseded delivery');
      if(row.state==='acked')return {replay:true};check(row.state==='leased'&&row.due>this.now(),'delivery not active');
      this.db.prepare("UPDATE delivery SET state='acked' WHERE id=? AND receiver=?").run(a.eventId,actor);this.audit(a.eventId,actor,'acked',String(a.attempt));return {replay:false};});
  }
  status(receiver){check(roles.includes(receiver),'invalid receiver');return this.tx(()=>{this.sweep();return this.db.prepare('SELECT id,receiver,state,attempt,due,reason FROM delivery WHERE receiver=? ORDER BY id').all(receiver);});}
  history(receiver){check(roles.includes(receiver),'invalid receiver');return this.db.prepare("SELECT seq,at,id,receiver,action,detail FROM audit WHERE receiver=? OR (receiver IS NULL AND id IN (SELECT id FROM delivery WHERE receiver=?)) ORDER BY seq DESC LIMIT 1000").all(receiver,receiver);}
  close(){this.db.close();}
}
