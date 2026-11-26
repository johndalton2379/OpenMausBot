#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { Ledger, verify, roles } from './core.mjs';
import { publish,tick } from './spool.mjs';
import { receive } from './receiver.mjs';
import { drainFiles, drainNative } from './clients.mjs';
const [command,...args]=process.argv.slice(2);
const json=file=>JSON.parse(readFileSync(file,'utf8'));
try {
  if(command==='publish'&&args.length===4){const [root,actor,bodyFile,keyFile]=args;console.log(JSON.stringify({path:publish(root,actor,json(bodyFile),readFileSync(keyFile,'utf8').trim())}));}
  else if(command==='verify-delivery'&&args.length===3){const [actor,file,keyFile]=args;const {body,actor:sender}=verify(json(file),{[actor]:readFileSync(keyFile,'utf8').trim()});if(sender!==actor||body.receiver!==actor||body.type!=='delivery'||body.schemaVersion!==1||body.ackBy<=Date.now()||body.event.expiresAt<=Date.now())throw new Error('not an active delivery');console.log(JSON.stringify(body));}
  else if(command==='receive'&&args.length===5){const [root,actor,file,keyFile,db]=args;const key=readFileSync(keyFile,'utf8').trim();const result=receive(json(file),actor,key,db);console.log(JSON.stringify({replay:result.replay,ackPath:publish(root,actor,result.ack,key)}));}
  else if(command==='drain'&&args.length===4){const [root,actor,keyFile,db]=args;console.log(JSON.stringify(drainFiles(root,actor,readFileSync(keyFile,'utf8').trim(),db)));}
  else if(command==='native-drain'&&args.length===4){const [url,actor,keyFile,db]=args;console.log(JSON.stringify(await drainNative(url,actor,readFileSync(keyFile,'utf8').trim(),db)));}
  else if(command==='tick'&&args.length===3){const [root,db,keysFile]=args;const keys=json(keysFile);if(!roles.every(r=>typeof keys[r]==='string'&&keys[r].length>=32)||new Set(roles.map(r=>keys[r])).size!==roles.length)throw new Error('three distinct identity keys required');const ledger=new Ledger(db);try{console.log(JSON.stringify(tick(root,ledger,keys)));}finally{ledger.close();}}
  else throw new Error('usage: publish ROOT ACTOR BODY.json KEYFILE | verify-delivery ACTOR DELIVERY.json KEYFILE | receive ROOT ACTOR DELIVERY.json KEYFILE LOCAL_DB | drain ROOT ACTOR KEYFILE LOCAL_DB | native-drain LOOPBACK_URL ACTOR KEYFILE LOCAL_DB | tick ROOT PRIVATE_DB PRIVATE_KEY_MAP.json');
}catch(e){console.error(e.message);process.exitCode=1;}
