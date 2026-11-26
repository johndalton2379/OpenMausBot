# Zero-model receipt transport v1

Standalone Node.js 24+ files; no npm dependencies, network, bot inbox, provider, orders or automation calls in the file mode. The native mode is an explicit, independently started Node HTTP service. It is **not** the app's message Bot API or a webhook routine. No operation authorizes a session startup, changes a trading account, or grants Skylit credits.

## Wire contract

An event has exactly `schemaVersion: 1`, a lowercase UUID `eventId` generated once by the producer and retained on retry, `type`, `producer`, integer UTC epoch-millisecond `createdAt` and `expiresAt`, boolean `synthetic`, and `data`. Expiry is at most seven days after creation; future creation tolerance is one minute. Unknown properties are rejected. Text evidence and revision are nonempty strings of at most 512 characters, without control characters. Evidence should be a nonsecret artifact identifier, not embedded sensitive content.

| Event | Producer | Subscribers | Required `data` |
|---|---|---|---|
| `deployment-readiness` | `runtime-keeper` | `paper-operator`, `mausbot` | `status` (`ready` or `blocked`), `startupAuthorized: false`, `revision`, `evidence` |
| `paper-session-closeout` | `paper-operator` | `runtime-keeper`, `mausbot` | `status` (`closed` or `incomplete`), `simulatedBankrollUsd: 10000`, `skylitCreditCeiling: 4000`, `skylitCreditsUsed` (nonnegative safe integer, can report an overrun), `evidence` |

Acknowledgement: `{ "schemaVersion": 1, "type": "ack", "eventId": "<same UUID>", "attempt": 1 }`. An ACK means the receiver durably recorded the informational receipt, not that it started work or approved anything. A delivery contains `{schemaVersion:1,type:"delivery",event,receiver,attempt,ackBy}`. Every packet is `{actor,payload,signature}`: payload is the base64url encoding of UTF-8 JSON bytes; signature is lowercase hex HMAC-SHA256 over `omb-receipt-v1:<actor>:<payload>`. Keys are independently generated high-entropy strings, minimum 32 chars. Compare signatures in constant time. Maximum packet size 32768 bytes. Never put keys in event data, shell arguments, logs or repository.

## Phase 1: shared-file spool

On an isolated or approved host, establish a local filesystem with `inbox/<actor>`, `outbox/<actor>`, and `rejected`. Use service-owned directories and OS-level permissions/ACLs: each actor may publish into its own inbox and read its own outbox; broker may access all. Broker DB and full key map must be on a private filesystem outside the spool and outside actor-visible folders. Each actor receives only its own key via an approved secure channel. The scripts create missing directories private by default; cross-account permissions must be provisioned explicitly. On a POSIX host use per-actor groups containing only that actor and broker, pre-create setgid inbox directories writable/readable by both, and setgid outbox directories writable by broker and readable by that actor. The publisher uses mode 0640 for event/delivery files to permit that group read. Do not use the same unisolated OS identity as a security boundary. No live credentials, ACLs, jobs, or shared production spool have been provisioned by this patch.

```sh
node --test receipt.test.mjs
node cli.mjs publish "$SPOOL" runtime-keeper event.json "$ACTOR_KEY_FILE"
node cli.mjs tick "$SPOOL" "$BROKER_DB" "$BROKER_KEY_MAP"
node cli.mjs drain "$SPOOL" mausbot "$MAUSBOT_KEY_FILE" "$MAUSBOT_RECEIPT_DB"
node cli.mjs tick "$SPOOL" "$BROKER_DB" "$BROKER_KEY_MAP"
```

The key map is a JSON object keyed by the three role names; no values are supplied here. `publish` accepts an event or ack JSON file, signs it, fsyncs a uniquely named temporary file, renames it atomically to the final filename, then fsyncs the directory. `tick` validates and consumes completed `.json` files; ignores incomplete `.tmp` files; handles absent input as an empty pass. Rejected committed inputs are removed with sanitized audit/rejection records. Do not write directly to final names. The SQLite ledger is authoritative (WAL, FULL sync, transactions). Same ID and same normalized data is a replay; same ID with changed content is rejected. Each subscribed receiver gets its own persistent attempt and ACK state. Delivery file names include event ID and attempt; restart republishes active files if publication was interrupted. Files may remain after acknowledgement; a consumer must honor `ackBy`, use `receive` with its own durable local DB, and never treat files as exactly-once callbacks. `drain` checks the actor outbox with no model (missing input is safe), and `receive` handles one delivery file. The receive command records once then publishes the ACK; if interrupted after record but before ACK, rerun safely. No consumer calls any bot or model. `verify-delivery` is a read-only inspection command, not a delivery acknowledgement.

Default delay is 30s, 60s, 120s, 240s across four attempts, checked by a deterministic broker tick (suggested cadence 5 seconds, not enabled). Expiry or final failed lease creates a terminal `dead` state with reason and append-only audit entry. A lost receiver can reconnect before expiry/final exhaustion, acknowledge the current attempt, and resume new events; old/superseded ACKs reject. A terminal dead delivery does not silently revive; a producer must investigate and issue a new independently meaningful event ID if appropriate. No automatic dead-letter re-drive. For operations, inspect the broker DB read-only (`delivery`, `events`, `audit`), not the outbox file count. Retain/backup ledger as the deduplication authority; do not purge events or manually alter state. Archive old output and rejection files under a service-owned retention policy only after confirming ledger status. No automatic retention policy in this patch.

## Phase 2: native transport / migration

`node native.mjs "$BROKER_DB" "$BROKER_KEY_MAP" 8765` starts the **opt-in loopback-only** dedicated receipt service in an isolated environment. It won't start the app, an AI, or a receiver turn. Do not proxy publicly without independent security review and TLS. No browser sessions, cookies, or app trust are accepted. The service checks `X-Receipt-Actor` and `Authorization: Bearer <actor key>`; submit/ack also require the signed packet and matching actor. All POSTs with packets use `Content-Type: application/json`. Only the configured identity can poll or view its subscribed receipts. HTTP routes:

- `POST /v1/events`: authenticated signed event; `{eventId,replay}`.
- `POST /v1/poll`: returns `{deliveries:[signed packet,...]}` for authenticated actor; repeated poll during active lease returns current delivery.
- `POST /v1/ack`: authenticated signed ACK; `{replay}`.
- `GET /v1/status`: actor-scoped delivery states (including dead reason).
- `GET /v1/audit`: latest 1,000 actor-scoped audit entries; full history remains in SQLite.

The independent service's timer claims due deliveries for all subscriptions every 30 seconds, even with no receiver online. A caller must persist received events idempotently before acknowledging; the shared `receive()` function can record native packets and return a signed-ACK body for transport. For an unattended machine receiver, `node cli.mjs native-drain http://127.0.0.1:8765 mausbot "$MAUSBOT_KEY_FILE" "$MAUSBOT_RECEIPT_DB"` polls, persists and acknowledges; no redirects or non-loopback destinations receive a key. No automatic startup or trade actions are wired. The HTTP service accepts only the specified local route set. No webhook/turn route is touched.

Migration: stop only the **file broker** in a separately approved maintenance window (not the OpenMausBot app); drain committed inbox items and checkpoint/backup the broker SQLite DB via its SQLite backup mechanism, then point native service at the same database and same privately held role key map. The same schema preserves dedupe, active attempts, terminal states and audit. Pending spool files can be drained with `tick` against that DB; do not fork competing databases. Any concurrent bridge must use the same local filesystem ledger and matching retry policy. Clients migrating from files should retain their receiver-local DBs. Tests prove replay of file-submitted, already-leased receipts through native HTTP without duplicating acceptance.

## Test isolation and release

Tests create disposable temp roots, synthetic keys, synthetic no-write events and a random loopback port. No real keys, provider calls, trading or app service. `node --test receipt.test.mjs` covers success, schema/scope and separate budgets, replay, malformed/stale/partial/missing input, concurrent processes, receiver outage, exhaustion/recovery, durable ACK, and migration/HTTP scoping. The native implementation is a release candidate only, not production-certified. An independent security and operational review must validate identity custody, host ACLs, backups, rate/load/storage behavior, and deployment integration before enabling either transport for actual events.
