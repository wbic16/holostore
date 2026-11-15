# PHEXT.md — Phext integration analysis for HoloStore

Provenance: analysis of `kellabyte/holostore` at commit
`087c429440a529137e58d2d49d89d63dfb9d9772` (2026-05-09, PR #49), read from the
repository's own code, docs, and formats. Work lives in the fork
`wbic16/holostore`; upstream is untouched. Date: 2026-11-15.

Phext, in one line: text at 9-dimensional addresses
(Library.Shelf.Series / Collection.Volume.Book / Chapter.Section.Scroll),
each scroll a plain 2D page of text, coordinates 1-based and internal to one
phext. A scroll exists exactly when it holds at least one byte.

## What HoloStore is, from its files

A Rust workspace of four crates:

- `holo_accord` — a leaderless Accord consensus engine (pre-accept → accept →
  commit, per-key dependency tracking). `TxnId{node_id, counter}` packs
  group/epoch/sequence into the counter
  (group << 48 | epoch << 36 | dense seq). Commits persist through a
  `CommitLog` trait as `CommitLogEntry{txn_id, seq, deps[], command bytes}`.
- `holo_store` — the node: a versioned KV state machine over Fjall
  (`Version = (range_generation, seq, txn_id)`; separate versions and latest
  partitions), a WAL with two backends (default `raft-engine`, legacy
  `FileWal`; one binary entry encoding shared by both, length-prefixed
  records with CRC32 in the file backend), a Redis-compatible front end, a
  gRPC surface (`proto/holo.proto`, packed v2 batch envelopes), and a
  Cockroach-style meta plane: `ClusterState` (members, shard descriptors,
  replica roles, in-flight moves/merges, controller leases) applied by meta
  Accord groups and checkpointed to `meta/cluster_state.json`. Ranges split
  and merge dynamically; each descriptor carries a durable `generation`.
- `holo_fusion` — a SQL layer (PostgreSQL wire protocol, DataFusion,
  optional Ballista). SQL objects map to binary, order-preserving keys
  (prefix byte + big-endian ids); table and secondary-index metadata values
  are **serde_json records** stored in the KV; row values are a canonical
  binary tuple encoding.
- `holo_workload` + `tools/` — workload generator, Antithesis harness, and
  Porcupine linearizability checking over JSON operation histories.

## Classification

Each persistent or structured part, judged on the five phext tests (text,
stable identity, local granularity, single-record access, provenance value):

| Part | Class | Why |
|---|---|---|
| User KV data plane (Fjall versions/latest, binary version keys) | **D** | Values are arbitrary bytes on a hot LSM path; no text meaning to address. Phext gains nothing and would cost the design its throughput. |
| WAL entry format (both backends) | **D** canonical, **B** as a view | Append-hot, batched, CRC-framed binary. Never replace it. But its entries are fully structured (txn, seq, deps, decodable KV commands), so a decoded read-only journal view is real (below). |
| `ClusterState` JSON checkpoint (`meta/cluster_state.json`) | **B** (A candidate for the checkpoint format itself) | The meta log is the replicated truth; the JSON file is explicitly a local checkpoint, rewritten whole on every persist. It is text, field-structured, low-frequency, and operators already inspect it (`holoctl state`). Per-field scrolls make it diffable and field-addressable at no hot-path cost. |
| Meta-plane commands (`ClusterCommand`, serde_json) | **B** | Same reasoning: JSON today, low-frequency, provenance-rich. A phext rendering of the meta log is an audit view, not a new truth. |
| HoloFusion catalog metadata (table/index records, serde_json values under binary id keys) | **C** bridge, promotable to **B** | Stable u64 identities everywhere (db/schema/table/column/index), per-record updates (the v1→v2 metadata migration rewrites records one at a time with expected-version checks), and operators genuinely want one record at a time. Rows stay binary; the catalog is the phext-shaped part. |
| HoloFusion row values (binary tuple encoding) | **D** | Order-preserving binary whose meaning is its exact bit pattern; text encoding buys no addressability anyone will use. |
| gRPC wire format (protobuf, packed batches) | **D** | Hot RPC path; protobuf is the right tool. |
| Recovery checkpoint manifest (`meta/recovery_checkpoints.json`, JSON envelope + CRC) | **B** | Same shape as cluster state: JSON, low-frequency, operator-facing. Render with the same exporter. |
| Porcupine/Antithesis histories (JSON) | **B** | Evidence artifacts. An ops-as-scrolls rendering (Book = client, Scroll = op) makes a linearizability history greppable and citable per operation. Optional, low priority. |
| Docs corpus (24 markdown files, ~178 KB) | **A** | Text with stable identity and provenance value; a bundled docs phext is a pure win and the cheapest first ship. |
| Config (env vars, docker-compose YAML) | **D** | Leave as-is; document inside the docs phext instead. |

Verdict in one sentence: **do not phextize HoloStore's data plane; phextize
its control plane's checkpoints, its SQL catalog, its evidence — and,
inverted, let HoloStore host phext itself.**

## Integration 1 — HoloStore as a phext scroll store (the headline)

Phext scrolls pass all five tests against HoloStore's KV: text content,
stable identity (the coordinate), per-scroll updates, single-scroll reads,
and provenance value. HoloStore already provides everything a phext host
needs and a local file host (SQ) does not: strong consistency via Accord,
MVCC versions per key, a latest index, and lexicographic ranges that split
and merge.

Design:

- **Key**: `0x50` ('P') followed by the nine coordinate components, each a
  big-endian u32 (36 bytes). Fixed-width big-endian keeps lexicographic key
  order identical to coordinate order, so a coordinate subtree (a Library,
  a Collection, a Book) is exactly a Holostore range that can split and merge as its own shard.
- **Value**: the scroll's text bytes, UTF-8.
- **Delete**: write a zero-byte value. Phext already defines a scroll as
  existing only when it holds at least a byte, so Holostore's missing
  `delete` on the `KvEngine` trait is not a gap here — empty *is* absent.
- **History**: Holostore's `Version` and its get-at-version reads give
  per-scroll MVCC history for free; the phext host exposes it as revision
  reads at the same coordinate.
- **Chunking**: oversized scrolls chunk across Scroll increments under one
  Section (the segmented-sequence archetype), with the chunk count in the
  first chunk's header line.

Seam: a client-side adapter over the existing RPCs (`KvGet`,
`RangeWriteLatest`, `RangeSnapshotLatest` for subtree reads) — no server
changes. A native `holo_phext` crate can come later; the wire surface is
already sufficient.

Honest costs: every scroll write pays Accord consensus — this is a
replicated, strongly consistent phext tier, not a replacement for a local
file host on the hot path. Coordinate components are capped at u32 by the
key encoding; that cap is a stated format limit, recorded in the manifest.

## Integration 2 — cluster-state phext (Class B)

One phext per node checkpoint, exported from `ClusterState` (or dual-written
beside the JSON by `ClusterStateStore::persist`):

- Manifest: `1.1.1/1.1.1/1.1.1` — format, version, source epoch, source
  commit.
- Header fields: epoch `1.1.1/1.1.1/1.2.1`, frozen `…/1.2.2`,
  replication_factor `…/1.2.3`, range_split_cooldown_until_ms `…/1.2.4`.
- Members: Volume 2, Book = node_id; Chapter 1 field scrolls:
  1 = grpc_addr, 2 = redis_addr, 3 = status.
- Shards: Volume 3, Book = shard_id; Chapter 1 descriptor fields
  (1 = start_key hex, 2 = end_key hex, 3 = generation, 4 = leaseholder,
  5 = storage index), Chapter 2 replicas (Scroll = ordinal → node_id),
  Chapter 3 roles (Scroll = node_id → role), Chapter 4 replica-move state,
  Chapter 5 merge state, Chapter 6 fence, Chapter 7 retired-range record.
- Meta ranges: Volume 4, Book = meta_range_id, same lanes.
- Controller leases: Volume 5, Book = domain ordinal (1 meta, 2 range,
  3 rebalance); fields holder / term / lease_until_ms.
- Keys that are not valid UTF-8 render as hex — the same rule the journal
  view uses; the phext itself stays valid UTF-8 throughout.

Epoch snapshots: one phext per epoch (`cluster-state-<epoch>.phext`) with
the same map, so control-plane history becomes a diffable text archive
instead of an overwritten JSON file.

## Integration 3 — HoloFusion catalog bridge (Class C → B)

One catalog phext per cluster, exported from `list_table_metadata` and the
secondary-index metadata:

- Manifest `1.1.1/1.1.1/1.1.1`; metadata schema state at
  `1.1.1/1.1.1/2.1.1` (schema_version), `…/2.1.2` (checkpoint_table_id),
  `…/2.1.3` (updated_at_unix_ms).
- Volume = db_id, Book = schema_id, Chapter = table_id.
- Section 1, table fields: 1 = name, 2 = model, 3 = primary_key_column,
  4 = page_size, 5 = metadata_schema_version, 6 = created_at.
- Section 2, columns: Scroll = column ordinal; the scroll is a small 2D
  record (id / name / logical_type / nullable / default_expr / ordinal as
  labelled lines — lines inside a scroll are free; do not spend dimensions
  on them).
- Section 3, secondary indexes: Scroll = index_id; labelled lines
  (name / column_ids / unique / created_at).
- Section 4, name map: 1 = db name, 2 = schema name, 3 = table name.

Import direction: rebuild the JSON records from the scrolls and write them
through the existing metadata APIs. The bridge is verified when export →
import → export is a fixed point. Note the implementation stores columns
inside the table record JSON (the storage-model doc's separate `0x04`
column keys were not built); the map follows the implementation.

## Integration 4 — WAL journal view (Class B)

A read-only exporter that decodes a `FileWal` `wal.log` (the `raft-engine`
backend shares the entry encoding) into one journal phext per node:

- Volume = group_id + 1, Book = origin node_id, Chapter = txn epoch + 1,
  Section 1, Scroll = dense per-epoch sequence (`txn_seq`).
- Each entry scroll is a 2D record: `seq=`, `deps=` (comma-separated
  `node.counter`), then the decoded KV command — kind, key (UTF-8 when
  valid, else hex), value (UTF-8 when valid, else base64), and
  `gen=` (range generation, from the v2/v3 command encodings).
- The journal is trustworthy exactly as far as the native reader is: the
  first CRC mismatch ends the volume, and the manifest scroll says so.

This turns crash-recovery and linearizability forensics from a binary dig
into a text search — you can point at one committed command by address.

## Integration 5 — docs corpus (Class A)

`holostore-docs.phext`: Volume per crate (1 = holo_store, 2 = holo_fusion,
3 = holo_accord, 4 = tools), Book per document in README order, one scroll
per document, verbatim. Manifest carries the source commit and a SHA-256
per document. Build it in CI so it cannot drift.

## Build plan in this fork

Native formats, APIs, and tests stay untouched throughout; all phext work
is additive.

1. **Phase 0** — this file.
2. **Phase 1 (bridge)** — `holoctl phext export-state` and
   `holoctl phext export-catalog` (read-only), plus the docs corpus
   builder. Gate: semantic roundtrip (phext → JSON equals the source
   `ClusterState`; catalog export → import → export is a fixed point) and
   the mutation control (one member field changes → exactly one scroll
   changes).
3. **Phase 2** — `holoctl phext export-wal` journal for a stopped node's
   data dir. Gate: entry-for-entry equality with the native reader,
   including the CRC-stop behavior.
4. **Phase 3** — `crates/holo_phext`: the scroll-store adapter over the
   existing RPCs. Parsing and serialization go through libphext-rs, never
   a hand-rolled parser. Gate: phext conformance tests against files the
   adapter did not write, plus a split/merge test proving a coordinate
   subtree survives range movement.
5. **Phase 4 (only with receipts from Phase 1)** — dual-write the phext
   checkpoint beside `cluster_state.json` in `persist()`; the JSON stays
   until the phext checkpoint has carried a real restart.

Every phase verifies: strict UTF-8 over the whole phext, a delimiter-walk
cross-parse with an independent parser, and SHA-256 hashes on shipped
artifacts.

## Open questions / stated limits

- Value-size behaviour for very large scrolls is not documented in the
  repo; the chunking rule in Integration 1 is the design answer, sized
  once Phase 3 measures real limits.
- The u32 coordinate cap (Integration 1) is a format limit of the key
  encoding, not of phext; a v2 key encoding can widen it without moving
  existing addresses if it sorts identically for values < 2^32.
- Nothing here proposes changing Accord, the WAL, Fjall layout, or the
  wire protocol. If a future phase wants that, it starts as its own
  analysis.
