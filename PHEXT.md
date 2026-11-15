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

---

# Round 2 — Re-implementation: eliminate the complexity, measure what's left

Round 1 mapped phext seams onto HoloStore as it stands. Round 2 tests the
stronger hypothesis: **phext eliminates complexity** — so the storage core
was re-implemented phext-natively, as simply as the format allows, and the
two implementations were compared on performance, development cost,
stability, and legibility. Upstream mergeability was not a goal; finding
what phext is missing was. All numbers below were measured 2026-11-15 on
aletheia-core (16 cores) with release builds and one Python RESP client
(`crates/holo_phext/bench.py`) driving both servers identically.

## What was built: `crates/holo_phext`

One Rust binary, one dependency (libphext), one file. The store is a
single phext, `store.phext`:

- **1.1.1/1.1.1/1.1.1 — manifest scroll**: `format=`, `node=`,
  `snapshot_seq=`, `next_seq=` as text lines. This replaces the JSON
  cluster-state checkpoint, the recovery manifest, and the epoch sidecar
  file — the whole meta plane's local footprint — with four lines.
- **Volume 2 — snapshot**: live keys sorted, 1,000 per Chapter, identity
  in content (`key=` / `value=` lines), coordinates as pagination.
- **Volume 3 — WAL**: one scroll per committed op (`seq=` / `op=` /
  `key=` / `value=` lines), addressed by slot relative to the snapshot
  baseline. Last volume in the file, so a write is a pure append: the
  delimiter run to the next coordinate, then the entry text, then one
  fdatasync per group-committed batch. An op is acked only after its
  batch syncs — the original's durability contract.

Recovery replays snapshot + WAL through libphext; the first WAL scroll
that fails validation (sequence gap, missing fields, no trailing
newline) ends the log and the file is truncated at that scroll's
delimiter run. Checkpoint folds the WAL into the snapshot with one
`libphext::implode` rewrite (tmp file, rename, directory sync). Deletes
are WAL ops; the snapshot simply omits the key. Parsing and whole-file
writing go through libphext throughout — never a hand-rolled parser;
the append path emits delimiter bytes directly (serialization, not
parsing) and a unit test proves libphext reads 10,050 appended entries
back exactly, across a chapter boundary. An independent check: Will's
Python phext parser reads the checkpointed store file perfectly
(1,001 scrolls: manifest + 1,000 entries).

## The complexity-elimination map

What the format absorbed, item by item:

| Original machinery | Lines | Phext form |
|---|---|---|
| WAL record framing: length prefixes, CRC32, segment files, two backends (raft-engine + FileWal), synced-flag sidecar | most of wal.rs (2,206) | a delimiter run + text lines; the format is the framing |
| Version-key encoding: magic prefixes, generation markers, separate latest index in Fjall | much of kv.rs (4,203) | coordinates + one replay rule |
| Cluster-state JSON blob: serde model, normalization, whole-blob rewrite, fast-flag dual format | much of cluster.rs (4,220) | a 4-line manifest scroll; per-field scrolls need no blob |
| Recovery checkpoints manifest (JSON envelope + CRC) + `meta/txn_epoch` file | in cluster.rs / cluster state code | manifest scroll fields |
| Persisted-state serde models throughout | across the crate | `parse_fields`: split lines on the first `=` |

What phext did **not** remove, because it never claimed to: consensus
(Accord is agreement, not representation — out of scope for the format
and for this re-implementation, which is single-node), the fsync
discipline itself, group commit, the RESP wire surface, and systems
tuning. The hypothesis survives precisely scoped: **representation
complexity is what collapsed.**

## The four-way comparison

### Performance (same machine, same client, release builds)

| Workload | holo-phext (1 node) | HoloStore (3-node cluster) |
|---|---|---|
| SET, pipeline 50, 10 threads | 152,471 ops/s | 203,792 ops/s |
| GET, pipeline 50, 10 threads | 341,613 ops/s | 76,168 ops/s |
| SET, pipeline 1, 10 threads | 9,837 ops/s | 5,684 ops/s |
| GET, pipeline 1, 50 threads | 51,924 ops/s | 6,653 ops/s |
| Load 30,000 SETs | 0.20 s | 0.17 s |

Read the table with its asymmetry stated plainly: the cluster pays
Accord consensus and 3× replication on every write and read-index work
on reads; holo-phext is one node and pays neither. The comparison
isolates the layer phext claims to simplify — persistence and serving —
and at that layer the text implementation is in the same class, ahead on
three of four workloads through this client. (HoloStore's README reports
569k GET/s with the C redis-benchmark at pipeline 100; the Python client
used here caps both servers well below their ceilings, which is why it
was used for both.) Two of holo-phext's three real bugs were pure
measurement finds: per-connection reply serialization silently capped
pipelined writes at ~11.9k ops/s (fixed by decoupling parse from ack),
and Nagle/delayed-ACK stalls capped pipelined reads the same way (fixed
with TCP_NODELAY — GETs went 12k → 499k ops/s before the write-path fix
settled them at 342k under mixed load).

### Development cost

| | holo-phext | HoloStore storage core |
|---|---|---|
| Code | **717 lines**, one file, tests included | **12,331 lines** across wal.rs + kv.rs + cluster.rs + redis_server.rs + packed_batch.rs (workspace total: 65,062 in crates/) |
| Dependencies | 1 direct (libphext); 3 crates in the tree | 33 direct in holo_store's Cargo.toml; ~900 crates in its tree |
| Build | 0.6 s incremental; ~24 s cold including libphext | 1 m 49 s for `-p holo_store` release on the same 16 cores |
| Time to working, tested, benchmarked | one session | — |

### Stability

- **Mid-flight kill, twice.** SIGKILL during a 2M-key load: 272,500 ops
  acked at the kill; after restart, **272,500/272,500 present, 0 lost**;
  DBSIZE was 273,000 — the extra 500 are the in-flight batch that had
  synced but whose acks never reached the client. Durability is a
  superset of acknowledgment, which is the correct direction. An earlier
  run with the first server version: 17,450 acked, 0 lost.
- **Torn tail.** A partial scroll plus garbage bytes appended to the
  file: startup truncates at the last valid entry; the store keeps
  serving and appending (unit-tested).
- **Checkpoint.** 350,000 keys folded (16.0 MB WAL file → 12.1 MB
  snapshot), restart-verified. One real bug found and fixed by testing:
  directory sync after rename failed for bare relative paths.
- **Conformance.** libphext re-reads every appended byte (10,050-entry
  test); the independent Python parser reads the store file exactly.
- Scope honesty: HoloStore's Antithesis + Porcupine linearizability
  harness is stronger evidence on the *consensus* axis. holo-phext has
  no consensus to check; its tests cover crash, durability, and format
  conformance — the axes its design actually has.

### Legibility

- HoloStore state on disk: raft-engine `.raftlog`/`.rewrite` binaries,
  Fjall SST files, `cluster_state.json`, `recovery_checkpoints.json`,
  `txn_epoch`, LOCK files — five-plus representations, inspectable only
  through holoctl or a running node.
- holo-phext state on disk: one text file. Its first bytes are
  `format=holo-phext/1`, `snapshot_seq=1000`, then `key=key-0` /
  `value=value-0` entries separated by visible delimiter bytes. An
  operator reads the whole store with `less`, searches it with `grep`,
  diffs two checkpoints with `diff`, and opens it in any phext tool.
  The 130,000-key store, checkpointed, is 4.2 MB of that text.

## What phext is missing — found by building, not by reading

1. **No append in libphext.** `insert`/`replace` take and return a whole
   `String`; there is no way to say "this scroll sorts last, just extend
   the buffer." The format supports pure appends — a delimiter run plus
   content at EOF — but the library doesn't expose one, so the append
   path here hand-emits delimiter bytes and proves conformance after the
   fact. A streaming/append writer is the single highest-value addition
   this exercise found: every log-shaped phext workload needs it.
2. **No per-scroll integrity.** libphext can seal a whole phext
   (`seal.rs`, xz-based), but nothing checksums or length-frames an
   individual scroll, so a torn write is detected by content convention
   — here, "a committed entry ends with `\n`." A tear that lands exactly
   on a line boundary is indistinguishable from a commit. The original's
   WAL carries CRC32 per record; a phext-native store wants an optional
   per-scroll seal or checksum line convention blessed by the format.
3. **No atomic multi-scroll write.** Batch atomicity still comes from
   log-first discipline — write the intent as WAL scrolls, apply after —
   exactly as in any format. Phext doesn't claim this, but a native
   "commit" primitive (a scroll whose presence validates a preceding
   run) would make the pattern a format feature instead of a convention.
4. **No durability contract.** Nothing in the library or the format
   documentation states the persistence rules a host must follow —
   fdatasync per commit batch, tmp-write + rename + directory sync per
   rewrite. Every phext host re-derives them privately; we did, and one
   of our three bugs lived exactly there (directory sync on a bare
   relative path). A one-page durability profile for phext hosts would
   eliminate a class of silent data-loss bugs across every future store.
5. **Empty-scroll addressing friction.** Advancing across a volume or
   book boundary mints empty intermediate scrolls, and whether
   `explode` materializes them is the kind of detail you learn only by
   building on it. The WAL addressing here is relative to the snapshot
   baseline precisely so an append never needs more than one break past
   live content. The rule "a scroll exists iff it holds a byte" is clear
   in prose; its round-trip behavior through implode/explode deserves to
   be stated just as plainly.
6. **Binary values inside line-structured scrolls.** `binary.rs`
   hoisting exists and works for whole scrolls, but it doesn't compose
   inside a line grammar — a binary field in a `key=`/`value=` scroll
   rides as base64 at +33%. Fine for this core (values were text); a
   store with binary values pays it on every byte.

## Revised verdict

Round 1's seams stand, and Integration 1 (HoloStore as a phext scroll
store) is now demonstrated rather than argued: the storage core of a
consensus KV store re-implemented in 717 lines and one text format,
performance in the same class as the original at the layer phext
addresses, zero acknowledged writes lost across kill -9 testing, and a
store an operator can read with `less`. The complexity didn't move —
it was representation, and it is gone. What remains in the 717 lines is
the irreducible core: ordering, syncing, serving. The gaps above are
the format's growth list, in the order this build hit them: an append
writer, per-scroll integrity, a durability profile, a commit primitive,
and plainer words about empty scrolls.
