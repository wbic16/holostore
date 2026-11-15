//! holo-phext — HoloStore's storage core on the HoloPhext engine.
//!
//! Round 2 proved the shape by hand: one phext file holding a manifest,
//! a snapshot volume, and a WAL volume, with group commit and torn-tail
//! recovery, in 717 lines. This build keeps the RESP server, the
//! group-commit writer, and the field codec from that experiment — and
//! replaces the hand-rolled store underneath with the layer2 engine
//! (wbic16/holo-phext), which is that shape factored out and completed:
//! checked WAL entries with commit markers, snapshot-isolated reads,
//! and a checkpoint that folds the log into a memory-mapped base.
//!
//! The mapping is one scroll per hash bucket: 65,536 buckets at
//! small coordinates, each bucket scroll holding its keys as the
//! round-2 field codec's `key=` / `value=` lines — identity lives
//! in content, coordinates are pagination, exactly as round 2 had
//! it. (A first draft hashed each key to its own scroll at
//! checksum-spread coordinates; phext encodes coordinate distance
//! as unary delimiter runs, so components in the billions made
//! checkpoint's implode walk effectively forever. Coordinates are
//! positions, not hashes — the bucket layout is the fix, recorded
//! in PHEXT.md round 3.) The base document at `--path` is a plain
//! phext; the engine's WAL lives beside it as `<name>.wal.phext`.
//!
//! Durability rule (unchanged): an op is acknowledged only after its
//! transaction is written and synced by the engine's commit path.
//! The writer below still batches: appends accumulate for one
//! group-commit window, then one `commit_batch` syncs them all.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};

use libphext::phext::{self, Coordinate};
use holo_phext::core::{Core, Shared, Txn};

// ---------------------------------------------------------------------------
// Bucket addressing: a key lives in exactly one of 4,096 bucket
// scrolls, chosen by the xxh3-128 checksum of its base64 form.
// Bucket coordinates stay small (volume, chapter in 1..=256) — see
// the header note on unary coordinate distance. A bucket scroll's
// content is its entries as sorted `key=` / `value=` line pairs in
// the field codec below; a SET or DEL rewrites its bucket's scroll
// as one engine transaction.

const BUCKETS: u32 = 65_536;

fn bucket_of(key: &[u8]) -> u32 {
    let digest = phext::checksum(&b64_encode(key));
    u32::from_str_radix(&digest[..8], 16).unwrap_or(0) % BUCKETS
}

fn bucket_coord(bucket: u32) -> Coordinate {
    phext::to_coordinate(&format!(
        "1.1.1/1.{}.1/{}.1.1",
        bucket / 256 + 1,
        bucket % 256 + 1
    ))
}

/// Parse a bucket scroll's body into its live entries.
fn parse_bucket(body: &str) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    let mut lines = body.lines();
    while let Some(kline) = lines.next() {
        if let Some(k) = kline.strip_prefix("key=") {
            if let Some(vline) = lines.next() {
                if let Some(v) = vline.strip_prefix("value=") {
                    if let (Some(k), Some(v)) = (decode_field(k), decode_field(v)) {
                        map.insert(k, v);
                    }
                }
            }
        }
    }
    map
}

fn bucket_body(map: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>) -> String {
    let mut out = String::new();
    for (k, v) in map {
        out += &format!("key={}\nvalue={}\n", encode_field(k), encode_field(v));
    }
    out
}

// ---------------------------------------------------------------------------
// Field codec: a key or value rides on one content line. Raw when it is
// UTF-8 with no phext break bytes and no newline and no "b64:" prefix;
// base64 otherwise. Phext stays valid UTF-8 either way.

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let mut vals = Vec::with_capacity(s.len());
    for c in s.bytes() {
        vals.push(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => 64,
            _ => return None,
        });
    }
    let mut out = Vec::with_capacity(vals.len() * 3 / 4);
    for chunk in vals.chunks(4) {
        if chunk.len() < 4 {
            return None;
        }
        let n = ((chunk[0] as u32) << 18) | ((chunk[1] as u32) << 12) | ((chunk[2] as u32) << 6) | chunk[3] as u32;
        out.push((n >> 16) as u8);
        if chunk[2] != 64 {
            out.push((n >> 8) as u8);
        }
        if chunk[3] != 64 {
            out.push(n as u8);
        }
    }
    Some(out)
}

fn is_break_byte(b: u8) -> bool {
    matches!(b, 0x01 | 0x17 | 0x18 | 0x19 | 0x1A | 0x1C | 0x1D | 0x1E | 0x1F)
}

fn encode_field(data: &[u8]) -> String {
    let raw_ok = std::str::from_utf8(data).is_ok_and(|s| {
        !s.starts_with("b64:") && !s.bytes().any(|b| is_break_byte(b) || b == b'\n')
    });
    if raw_ok {
        String::from_utf8(data.to_vec()).unwrap()
    } else {
        format!("b64:{}", b64_encode(data))
    }
}

fn decode_field(s: &str) -> Option<Vec<u8>> {
    if let Some(rest) = s.strip_prefix("b64:") {
        b64_decode(rest)
    } else {
        Some(s.as_bytes().to_vec())
    }
}

/// Parse the `name=value` lines of one scroll. Values never contain '\n'
/// (the codec guarantees it), so lines are the whole grammar.
fn parse_fields(content: &str) -> HashMap<String, String> {
    content
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// The store: the round-2 Store surface (open / get / len / append_op /
// sync / checkpoint) over the HoloPhext engine. append_op stages a
// one-op transaction and mirrors it into a pending map, so reads in
// the same writer batch see it before the sync — the read-your-batch
// semantics the original's apply-on-append gave. sync() is the group
// commit: one commit_batch, one WAL sync, every staged op durable.

pub struct Store {
    core: Core,
    shared: Arc<Shared>,
    /// Staged per-bucket changes in the current writer batch:
    /// bucket -> key -> new value (None = deleted).
    pending: std::collections::HashMap<u32, std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    /// Every live key, rebuilt at open from the bucket scrolls —
    /// the DBSIZE surface.
    live: std::collections::HashSet<Vec<u8>>,
    /// Next sequence the engine will assign (INFO surface, as before).
    pub next_seq: u64,
    /// Sequence folded into the base document at the last checkpoint.
    pub snapshot_seq: u64,
}

impl Store {
    pub fn open(path: &Path) -> std::io::Result<Store> {
        let core = Core::open(path)?;
        let shared = core.shared.clone();
        let snapshot_seq = core.checkpoint_seq();
        let next_seq = core.seq() + 1;
        let snap = shared.snapshot();
        let mut live = std::collections::HashSet::new();
        for coord in snap.coordinates() {
            if let Some(body) = snap.get(&coord) {
                for k in parse_bucket(&body).into_keys() {
                    live.insert(k);
                }
            }
        }
        Ok(Store {
            core,
            shared,
            pending: std::collections::HashMap::new(),
            live,
            next_seq,
            snapshot_seq,
        })
    }

    /// The bucket's live entries: committed content with this
    /// batch's staged changes applied.
    fn bucket_view(&self, bucket: u32) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
        let mut map = self
            .shared
            .snapshot()
            .get(&bucket_coord(bucket))
            .map(|body| parse_bucket(&body))
            .unwrap_or_default();
        if let Some(staged) = self.pending.get(&bucket) {
            for (k, v) in staged {
                match v {
                    Some(val) => {
                        map.insert(k.clone(), val.clone());
                    }
                    None => {
                        map.remove(k);
                    }
                }
            }
        }
        map
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.bucket_view(bucket_of(key)).get(key).cloned()
    }

    pub fn len(&self) -> usize {
        let mut n = self.live.len() as i64;
        for staged in self.pending.values() {
            for (k, v) in staged {
                match (self.live.contains(k), v) {
                    (true, None) => n -= 1,
                    (false, Some(_)) => n += 1,
                    _ => {}
                }
            }
        }
        n.max(0) as usize
    }

    /// Stage one op into its bucket (no sync — the writer batches
    /// syncs). Returns the sequence the batch will carry.
    fn append_op(&mut self, op: &str, key: &[u8], value: Option<&[u8]>) -> std::io::Result<u64> {
        self.pending
            .entry(bucket_of(key))
            .or_default()
            .insert(key.to_vec(), value.map(|v| v.to_vec()));
        let _ = op; // SET and DEL are the same staging; the value decides
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    /// The group commit: every touched bucket becomes one op in
    /// a single engine transaction (its full new content, or a
    /// delete when empty), committed with one WAL sync.
    pub fn sync(&mut self) -> std::io::Result<()> {
        if !self.pending.is_empty() {
            let staged = std::mem::take(&mut self.pending);
            let mut buckets: Vec<u32> = staged.keys().cloned().collect();
            buckets.sort_unstable();
            // ONE engine transaction for the whole writer batch:
            // the batch is the ack unit (one sync), so it is the
            // atomic unit too — and the WAL carries one commit
            // marker per batch instead of one per bucket.
            let mut txns = Vec::with_capacity(1);
            let mut batch = Txn::new();
            for b in &buckets {
                // merged view WITHOUT the staged map (it was taken):
                // committed content + this bucket's staged changes
                let mut map = self
                    .shared
                    .snapshot()
                    .get(&bucket_coord(*b))
                    .map(|body| parse_bucket(&body))
                    .unwrap_or_default();
                for (k, v) in &staged[b] {
                    match v {
                        Some(val) => {
                            map.insert(k.clone(), val.clone());
                        }
                        None => {
                            map.remove(k);
                        }
                    }
                }
                if map.is_empty() {
                    batch = batch.delete(bucket_coord(*b));
                } else {
                    batch = batch.set(bucket_coord(*b), &bucket_body(&map));
                }
            }
            txns.push(batch);
            self.core.commit_batch(txns)?;
            for changes in staged.values() {
                for (k, v) in changes {
                    match v {
                        Some(_) => {
                            self.live.insert(k.clone());
                        }
                        None => {
                            self.live.remove(k);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Commit anything staged, then fold the WAL into the base.
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        self.sync()?;
        self.core.checkpoint()?;
        self.snapshot_seq = self.core.checkpoint_seq();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Group-commit writer: the only thread that appends. Clients ack only
// after the batch's bytes are fdatasync'd — the original's durability
// contract, in ~40 lines instead of a WAL subsystem.

enum Op {
    Set(Vec<u8>, Vec<u8>, Sender<Vec<u8>>),
    Del(Vec<u8>, Sender<Vec<u8>>),
    Get(Vec<u8>, Sender<Vec<u8>>),
    Exists(Vec<u8>, Sender<Vec<u8>>),
    DbSize(Sender<Vec<u8>>),
    Info(Sender<Vec<u8>>),
    Checkpoint(Sender<Vec<u8>>),
}

fn bulk(v: &[u8]) -> Vec<u8> {
    let mut r = format!("${}\r\n", v.len()).into_bytes();
    r.extend_from_slice(v);
    r.extend_from_slice(b"\r\n");
    r
}

/// All state ops — reads included — flow through this one ordered stream,
/// so a client's GET after its own SET always sees that SET. Writes in a
/// batch are appended and applied first (reads in the same batch see
/// them), then one fdatasync releases every write ack in the batch.
fn writer_loop(rx: Receiver<Op>, store: Arc<RwLock<Store>>) {
    while let Ok(first) = rx.recv() {
        let mut ops = vec![first];
        // Accumulate briefly so one fsync covers a real batch. A bare
        // try_recv drain syncs ~10 ops at a time and fsync latency becomes
        // the throughput ceiling (measured: 11.9k ops/s before this window,
        // fsync-bound at ~0.84 ms per micro-batch).
        while ops.len() < 512 {
            match rx.recv_timeout(std::time::Duration::from_micros(300)) {
                Ok(op) => ops.push(op),
                Err(_) => break,
            }
        }
        let mut deferred: Vec<(Sender<Vec<u8>>, Vec<u8>)> = Vec::new();
        let mut ok = true;
        {
            let mut st = store.write().unwrap();
            for op in ops {
                match op {
                    Op::Set(k, v, ack) => {
                        ok &= st.append_op("SET", &k, Some(&v)).is_ok();
                        deferred.push((ack, b"+OK\r\n".to_vec()));
                    }
                    Op::Del(k, ack) => {
                        let existed = st.get(&k).is_some();
                        ok &= st.append_op("DEL", &k, None).is_ok();
                        deferred.push((ack, if existed { b":1\r\n".to_vec() } else { b":0\r\n".to_vec() }));
                    }
                    Op::Get(k, ack) => {
                        let reply = match st.get(&k) {
                            Some(v) => bulk(&v),
                            None => b"$-1\r\n".to_vec(),
                        };
                        let _ = ack.send(reply);
                    }
                    Op::Exists(k, ack) => {
                        let _ = ack.send(if st.get(&k).is_some() { b":1\r\n".to_vec() } else { b":0\r\n".to_vec() });
                    }
                    Op::DbSize(ack) => {
                        let _ = ack.send(format!(":{}\r\n", st.len()).into_bytes());
                    }
                    Op::Info(ack) => {
                        let body = format!(
                            "keys={}\nnext_seq={}\nsnapshot_seq={}\n",
                            st.len(),
                            st.next_seq,
                            st.snapshot_seq
                        );
                        let _ = ack.send(bulk(body.as_bytes()));
                    }
                    Op::Checkpoint(ack) => {
                        let r = st.checkpoint().is_ok();
                        deferred.push((ack, if r { b"+OK\r\n".to_vec() } else { b"-ERR checkpoint failed\r\n".to_vec() }));
                    }
                }
            }
            ok &= st.sync().is_ok();
        }
        for (ack, mut reply) in deferred {
            if !ok {
                reply = b"-ERR write failed\r\n".to_vec();
            }
            let _ = ack.send(reply);
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP (Redis protocol) surface: PING SET GET DEL EXISTS DBSIZE
// CHECKPOINT INFO. Enough to be driven by redis-benchmark and any Redis
// client; the original's server file is 1,244 lines for the same surface.

use std::io::BufReader;
use std::net::{TcpListener, TcpStream};

fn read_command(r: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    use std::io::BufRead;
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line).ok()? == 0 {
        return None;
    }
    let line = String::from_utf8_lossy(&line).trim().to_string();
    if let Some(rest) = line.strip_prefix('*') {
        let n: usize = rest.parse().ok()?;
        let mut args = Vec::with_capacity(n);
        for _ in 0..n {
            let mut hdr = Vec::new();
            r.read_until(b'\n', &mut hdr).ok()?;
            let hdr = String::from_utf8_lossy(&hdr).trim().to_string();
            let len: usize = hdr.strip_prefix('$')?.parse().ok()?;
            let mut buf = vec![0u8; len + 2];
            r.read_exact(&mut buf).ok()?;
            buf.truncate(len);
            args.push(buf);
        }
        Some(args)
    } else if line.is_empty() {
        Some(Vec::new())
    } else {
        Some(line.split_whitespace().map(|s| s.as_bytes().to_vec()).collect())
    }
}

/// One reply slot per parsed command, in order. The reader thread never
/// waits for acks — it parses the whole pipeline and queues slots; the
/// socket thread drains slots in order, waiting on each op's ack. (The
/// first version waited per command and silently serialized every
/// pipeline to one op in flight per connection.)
enum Slot {
    Now(Vec<u8>),
    Later(Receiver<Vec<u8>>),
}

fn handle_conn(stream: TcpStream, tx: Sender<Op>) {
    // Replies are many and small; Nagle + delayed ACK stalls each pipeline
    // batch ~40 ms without this (measured — same class of tuning the
    // original's async server gets from its runtime).
    let _ = stream.set_nodelay(true);
    let writer = stream.try_clone().unwrap();
    let (slot_tx, slot_rx) = channel::<Slot>();
    std::thread::spawn(move || {
        let mut w = writer;
        while let Ok(slot) = slot_rx.recv() {
            let bytes = match slot {
                Slot::Now(b) => b,
                Slot::Later(rx) => match rx.recv() {
                    Ok(b) => b,
                    Err(_) => return,
                },
            };
            if w.write_all(&bytes).is_err() {
                return;
            }
        }
    });
    let mut reader = BufReader::new(stream);
    while let Some(args) = read_command(&mut reader) {
        if args.is_empty() {
            continue;
        }
        let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
        let slot = match cmd.as_str() {
            "PING" => Slot::Now(b"+PONG\r\n".to_vec()),
            "SET" if args.len() == 3 => {
                let (a, r) = channel();
                let _ = tx.send(Op::Set(args[1].clone(), args[2].clone(), a));
                Slot::Later(r)
            }
            "GET" if args.len() == 2 => {
                let (a, r) = channel();
                let _ = tx.send(Op::Get(args[1].clone(), a));
                Slot::Later(r)
            }
            "DEL" if args.len() == 2 => {
                let (a, r) = channel();
                let _ = tx.send(Op::Del(args[1].clone(), a));
                Slot::Later(r)
            }
            "EXISTS" if args.len() == 2 => {
                let (a, r) = channel();
                let _ = tx.send(Op::Exists(args[1].clone(), a));
                Slot::Later(r)
            }
            "DBSIZE" => {
                let (a, r) = channel();
                let _ = tx.send(Op::DbSize(a));
                Slot::Later(r)
            }
            "INFO" => {
                let (a, r) = channel();
                let _ = tx.send(Op::Info(a));
                Slot::Later(r)
            }
            "CHECKPOINT" => {
                let (a, r) = channel();
                let _ = tx.send(Op::Checkpoint(a));
                Slot::Later(r)
            }
            _ => Slot::Now(b"-ERR unknown command\r\n".to_vec()),
        };
        if slot_tx.send(slot).is_err() {
            return;
        }
    }
}

fn main() {
    let mut path = PathBuf::from("store.phext");
    let mut addr = "127.0.0.1:16399".to_string();
    let mut argv = std::env::args().skip(1);
    while let Some(a) = argv.next() {
        match a.as_str() {
            "--path" => path = PathBuf::from(argv.next().expect("--path needs a value")),
            "--addr" => addr = argv.next().expect("--addr needs a value"),
            other => panic!("unknown argument: {other}"),
        }
    }
    let store = Arc::new(RwLock::new(Store::open(&path).expect("open store")));
    let (tx, rx) = channel::<Op>();
    let writer_store = store.clone();
    std::thread::spawn(move || writer_loop(rx, writer_store));
    let listener = TcpListener::bind(&addr).expect("bind");
    eprintln!("holo-phext listening on {addr}, store at {}", path.display());
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            let t = tx.clone();
            std::thread::spawn(move || handle_conn(s, t));
        }
    }
}


// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("holo-phext-{name}-{}.phext", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let wal = p.with_file_name(format!(
            "{}.wal.phext",
            p.file_name().unwrap().to_string_lossy().strip_suffix(".phext").unwrap()
        ));
        let _ = std::fs::remove_file(&wal);
        p
    }

    #[test]
    fn codec_roundtrips_anything() {
        for raw in [
            b"plain".as_slice(),
            b"b64:impostor".as_slice(),
            b"line one\nline two".as_slice(),
            b"has\x17scroll-break".as_slice(),
            &[0u8, 159, 146, 150], // not UTF-8 at all
            b"".as_slice(),
        ] {
            assert_eq!(decode_field(&encode_field(raw)).unwrap(), raw);
        }
    }

    #[test]
    fn appends_sync_and_reopen() {
        let path = tmp("engine");
        let mut st = Store::open(&path).unwrap();
        for i in 0..10_050u32 {
            st.append_op("SET", format!("key-{i}").as_bytes(), Some(b"v")).unwrap();
        }
        st.sync().unwrap();
        assert_eq!(st.len(), 10_050);
        drop(st);
        // Recovery replays the engine WAL: everything acked is present.
        let st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 10_050);
        assert_eq!(st.get(b"key-9999"), Some(b"v".to_vec()));
        assert_eq!(st.get(b"key-0"), Some(b"v".to_vec()));
        // The base document after a checkpoint is a plain phext that
        // the reference parser reads directly, at the key's coordinate.
        drop(st);
        let mut st = Store::open(&path).unwrap();
        st.checkpoint().unwrap();
        drop(st);
        let text = std::fs::read_to_string(&path).unwrap();
        let at = phext::fetch(&text, bucket_coord(bucket_of(b"key-42")));
        assert!(at.contains("key=key-42"), "checkpointed scroll unreadable: {at:?}");
    }

    #[test]
    fn checkpoint_folds_wal_and_reopens() {
        let path = tmp("ckpt");
        let mut st = Store::open(&path).unwrap();
        for i in 0..300u32 {
            st.append_op("SET", format!("k{i}").as_bytes(), Some(format!("v{i}").as_bytes())).unwrap();
        }
        st.append_op("DEL", b"k7", None).unwrap();
        st.checkpoint().unwrap();
        // The WAL restarts at the checkpoint: meta scroll only.
        let wal = path.with_file_name(format!(
            "{}.wal.phext",
            path.file_name().unwrap().to_string_lossy().strip_suffix(".phext").unwrap()
        ));
        let wal_text = std::fs::read_to_string(&wal).unwrap();
        assert_eq!(phext::explode(&wal_text).len(), 1, "WAL must be folded away");
        // One engine transaction per touched bucket per batch, so
        // the checkpoint sequence is the batch's transaction count,
        // not the op count — read it from the store, not a constant.
        let expect = format!("checkpoint_seq={}", st.snapshot_seq);
        assert!(wal_text.contains(&expect), "{wal_text}");
        drop(st);
        let mut st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 299);
        assert_eq!(st.get(b"k7"), None);
        assert_eq!(st.get(b"k42"), Some(b"v42".to_vec()));
        // Appends after a checkpoint land in the fresh WAL.
        st.append_op("SET", b"after", Some(b"ckpt")).unwrap();
        st.sync().unwrap();
        drop(st);
        let st = Store::open(&path).unwrap();
        assert_eq!(st.get(b"after"), Some(b"ckpt".to_vec()));
        assert_eq!(st.len(), 300);
    }

    #[test]
    fn torn_wal_tail_is_discarded_not_fatal() {
        let path = tmp("torn");
        let mut st = Store::open(&path).unwrap();
        for i in 0..50u32 {
            st.append_op("SET", format!("k{i}").as_bytes(), Some(b"v")).unwrap();
        }
        st.sync().unwrap();
        drop(st);
        // Simulate a crash mid-append: garbage bytes at the WAL's tail.
        let wal = path.with_file_name(format!(
            "{}.wal.phext",
            path.file_name().unwrap().to_string_lossy().strip_suffix(".phext").unwrap()
        ));
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal).unwrap();
        use std::io::{Read, Write};
        f.write_all(b"\x1c\x19partial-forged-entry").unwrap();
        f.write_all(&[0xFF, 0xFE, 0x00]).unwrap();
        drop(f);
        let mut st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 50, "torn tail must not be applied");
        // The store keeps working at the truncation point.
        st.append_op("SET", b"after", Some(b"torn")).unwrap();
        st.sync().unwrap();
        drop(st);
        let st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 51);
        assert_eq!(st.get(b"after"), Some(b"torn".to_vec()));
    }
}
