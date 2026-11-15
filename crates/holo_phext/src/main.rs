//! holo-phext — HoloStore's storage core, re-implemented phext-natively.
//!
//! The original persists one logical thing (an ordered, durable KV log plus
//! its folded state) through five representations: a CRC-framed binary WAL
//! (two backends), Fjall's LSM files with hand-rolled version-key encodings,
//! a JSON cluster-state checkpoint, a JSON recovery manifest, and a protobuf
//! wire codec. This implementation persists the same core in exactly one
//! representation: a phext file.
//!
//! Layout of store.phext (Library.Shelf.Series/Collection.Volume.Book/
//! Chapter.Section.Scroll; everything here is 1.1.1/1.V.B/1.1.1-shaped):
//!   1.1.1/1.1.1/1.1.1   manifest: format, snapshot_seq, next_seq (text lines)
//!   Volume 2            snapshot: live keys, sorted; Chapter = page of 1000,
//!                       Scroll = slot in page; content is key=/value= lines.
//!                       Coordinates are pagination; identity is in content.
//!   Volume 3            WAL: one scroll per committed op, addressed by
//!                       Chapter = (seq-1)/10000+1, Scroll = (seq-1)%10000+1;
//!                       content is seq=/op=/key=/value= lines. The WAL is
//!                       the last volume so appends are pure file appends:
//!                       a delimiter run to the next coordinate, then text.
//!
//! Durability rule (the whole protocol): an op is acknowledged only after
//! its WAL bytes are written and fdatasync'd. Recovery replays snapshot +
//! WAL; the first WAL scroll that fails validation ends the log and the
//! file is truncated at that scroll. Checkpoint folds the WAL into the
//! snapshot by rewriting the file once, through libphext::implode.
//!
//! Parsing and whole-file writing go through libphext (the reference
//! implementation) — never a hand-rolled parser. The append fast path emits
//! delimiter bytes directly (that is serialization, not parsing) and the
//! test suite proves libphext reads every appended byte back exactly.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};

use libphext::phext::{self, Coordinate, XCoordinate, YCoordinate, ZCoordinate};

// ---------------------------------------------------------------------------
// Coordinates

fn coord(volume: usize, chapter: usize, scroll: usize) -> Coordinate {
    Coordinate {
        z: ZCoordinate { library: 1, shelf: 1, series: 1 },
        y: YCoordinate { collection: 1, volume, book: 1 },
        x: XCoordinate { chapter, section: 1, scroll },
    }
}

fn manifest_coord() -> Coordinate {
    coord(1, 1, 1)
}
fn snapshot_coord(index: usize) -> Coordinate {
    coord(2, index / 1000 + 1, index % 1000 + 1)
}
/// WAL coordinates are slots relative to the snapshot baseline, so the
/// first entry after a checkpoint lands at Volume 3, Chapter 1, Scroll 1 —
/// one volume break from the snapshot's last scroll, no empty scrolls.
fn wal_coord(seq: u64, snapshot_seq: u64) -> Coordinate {
    let s = (seq - snapshot_seq - 1) as usize;
    coord(3, s / 10_000 + 1, s % 10_000 + 1)
}

fn comps(c: &Coordinate) -> [usize; 9] {
    [
        c.z.library, c.z.shelf, c.z.series, c.y.collection, c.y.volume, c.y.book, c.x.chapter,
        c.x.section, c.x.scroll,
    ]
}

/// Delimiter bytes, highest dimension first (library .. scroll).
const BREAKS: [u8; 9] = [0x01, 0x1F, 0x1E, 0x1D, 0x1C, 0x1A, 0x19, 0x18, 0x17];

/// Emit delimiter bytes that advance `cur` to `tgt` (tgt must not precede
/// cur). A break increments its dimension and resets all lower dimensions
/// to 1 — that is the phext addressing rule, applied forwards.
fn emit_until(out: &mut Vec<u8>, cur: &mut [usize; 9], tgt: [usize; 9]) {
    while *cur != tgt {
        let i = (0..9)
            .find(|&i| cur[i] < tgt[i])
            .expect("append target precedes current coordinate");
        out.push(BREAKS[i]);
        cur[i] += 1;
        for j in i + 1..9 {
            cur[j] = 1;
        }
    }
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
// The store

pub struct Store {
    path: PathBuf,
    file: File,
    map: HashMap<Vec<u8>, Vec<u8>>,
    snapshot_seq: u64,
    next_seq: u64,
    /// Components of the coordinate the file currently ends at.
    cur: [usize; 9],
}

fn manifest_text(snapshot_seq: u64, next_seq: u64) -> String {
    format!("format=holo-phext/1\nnode=1\nsnapshot_seq={snapshot_seq}\nnext_seq={next_seq}\n")
}

impl Store {
    pub fn open(path: &Path) -> std::io::Result<Store> {
        if !path.exists() {
            let text = phext::implode(HashMap::from([(manifest_coord(), manifest_text(0, 1))]));
            std::fs::write(path, text)?;
        }
        let bytes = std::fs::read(path)?;
        // A torn final write can split a UTF-8 sequence; keep the valid prefix.
        let valid = match std::str::from_utf8(&bytes) {
            Ok(_) => bytes.len(),
            Err(e) => e.valid_up_to(),
        };
        let text = String::from_utf8_lossy(&bytes[..valid]).into_owned();
        let exploded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| phext::explode(&text)))
            .unwrap_or_default();

        let manifest = exploded.get(&manifest_coord()).cloned().unwrap_or_default();
        let mf = parse_fields(&manifest);
        let snapshot_seq: u64 = mf.get("snapshot_seq").and_then(|v| v.parse().ok()).unwrap_or(0);

        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut snap_coords: Vec<Coordinate> = exploded
            .keys()
            .filter(|c| c.y.volume == 2)
            .cloned()
            .collect();
        snap_coords.sort_by_key(|c| (c.x.chapter, c.x.scroll));
        let mut last_coord = manifest_coord();
        for c in &snap_coords {
            let f = parse_fields(&exploded[c]);
            if let (Some(k), Some(v)) = (f.get("key"), f.get("value")) {
                if let (Some(k), Some(v)) = (decode_field(k), decode_field(v)) {
                    map.insert(k, v);
                }
            }
            last_coord = *c;
        }

        // Replay the WAL in coordinate order (== seq order by construction).
        // The first entry that is incomplete or out of sequence ends the
        // log; the file is truncated at that scroll (torn tail recovery).
        // Completeness marker: every committed entry ends with '\n' — a
        // write torn mid-line fails that check. (Phext has no per-scroll
        // checksum; this is the gap, recorded in PHEXT.md round 2.)
        let mut wal: Vec<(Coordinate, String)> = exploded
            .iter()
            .filter(|(c, _)| c.y.volume == 3)
            .map(|(c, content)| (*c, content.clone()))
            .collect();
        wal.sort_by_key(|(c, _)| (c.x.chapter, c.x.scroll));
        let mut expect = snapshot_seq + 1;
        let mut truncate_at: Option<usize> = None;
        for (c, content) in &wal {
            let f = parse_fields(content);
            let seq: Option<u64> = f.get("seq").and_then(|s| s.parse().ok());
            let ok = content.ends_with('\n')
                && seq == Some(expect)
                && match f.get("op").map(String::as_str) {
                    Some("SET") => f.contains_key("key") && f.contains_key("value"),
                    Some("DEL") => f.contains_key("key"),
                    _ => false,
                };
            if !ok {
                // Truncate at the START of this scroll's delimiter run, so
                // the file ends exactly at the previous scroll's content
                // and the next append re-emits the run cleanly. Walking
                // back over break bytes is safe: committed scroll content
                // never ends with a break byte (the codec forbids them in
                // raw fields, and every entry ends with '\n').
                let mut at = phext::offset(&text, *c);
                while at > 0 && is_break_byte(text.as_bytes()[at - 1]) {
                    at -= 1;
                }
                truncate_at = Some(at);
                break;
            }
            let key = decode_field(f["key"].as_str()).unwrap_or_default();
            match f["op"].as_str() {
                "SET" => {
                    map.insert(key, decode_field(f["value"].as_str()).unwrap_or_default());
                }
                _ => {
                    map.remove(&key);
                }
            }
            last_coord = *c;
            expect += 1;
        }
        let next_seq = expect;

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        if let Some(at) = truncate_at {
            file.set_len(at as u64)?;
            file.sync_data()?;
        } else if valid < bytes.len() {
            file.set_len(valid as u64)?;
            file.sync_data()?;
        }
        file.seek(SeekFrom::End(0))?;

        Ok(Store {
            path: path.to_path_buf(),
            file,
            map,
            snapshot_seq,
            next_seq,
            cur: comps(&last_coord),
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).cloned()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Append one op to the WAL (no fsync — the writer batches syncs),
    /// then apply it. Returns the assigned seq.
    fn append_op(&mut self, op: &str, key: &[u8], value: Option<&[u8]>) -> std::io::Result<u64> {
        let seq = self.next_seq;
        let mut content = format!("seq={seq}\nop={op}\nkey={}\n", encode_field(key));
        if let Some(v) = value {
            content += &format!("value={}\n", encode_field(v));
        }
        let target = wal_coord(seq, self.snapshot_seq);
        let mut bytes = Vec::with_capacity(content.len() + 9);
        let mut cur = self.cur;
        emit_until(&mut bytes, &mut cur, comps(&target));
        bytes.extend_from_slice(content.as_bytes());
        self.file.write_all(&bytes)?;
        self.cur = cur;
        self.next_seq += 1;
        match op {
            "SET" => {
                self.map.insert(key.to_vec(), value.unwrap_or_default().to_vec());
            }
            _ => {
                self.map.remove(key);
            }
        }
        Ok(seq)
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.file.sync_data()
    }

    /// Fold snapshot + WAL into a fresh file: manifest + snapshot only.
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        let snapshot_seq = self.next_seq - 1;
        let mut entries: Vec<(&Vec<u8>, &Vec<u8>)> = self.map.iter().collect();
        entries.sort();
        let mut file_map: HashMap<Coordinate, String> =
            HashMap::from([(manifest_coord(), manifest_text(snapshot_seq, self.next_seq))]);
        let mut last = manifest_coord();
        for (i, (k, v)) in entries.iter().enumerate() {
            let c = snapshot_coord(i);
            file_map.insert(
                c,
                format!("key={}\nvalue={}\n", encode_field(k), encode_field(v)),
            );
            last = c;
        }
        let text = phext::implode(file_map);
        let tmp = self.path.with_extension("phext.tmp");
        std::fs::write(&tmp, text)?;
        File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &self.path)?;
        // Sync the directory so the rename itself is durable. A bare
        // relative filename has parent "" — that means ".", not an error.
        let dir = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(dir)?.sync_all()?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        self.snapshot_seq = snapshot_seq;
        self.cur = comps(&last);
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
    fn appends_are_libphext_conformant() {
        let path = tmp("conform");
        let mut st = Store::open(&path).unwrap();
        // 10,050 ops cross a WAL chapter boundary (10,000 scrolls per chapter).
        for i in 0..10_050u32 {
            st.append_op("SET", format!("key-{i}").as_bytes(), Some(b"v")).unwrap();
        }
        st.sync().unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        // The reference parser reads back exactly what the append path wrote.
        let at = phext::fetch(&text, wal_coord(10_050, 0));
        assert!(at.contains("seq=10050"), "tail scroll unreadable: {at:?}");
        let first = phext::fetch(&text, wal_coord(1, 0));
        assert!(first.contains("seq=1"));
        let exploded = phext::explode(&text);
        assert_eq!(exploded.keys().filter(|c| c.y.volume == 3).count(), 10_050);
        drop(st);
        let st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 10_050);
        assert_eq!(st.get(b"key-9999"), Some(b"v".to_vec()));
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
        let text = std::fs::read_to_string(&path).unwrap();
        let exploded = phext::explode(&text);
        assert_eq!(exploded.keys().filter(|c| c.y.volume == 3).count(), 0, "WAL must be folded away");
        assert_eq!(exploded.keys().filter(|c| c.y.volume == 2).count(), 299);
        drop(st);
        let mut st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 299);
        assert_eq!(st.get(b"k7"), None);
        assert_eq!(st.get(b"k42"), Some(b"v42".to_vec()));
        // Appends after a checkpoint land in a fresh WAL volume.
        st.append_op("SET", b"after", Some(b"ckpt")).unwrap();
        st.sync().unwrap();
        drop(st);
        let st = Store::open(&path).unwrap();
        assert_eq!(st.get(b"after"), Some(b"ckpt".to_vec()));
        assert_eq!(st.len(), 300);
    }

    #[test]
    fn torn_tail_is_truncated_not_fatal() {
        let path = tmp("torn");
        let mut st = Store::open(&path).unwrap();
        for i in 0..50u32 {
            st.append_op("SET", format!("k{i}").as_bytes(), Some(b"v")).unwrap();
        }
        st.sync().unwrap();
        drop(st);
        // Simulate a crash mid-append: partial scroll, then garbage bytes.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"\x17seq=51\nop=SET\nkey=torn\nvalue=hal").unwrap();
        f.write_all(&[0xFF, 0xFE, 0x00]).unwrap();
        drop(f);
        let mut st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 50, "torn entry must not be applied");
        assert_eq!(st.get(b"torn"), None);
        // The store keeps working at the truncation point.
        st.append_op("SET", b"after", Some(b"torn")).unwrap();
        st.sync().unwrap();
        drop(st);
        let st = Store::open(&path).unwrap();
        assert_eq!(st.len(), 51);
        assert_eq!(st.get(b"after"), Some(b"torn".to_vec()));
    }
}
