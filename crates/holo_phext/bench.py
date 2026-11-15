#!/usr/bin/env python3
"""Pipelined RESP bench/load/verify client for holo-phext (and any Redis-ish server).

usage:
  bench.py load  --port P --n 30000 --pipeline 50 --threads 10 --progress FILE
  bench.py bench --port P --n 200000 --pipeline 50 --threads 10 --op set|get --prefix bench
  bench.py verify --port P --n 30000 --prefix key
Progress file (load): after each pipeline batch is acked, the highest acked
index is written — a kill -9 mid-load leaves an exact durability frontier.
"""
import argparse, socket, sys, threading, time

def enc(*args):
    out = [f"*{len(args)}\r\n".encode()]
    for a in args:
        if isinstance(a, str): a = a.encode()
        out.append(f"${len(a)}\r\n".encode()); out.append(a); out.append(b"\r\n")
    return b"".join(out)

class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.buf = b""
    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d: raise ConnectionError("closed")
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line
    def reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in b"+-:": return rest
        if t == b"$":
            n = int(rest)
            if n < 0: return None
            while len(self.buf) < n + 2:
                d = self.s.recv(65536)
                if not d: raise ConnectionError("closed")
                self.buf += d
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        raise ValueError(f"bad reply {line!r}")
    def pipe(self, cmds):
        self.s.sendall(b"".join(cmds))
        return [self.reply() for _ in cmds]

def worker_range(n, threads, tid):
    chunk = n // threads
    return range(tid * chunk, (tid + 1) * chunk if tid < threads - 1 else n)

def do_load(a):
    def run(tid):
        c = Conn(a.port); batch = []; acked = 0
        prog = open(f"{a.progress}.{tid}", "w") if a.progress else None
        for i in worker_range(a.n, a.threads, tid):
            batch.append(enc("SET", f"{a.prefix}-{i}", f"value-{i}"))
            if len(batch) >= a.pipeline:
                c.pipe(batch); acked += len(batch); batch = []
                if prog: prog.write(f"{acked}\n"); prog.flush()
        if batch: c.pipe(batch)
    ts = [threading.Thread(target=run, args=(t,)) for t in range(a.threads)]
    t0 = time.time(); [t.start() for t in ts]; [t.join() for t in ts]
    print(f"load: {a.n} SETs acked in {time.time()-t0:.2f}s")

def do_bench(a):
    # preload for get bench happens in load phase; here just run the op
    counts = [0] * a.threads
    def run(tid):
        c = Conn(a.port); batch = []; n = 0
        for i in worker_range(a.n, a.threads, tid):
            if a.op == "set": batch.append(enc("SET", f"{a.prefix}-{i}", f"value-{i}"))
            else: batch.append(enc("GET", f"{a.prefix}-{i}"))
            if len(batch) >= a.pipeline:
                c.pipe(batch); batch = []; n += a.pipeline
        if batch: c.pipe(batch); n += len(batch)
        counts[tid] = n
    ts = [threading.Thread(target=run, args=(t,)) for t in range(a.threads)]
    t0 = time.time(); [t.start() for t in ts]; [t.join() for t in ts]
    dt = time.time() - t0
    print(f"bench {a.op}: {sum(counts)} ops, {a.threads} threads, pipeline {a.pipeline}: {sum(counts)/dt:,.0f} ops/s")

def do_verify(a):
    c = Conn(a.port); missing = 0; batch = []; idx = []
    def flush():
        nonlocal missing
        if not batch: return
        for i, r in zip(idx, c.pipe(batch)):
            if r != f"value-{i}".encode(): missing += 1
        batch.clear(); idx.clear()
    for i in range(a.n):
        batch.append(enc("GET", f"{a.prefix}-{i}")); idx.append(i)
        if len(batch) >= 200: flush()
    flush()
    print(f"verify: {a.n - missing}/{a.n} keys correct, {missing} missing/wrong")
    sys.exit(1 if missing else 0)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["load", "bench", "verify"])
    p.add_argument("--port", type=int, default=16399)
    p.add_argument("--n", type=int, default=10000)
    p.add_argument("--pipeline", type=int, default=50)
    p.add_argument("--threads", type=int, default=10)
    p.add_argument("--op", default="set")
    p.add_argument("--prefix", default="key")
    p.add_argument("--progress", default="")
    a = p.parse_args()
    {"load": do_load, "bench": do_bench, "verify": do_verify}[a.mode](a)
