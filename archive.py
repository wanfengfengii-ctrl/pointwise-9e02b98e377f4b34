"""实验室单日观测档案(串行实现, Python + SQLite)。

接口:
    arch = Archive(root)
    (成果号, 段代)          = arch.ingest(req, day, events)
    (账面, 非空和, 原件列表) = arch.read(day, ver=None)
    新段代                  = arch.compact(req, day, gen)

事件: {"id","entity","source","seq","prev"?,"raw"}
    raw 为 UTF-8 JSON 原字节: {} 不改 / {"v":null} 清空 / {"v":整数} 赋值。

存储布局:
    root/archive.db                      账: 事件索引 / 版本 / 段登记 / 请求存根
    root/days/<day>/gen-<g>/seg-*.jsonl  载荷段: 首行段头, 其余每行一件原件

不变式:
    * id 全库唯一; (day, entity, seq) 唯一;
      prev 限已收或同批的同日同实体更早序。
    * 成果号只增; 版本 v 冻结 "version<=v" 的全部已收原件, 按(实体,序号)重放。
    * 读账与续收只从现行段代取载荷, 逐段核 sha256。
    * 压实不增成果; 候选独立读回核齐身份、字节和出处才换代实删旧段。
    * 过期 / 错漏 / 提交失败: 保留旧目录、段和账, 无新首执。
    * 提交后中断: 重开补删(旧段代目录与未入账孤儿段)。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import threading
from contextlib import contextmanager
from urllib.parse import quote, unquote

__all__ = ["Archive", "ArchiveError", "RejectError", "StaleError", "CandidateError"]


class ArchiveError(Exception):
    """档案操作失败基类。"""


class RejectError(ArchiveError):
    """坏件、冲突或同号异参: 整批拒绝。"""


class StaleError(ArchiveError):
    """段代过期。"""


class CandidateError(ArchiveError):
    """压实候选读回核验不齐(错漏)。"""


# ---------------------------------------------------------------- 基础工具

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _raw_bytes(raw) -> bytes:
    """校验并归一 raw 原字节: {} 不改 / {"v":null} 清空 / {"v":整数} 赋值。"""
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    elif isinstance(raw, (bytearray, memoryview)):
        raw = bytes(raw)
    if not isinstance(raw, bytes):
        raise RejectError("raw 须为 UTF-8 JSON 原字节")
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise RejectError("raw 非合法 UTF-8 JSON") from None
    if not isinstance(obj, dict):
        raise RejectError("raw 须为 JSON 对象")
    if len(obj) == 0:
        return raw
    if set(obj) == {"v"}:
        v = obj["v"]
        if v is None:
            return raw
        if isinstance(v, bool) or not isinstance(v, int):
            raise RejectError('raw 的 "v" 只许 null 或整数')
        return raw
    raise RejectError('raw 只许 {} / {"v":null} / {"v":整数}')


def _apply_raw(raw: bytes):
    """重放语义: 返 ('noop'|'clear'|'assign', 值)。"""
    obj = json.loads(raw.decode("utf-8"))
    if not obj:
        return ("noop", None)
    v = obj.get("v")
    if v is None:
        return ("clear", None)
    return ("assign", v)


_SEG_KEYS = {"id", "entity", "source", "seq", "prev", "raw"}


def _dump_segment(meta: dict, recs: list) -> bytes:
    """序列化载荷段: 首行 {"_meta":...}, 其余每行一件, raw 以原文字串保留原字节。"""
    lines = [json.dumps({"_meta": meta}, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"))]
    for r in recs:
        lines.append(json.dumps(
            {"id": r["id"], "entity": r["entity"], "source": r["source"],
             "seq": r["seq"], "prev": r["prev"], "raw": r["raw"].decode("utf-8")},
            ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _parse_segment(data: bytes):
    """解析载荷段, 返 (meta, [rec, ...]); rec["raw"] 还原为原字节。"""
    try:
        text = data.decode("utf-8")
        lines = text.split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        if not lines:
            raise ValueError("空段")
        head = json.loads(lines[0])
        if not isinstance(head, dict) or not isinstance(head.get("_meta"), dict):
            raise ValueError("缺段头")
        recs = []
        for ln in lines[1:]:
            o = json.loads(ln)
            if not isinstance(o, dict) or set(o) != _SEG_KEYS:
                raise ValueError("件行残缺")
            if not isinstance(o["raw"], str):
                raise ValueError("件行 raw 非串")
            recs.append({"id": o["id"], "entity": o["entity"], "source": o["source"],
                         "seq": o["seq"], "prev": o["prev"],
                         "raw": o["raw"].encode("utf-8")})
        return head["_meta"], recs
    except (UnicodeDecodeError, ValueError) as e:
        raise ArchiveError(f"载荷段不可解析: {e}") from None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS days(
  day         TEXT PRIMARY KEY,
  current_gen INTEGER NOT NULL,
  version     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  id         TEXT PRIMARY KEY,
  day        TEXT NOT NULL,
  entity     TEXT NOT NULL,
  source     TEXT NOT NULL,
  seq        INTEGER NOT NULL,
  prev       TEXT,
  raw_sha256 TEXT NOT NULL,
  version    INTEGER NOT NULL,
  seg_file   TEXT NOT NULL,
  UNIQUE(day, entity, seq)
);
CREATE TABLE IF NOT EXISTS segments(
  day      TEXT NOT NULL,
  gen      INTEGER NOT NULL,
  file     TEXT NOT NULL,
  sha256   TEXT NOT NULL,
  n_events INTEGER NOT NULL,
  PRIMARY KEY(day, gen, file)
);
CREATE TABLE IF NOT EXISTS requests(
  req    TEXT PRIMARY KEY,
  op     TEXT NOT NULL,
  params TEXT NOT NULL,
  result TEXT NOT NULL
);
"""


class Archive:
    """串行单日观测档案。fault_hooks 仅供核验注入故障(键为钩子名, 值为可调用)。"""

    def __init__(self, root, fault_hooks=None):
        self._root = os.path.abspath(root)
        self._fault = fault_hooks if fault_hooks is not None else {}
        self._lock = threading.RLock()
        os.makedirs(self._root, exist_ok=True)
        self._db = sqlite3.connect(os.path.join(self._root, "archive.db"),
                                   isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(_SCHEMA)
        self._sweep()

    # ---------------------------------------------------------- 基础

    def close(self):
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def _hook(self, name, *args):
        fn = self._fault.get(name)
        if fn is not None:
            fn(*args)

    @contextmanager
    def _txn(self):
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self._db.execute("ROLLBACK")
            raise
        try:
            self._db.execute("COMMIT")
        except Exception:
            try:
                self._db.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def _day_dir(self, day):
        return os.path.join(self._root, "days", quote(day, safe=""))

    def _gen_dir(self, day, gen):
        return os.path.join(self._day_dir(day), f"gen-{gen:06d}")

    @staticmethod
    def _fsync_dir(path):
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    def _atomic_write(self, path, data: bytes):
        d = os.path.dirname(path)
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            self._fsync_dir(d)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _sweep(self):
        """重开收尾: 补删已换代的旧段、未提交的候选与未入账孤儿, 不动现行段账。"""
        days_root = os.path.join(self._root, "days")
        os.makedirs(days_root, exist_ok=True)
        known = {r["day"]: r["current_gen"]
                 for r in self._db.execute("SELECT day, current_gen FROM days")}
        for name in os.listdir(days_root):
            day_dir = os.path.join(days_root, name)
            if not os.path.isdir(day_dir):
                continue
            day = unquote(name)
            if day not in known:
                shutil.rmtree(day_dir, ignore_errors=True)      # 从未入账
                continue
            cur = known[day]
            for gname in os.listdir(day_dir):
                gdir = os.path.join(day_dir, gname)
                if os.path.isdir(gdir) and gname != f"gen-{cur:06d}":
                    shutil.rmtree(gdir, ignore_errors=True)     # 旧段/未提交候选: 补删
            cur_dir = os.path.join(day_dir, f"gen-{cur:06d}")
            if os.path.isdir(cur_dir):
                registered = {r["file"] for r in self._db.execute(
                    "SELECT file FROM segments WHERE day=? AND gen=?", (day, cur))}
                for f in os.listdir(cur_dir):
                    if f not in registered:
                        os.remove(os.path.join(cur_dir, f))     # 提交前中断的孤儿段

    # ---------------------------------------------------------- 请求存根

    @staticmethod
    def _canon(obj) -> str:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"))

    def _req_lookup(self, req, op, params_json):
        row = self._db.execute(
            "SELECT op, params, result FROM requests WHERE req=?", (req,)).fetchone()
        if row is None:
            return None
        if row["op"] != op or row["params"] != params_json:
            raise RejectError("同号异参, 拒绝复领")
        return json.loads(row["result"])

    def _req_record(self, req, op, params_json, result):
        self._db.execute(
            "INSERT INTO requests(req, op, params, result) VALUES(?,?,?,?)",
            (req, op, params_json, self._canon(result)))

    # ---------------------------------------------------------- 校验

    @staticmethod
    def _check_req_day(req, day):
        if not isinstance(req, str) or not req:
            raise RejectError("req 须为非空字符串")
        if not isinstance(day, str) or not day:
            raise RejectError("day 须为非空字符串")

    @staticmethod
    def _normalize(ev):
        if not isinstance(ev, dict):
            raise RejectError("事件须为对象")
        keys = set(ev)
        if not {"id", "entity", "source", "seq", "raw"} <= keys:
            raise RejectError("事件缺字段")
        if not keys <= {"id", "entity", "source", "seq", "prev", "raw"}:
            raise RejectError("事件多字段")
        for k in ("id", "entity", "source"):
            if not isinstance(ev[k], str) or not ev[k]:
                raise RejectError(f"{k} 须为非空字符串")
        seq = ev["seq"]
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise RejectError("seq 须为整数")
        prev = ev.get("prev")
        if prev is not None and (not isinstance(prev, str) or not prev):
            raise RejectError("prev 须为非空字符串或缺省")
        raw = _raw_bytes(ev["raw"])
        return {"id": ev["id"], "entity": ev["entity"], "source": ev["source"],
                "seq": seq, "prev": prev, "raw": raw, "raw_sha256": _sha256(raw)}

    def _load_payloads(self, day, gen):
        """只从现行段代取载荷, 逐段核 sha256 与件数。返 {id: rec}。"""
        out = {}
        for seg in self._db.execute(
                "SELECT file, sha256, n_events FROM segments"
                " WHERE day=? AND gen=? ORDER BY file", (day, gen)):
            path = os.path.join(self._gen_dir(day, gen), seg["file"])
            try:
                with open(path, "rb") as f:
                    data = f.read()
            except OSError as e:
                raise ArchiveError(f"现行段缺失: {seg['file']}: {e}") from None
            if _sha256(data) != seg["sha256"]:
                raise ArchiveError(f"现行段字节与账不符: {seg['file']}")
            meta, recs = _parse_segment(data)
            if meta.get("day") != day or meta.get("gen") != gen:
                raise ArchiveError(f"现行段身份不符: {seg['file']}")
            if len(recs) != seg["n_events"]:
                raise ArchiveError(f"现行段件数与账不符: {seg['file']}")
            for rec in recs:
                if rec["id"] in out:
                    raise ArchiveError(f"现行段间重件: {rec['id']}")
                out[rec["id"]] = rec
        return out

    # ---------------------------------------------------------- 接纳

    def ingest(self, req, day, events):
        """接纳一批事件, 成功返 (成果号, 段代)。坏件或冲突整批拒绝, 同件去重。"""
        with self._lock:
            self._check_req_day(req, day)
            if not isinstance(events, (list, tuple)):
                raise RejectError("events 须为列表")
            norm = [self._normalize(e) for e in events]
            params = self._canon({
                "op": "ingest", "day": day,
                "events": [[e["id"], e["entity"], e["source"], e["seq"],
                            e["prev"], e["raw_sha256"]] for e in norm]})
            cached = self._req_lookup(req, "ingest", params)
            if cached is not None:
                return (cached["version"], cached["gen"])       # 同号同参复领

            # 批内: 同件去重, 异件冲突整批拒绝
            by_id, by_es = {}, {}
            for e in norm:
                dup = by_id.get(e["id"])
                if dup is not None:
                    if dup != e:
                        raise RejectError(f"同号异件: {e['id']}")
                    continue
                if (e["entity"], e["seq"]) in by_es:
                    raise RejectError(f"同日同实体同序冲突: {e['entity']}#{e['seq']}")
                by_id[e["id"]] = e
                by_es[(e["entity"], e["seq"])] = e

            # 库内: 已收同件去重, 冲突整批拒绝
            new_events = []
            for e in by_id.values():
                row = self._db.execute(
                    "SELECT day, entity, source, seq, prev, raw_sha256"
                    " FROM events WHERE id=?", (e["id"],)).fetchone()
                if row is not None:
                    if (row["day"], row["entity"], row["source"], row["seq"],
                            row["prev"], row["raw_sha256"]) != (
                            day, e["entity"], e["source"], e["seq"],
                            e["prev"], e["raw_sha256"]):
                        raise RejectError(f"同号异件冲突: {e['id']}")
                    continue                                        # 已收同件
                clash = self._db.execute(
                    "SELECT id FROM events WHERE day=? AND entity=? AND seq=?",
                    (day, e["entity"], e["seq"])).fetchone()
                if clash is not None:
                    raise RejectError(f"同日同实体同序冲突: {e['entity']}#{e['seq']}")
                new_events.append(e)

            # 前置: 限已收或同批的同日同实体更早序
            for e in new_events:
                p = e["prev"]
                if p is None:
                    continue
                prow = self._db.execute(
                    "SELECT day, entity FROM events WHERE id=?", (p,)).fetchone()
                if prow is not None:
                    if prow["day"] != day or prow["entity"] != e["entity"]:
                        raise RejectError(f"前置非同日同实体: {p}")
                    continue
                pb = by_id.get(p)
                if pb is None or pb["entity"] != e["entity"] or pb["seq"] >= e["seq"]:
                    raise RejectError(f"前置未收或非更早序: {p}")

            row = self._db.execute(
                "SELECT current_gen, version FROM days WHERE day=?", (day,)).fetchone()
            cur_gen, cur_ver = (row["current_gen"], row["version"]) if row else (1, 0)

            if not new_events:                                      # 全为同件: 去重不增版
                with self._txn():
                    self._req_record(req, "ingest", params,
                                     {"version": cur_ver, "gen": cur_gen})
                return (cur_ver, cur_gen)

            new_ver = cur_ver + 1
            fname = f"seg-{new_ver:06d}.jsonl"
            meta = {"kind": "ingest", "day": day, "gen": cur_gen,
                    "version": new_ver, "req": req, "count": len(new_events)}
            data = _dump_segment(meta, new_events)
            path = os.path.join(self._gen_dir(day, cur_gen), fname)
            self._atomic_write(path, data)                          # 先落载荷段
            self._hook("ingest_segment_written", path)
            try:
                with self._txn():                                   # 再入账, 整批原子
                    if row is None:
                        self._db.execute(
                            "INSERT INTO days(day, current_gen, version) VALUES(?,?,?)",
                            (day, cur_gen, new_ver))
                    else:
                        self._db.execute("UPDATE days SET version=? WHERE day=?",
                                         (new_ver, day))
                    for e in new_events:
                        self._db.execute(
                            "INSERT INTO events(id, day, entity, source, seq, prev,"
                            " raw_sha256, version, seg_file) VALUES(?,?,?,?,?,?,?,?,?)",
                            (e["id"], day, e["entity"], e["source"], e["seq"],
                             e["prev"], e["raw_sha256"], new_ver, fname))
                    self._db.execute(
                        "INSERT INTO segments(day, gen, file, sha256, n_events)"
                        " VALUES(?,?,?,?,?)",
                        (day, cur_gen, fname, _sha256(data), len(new_events)))
                    self._req_record(req, "ingest", params,
                                     {"version": new_ver, "gen": cur_gen})
            except sqlite3.IntegrityError as e:
                raise RejectError(f"入账冲突: {e}") from None
            return (new_ver, cur_gen)

    # ---------------------------------------------------------- 读账

    def read(self, day, ver=None):
        """读账: 默认最新版。返 ({实体:(值,生效id)}, 非空和, 本版原件列表)。"""
        with self._lock:
            if not isinstance(day, str) or not day:
                raise ArchiveError("day 须为非空字符串")
            row = self._db.execute(
                "SELECT current_gen, version FROM days WHERE day=?", (day,)).fetchone()
            if row is None:
                if ver is None:
                    return ({}, 0, [])
                raise ArchiveError(f"日账不存在: {day}")
            cur_gen, latest = row["current_gen"], row["version"]
            if ver is None:
                v = latest
            else:
                if isinstance(ver, bool) or not isinstance(ver, int):
                    raise ArchiveError("版本号须为整数")
                v = ver
            if v < 1 or v > latest:
                raise ArchiveError(f"版本不存在: {v} (现行 {latest})")
            rows = self._db.execute(
                "SELECT id, entity, source, seq, prev FROM events"
                " WHERE day=? AND version<=? ORDER BY entity, seq",
                (day, v)).fetchall()
            payloads = self._load_payloads(day, cur_gen)            # 只取现行段载荷
            final, originals = {}, []
            for r in rows:                                          # 按(实体,序号)重放
                rec = payloads.get(r["id"])
                if rec is None:
                    raise ArchiveError(f"账有件而现行段无件: {r['id']}")
                op, val = _apply_raw(rec["raw"])
                if op == "assign":
                    final[r["entity"]] = (val, r["id"])
                elif op == "clear":
                    final.pop(r["entity"], None)
                originals.append({"id": r["id"], "entity": r["entity"],
                                  "source": r["source"], "seq": r["seq"],
                                  "prev": r["prev"], "raw": rec["raw"]})
            values = {e: final[e] for e in sorted(final)}
            return (values, sum(v for v, _ in values.values()), originals)

    # ---------------------------------------------------------- 压实

    def compact(self, req, day, gen):
        """压实: 合并该日全部现行载荷段为新一代, 候选核齐才换代实删旧段。返新段代。"""
        with self._lock:
            self._check_req_day(req, day)
            if isinstance(gen, bool) or not isinstance(gen, int) or gen < 1:
                raise RejectError("段代须为正整数")
            params = self._canon({"op": "compact", "day": day, "gen": gen})
            cached = self._req_lookup(req, "compact", params)
            if cached is not None:
                return cached["gen"]                                # 同号同参复领
            row = self._db.execute(
                "SELECT current_gen, version FROM days WHERE day=?", (day,)).fetchone()
            if row is None or row["current_gen"] != gen:
                cur = "无" if row is None else row["current_gen"]
                raise StaleError(f"段代过期: 现行 {cur}, 收到 {gen}")
            new_gen = gen + 1
            rows = self._db.execute(
                "SELECT id, entity, source, seq, prev, raw_sha256 FROM events"
                " WHERE day=? ORDER BY entity, seq", (day,)).fetchall()
            payloads = self._load_payloads(day, gen)
            if len(payloads) != len(rows) or any(r["id"] not in payloads for r in rows):
                raise ArchiveError("账与现行段不齐, 拒绝压实")
            sources = [{"file": s["file"], "sha256": s["sha256"], "count": s["n_events"]}
                       for s in self._db.execute(
                           "SELECT file, sha256, n_events FROM segments"
                           " WHERE day=? AND gen=? ORDER BY file", (day, gen))]
            merged = [payloads[r["id"]] for r in rows]
            meta = {"kind": "compact", "day": day, "gen": new_gen, "req": req,
                    "count": len(merged), "sources": sources}
            data = _dump_segment(meta, merged)
            fname = f"seg-compact-{new_gen:06d}.jsonl"
            new_dir = self._gen_dir(day, new_gen)
            if os.path.isdir(new_dir):
                shutil.rmtree(new_dir)                              # 清上次未提交候选残留
            os.makedirs(new_dir)
            cpath = os.path.join(new_dir, fname)
            self._atomic_write(cpath, data)                         # 候选落盘
            self._hook("compact_candidate_written", cpath)
            try:
                self._verify_candidate(day, gen, new_gen, fname, rows, sources)
                self._hook("compact_verified", new_gen)
                with self._txn():                                   # 换代入账
                    self._db.execute("UPDATE days SET current_gen=? WHERE day=?",
                                     (new_gen, day))
                    self._db.execute("DELETE FROM segments WHERE day=? AND gen=?",
                                     (day, gen))
                    self._db.execute(
                        "INSERT INTO segments(day, gen, file, sha256, n_events)"
                        " VALUES(?,?,?,?,?)",
                        (day, new_gen, fname, _sha256(data), len(merged)))
                    self._db.execute("UPDATE events SET seg_file=? WHERE day=?",
                                     (fname, day))
                    self._req_record(req, "compact", params, {"gen": new_gen})
            except Exception:
                shutil.rmtree(new_dir, ignore_errors=True)          # 错漏/提交失败: 撤候选
                raise
            self._hook("compact_committed", new_gen, self._gen_dir(day, gen))
            shutil.rmtree(self._gen_dir(day, gen), ignore_errors=True)  # 实删旧段
            return new_gen

    def _verify_candidate(self, day, old_gen, new_gen, fname, rows, sources):
        """候选独立读回: 核齐身份、字节和出处。"""
        path = os.path.join(self._gen_dir(day, new_gen), fname)
        try:
            with open(path, "rb") as f:
                data = f.read()
            meta, recs = _parse_segment(data)
        except (OSError, ArchiveError) as e:
            raise CandidateError(f"候选读回失败: {e}") from None
        if meta.get("kind") != "compact" or meta.get("day") != day \
                or meta.get("gen") != new_gen:
            raise CandidateError("候选身份不符")
        if meta.get("count") != len(rows) or len(recs) != len(rows):
            raise CandidateError("候选件数不符")
        if meta.get("sources") != sources:
            raise CandidateError("候选出处不符")
        fresh = self._load_payloads(day, old_gen)                   # 独立重读现行段核哈希
        if len(fresh) != len(rows):
            raise CandidateError("现行段件数与账不符")
        for rec, r in zip(recs, rows):
            if (rec["id"], rec["entity"], rec["source"], rec["seq"], rec["prev"]) != \
                    (r["id"], r["entity"], r["source"], r["seq"], r["prev"]):
                raise CandidateError(f"候选身份不符: {rec['id']}")
            src = fresh.get(rec["id"])
            if src is None or rec["raw"] != src["raw"]:
                raise CandidateError(f"候选字节不符: {rec['id']}")
            if _sha256(rec["raw"]) != r["raw_sha256"]:
                raise CandidateError(f"候选字节与账不符: {rec['id']}")
