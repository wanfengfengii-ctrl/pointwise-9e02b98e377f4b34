"""运行入口: python3 -B cli.py verify | demo

verify  核验三版账、去重/冲突/坏件、压实、坏候选、提交前后中断; 失败非零。
demo    演示三版账 -> 压实删旧段 -> 重开复读, 附交接说明。
"""

import os
import shutil
import sys
import tempfile
import traceback

import archive
from archive import Archive, ArchiveError, CandidateError, RejectError, StaleError

DAY = "2026-11-22"


def ev(id_, entity, source, seq, prev, raw):
    d = {"id": id_, "entity": entity, "source": source, "seq": seq, "raw": raw}
    if prev is not None:
        d["prev"] = prev
    return d


def boom(*_args):
    raise RuntimeError("模拟中断")


# ------------------------------------------------------------------ 核验

def cmd_verify():
    tmp = tempfile.mkdtemp(prefix="archive-verify-")
    print(f"核验工作目录: {tmp}")
    fails = []

    def check(name, cond, info=""):
        print(f"  [{'ok' if cond else 'FAIL'}] {name}"
              + (f"  -- {info}" if (info and not cond) else ""))
        if not cond:
            fails.append(name)

    def raises(name, kinds, fn):
        try:
            fn()
        except kinds:
            check(name, True)
            return
        except Exception as e:
            check(name, False, f"异常类型不符: {type(e).__name__}: {e}")
            return
        check(name, False, "未抛异常")

    root = os.path.join(tmp, "a1")

    print("[1] 三版账: 同实体依次收 10赋7 / 30清空 / 20赋9 (号即序)")
    A = Archive(root)
    r1 = A.ingest("r1", DAY, [ev("e1", "alpha", "lab-a", 10, None, b'{"v": 7}')])
    check("首件接纳返(1,1)", r1 == (1, 1), f"got {r1}")
    m, s, orig = A.read(DAY)
    check("一版合计7", s == 7 and m == {"alpha": (7, "e1")}, f"{m} {s}")
    check("一版含前1件且为原字节", len(orig) == 1 and orig[0]["raw"] == b'{"v": 7}')
    r2 = A.ingest("r2", DAY, [ev("e2", "alpha", "lab-a", 30, "e1", b'{"v": null}')])
    check("二件接纳返(2,1)", r2 == (2, 1), f"got {r2}")
    m, s, orig = A.read(DAY)
    check("清空后合计0且账面无值", s == 0 and m == {}, f"{m} {s}")
    check("二版含前2件, 旧账原件在册",
          [o["id"] for o in orig] == ["e1", "e2"] and orig[0]["raw"] == b'{"v": 7}')
    r3 = A.ingest("r3", DAY, [ev("e3", "alpha", "lab-a", 20, "e1", b'{"v":9}')])
    check("三件接纳返(3,1)", r3 == (3, 1), f"got {r3}")
    m, s, orig = A.read(DAY)
    check("三版合计0(按序重放10赋7->20赋9->30清空)", s == 0 and m == {}, f"{m} {s}")
    check("三版含前3件按(实体,序号)排列", [o["id"] for o in orig] == ["e1", "e3", "e2"])
    m1, s1, o1 = A.read(DAY, 1)
    m2, s2, o2 = A.read(DAY, 2)
    check("历史版回读: 一版7/二版0, 各含前1/2件",
          s1 == 7 and s2 == 0 and len(o1) == 1 and len(o2) == 2
          and m1 == {"alpha": (7, "e1")})
    raises("越界版本拒绝", ArchiveError, lambda: A.read(DAY, 4))
    raises("零版拒绝", ArchiveError, lambda: A.read(DAY, 0))
    check("未知日默认空账", A.read("2099-01-01") == ({}, 0, []))

    print("[2] 同件去重 / 冲突 / 坏件 / 前置 / 同号异参")
    check("同号同参复领原票",
          A.ingest("r1", DAY, [ev("e1", "alpha", "lab-a", 10, None, b'{"v": 7}')]) == (1, 1))
    check("同件新号去重不增版",
          A.ingest("r9", DAY, [ev("e1", "alpha", "lab-a", 10, None, b'{"v": 7}')]) == (3, 1))
    raises("同号异参拒绝(原字节不同即异参)", RejectError,
           lambda: A.ingest("r1", DAY, [ev("e1", "alpha", "lab-a", 10, None, b'{"v":7}')]))
    raises("同号异件冲突", RejectError,
           lambda: A.ingest("r10", DAY, [ev("e1", "alpha", "lab-a", 10, None, b'{"v":8}')]))
    raises("同实体同序冲突", RejectError,
           lambda: A.ingest("r11", DAY, [ev("eX", "alpha", "lab-a", 10, None, b'{"v":8}')]))
    bads = [b'{"v":1.5}', b'{"v":"x"}', b'{"v":true}', b'[1]', b'{"x":1}', b'{bad', b'\xff\xfe']
    for i, bad in enumerate(bads):
        raises(f"坏raw整批拒绝#{i}", RejectError,
               lambda bad=bad, i=i: A.ingest(f"rb{i}", DAY,
                                             [ev(f"eb{i}", "beta", "lab-b", 1, None, bad)]))
    raises("好坏同批整批拒绝", RejectError,
           lambda: A.ingest("r12", DAY, [ev("e4", "beta", "lab-b", 1, None, b'{"v":1}'),
                                         ev("e5", "beta", "lab-b", 2, "e4", b'{"v":1.5}')]))
    raises("整批拒绝后无新首执(第4版不存在)", ArchiveError, lambda: A.read(DAY, 4))
    raises("前置未收拒绝", RejectError,
           lambda: A.ingest("r13", DAY, [ev("e6", "beta", "lab-b", 1, "nope", b'{"v":1}')]))
    raises("前置跨实体拒绝", RejectError,
           lambda: A.ingest("r14", DAY, [ev("e7", "beta", "lab-b", 1, "e1", b'{"v":1}')]))
    raises("同批前置更晚序拒绝", RejectError,
           lambda: A.ingest("r15", DAY, [ev("e8", "beta", "lab-b", 2, "e9", b'{"v":1}'),
                                         ev("e9", "beta", "lab-b", 5, None, b'{"v":2}')]))
    r16 = A.ingest("r16", DAY, [ev("e9b", "beta", "lab-b", 2, None, b'{"v":2}'),
                                ev("e8b", "beta", "lab-b", 5, "e9b", b'{"v":1}')])
    check("同批前置更早序接纳", r16 == (4, 1), f"got {r16}")
    m, s, _ = A.read(DAY)
    check("beta重放得值1, 非空和1", m.get("beta") == (1, "e8b") and s == 1, f"{m} {s}")
    r17 = A.ingest("r17", DAY, [ev("e10", "gamma", "lab-c", 1, None, b'{}')])
    check("不改件接纳", r17 == (5, 1), f"got {r17}")
    m, s, orig = A.read(DAY)
    check("不改件不入账面但在原件册",
          "gamma" not in m and s == 1 and any(o["id"] == "e10" for o in orig))

    print("[3] 压实: 合并现行段, 候选核齐后换代实删旧段, 不增成果")
    snap = {v: A.read(DAY, v) for v in (1, 2, 3, 4, 5)}
    g = A.compact("c1", DAY, 1)
    check("压实返新段代2", g == 2, f"got {g}")
    check("压实同号同参复领", A.compact("c1", DAY, 1) == 2)
    raises("压实同号异参拒绝", RejectError, lambda: A.compact("c1", DAY, 2))
    raises("过期段代拒绝", StaleError, lambda: A.compact("c2", DAY, 1))
    raises("压实不增成果(第6版不存在)", ArchiveError, lambda: A.read(DAY, 6))
    check("压实前后五版账一致", all(A.read(DAY, v) == snap[v] for v in snap))
    day_dir = os.path.join(root, "days", DAY)
    check("旧段已实删, 现行段代唯一",
          not os.path.exists(os.path.join(day_dir, "gen-000001"))
          and os.listdir(day_dir) == ["gen-000002"], os.listdir(day_dir))
    r18 = A.ingest("r18", DAY, [ev("e11", "alpha", "lab-a", 40, "e2", b'{"v":5}')])
    check("压实后续收增版", r18 == (6, 2), f"got {r18}")
    m, s, _ = A.read(DAY)
    check("续收后按序重放", m["alpha"] == (5, "e11") and s == 6, f"{m} {s}")
    snap = {v: A.read(DAY, v) for v in (1, 2, 3, 4, 5, 6)}

    print("[4] 坏候选: 读回核不齐, 保留旧目录、段和账")
    A.close()
    hooks = {}
    A = Archive(root, fault_hooks=hooks)

    def corrupt(path):
        with open(path, "ab") as f:
            f.write(b'{"id":"zzz","entity":"x","source":"s","seq":1,'
                    b'"prev":null,"raw":"{}"}\n')

    hooks["compact_candidate_written"] = corrupt
    raises("坏候选拒绝", CandidateError, lambda: A.compact("c3", DAY, 2))
    hooks.clear()
    check("坏候选后候选目录已撤", not os.path.exists(os.path.join(day_dir, "gen-000003")))
    raises("坏候选后段代仍2(代1过期)", StaleError, lambda: A.compact("c3x", DAY, 1))
    check("坏候选后账不动", all(A.read(DAY, v) == snap[v] for v in snap))

    print("[5] 提交前中断: 无新首执, 可重执")
    check("正常压实到段代3", A.compact("c4", DAY, 2) == 3)
    hooks["compact_verified"] = boom
    raises("提交前中断", RuntimeError, lambda: A.compact("c5", DAY, 3))
    hooks.clear()
    check("中断后候选目录已撤", not os.path.exists(os.path.join(day_dir, "gen-000004")))
    raises("中断后段代仍3(代2过期)", StaleError, lambda: A.compact("c5y", DAY, 2))
    check("中断后账不动", all(A.read(DAY, v) == snap[v] for v in snap))
    check("失败后同号同参可重执", A.compact("c5", DAY, 3) == 4)

    print("[6] 提交后中断: 重开补删")
    hooks["compact_committed"] = boom
    raises("提交后中断", RuntimeError, lambda: A.compact("c6", DAY, 4))
    hooks.clear()
    old_dir = os.path.join(day_dir, "gen-000004")
    check("中断时旧段暂未删(现场保留)", os.path.isdir(old_dir))
    A.close()
    A = Archive(root)                                   # 重开
    check("重开补删旧段", not os.path.exists(old_dir))
    check("重开复领同票", A.compact("c6", DAY, 4) == 5)
    check("重开后六版账一致", all(A.read(DAY, v) == snap[v] for v in snap))

    print("[7] 续收中断留孤儿, 重开清除; 读账只取现行段")
    A.close()
    A = Archive(root, fault_hooks={"ingest_segment_written": boom})
    raises("落段后未入账中断", RuntimeError,
           lambda: A.ingest("r19", DAY, [ev("e12", "delta", "lab-d", 1, None, b'{"v":3}')]))
    cur_dir = os.path.join(day_dir, "gen-000005")
    check("孤儿段暂存现场", any(f.startswith("seg-000007") for f in os.listdir(cur_dir)),
          os.listdir(cur_dir))
    A.close()
    A = Archive(root)                                   # 重开
    check("重开清除孤儿段", not any(f.startswith("seg-000007") for f in os.listdir(cur_dir)))
    raises("中断未增版(第7版不存在)", ArchiveError, lambda: A.read(DAY, 7))
    check("同号同参重执成功",
          A.ingest("r19", DAY, [ev("e12", "delta", "lab-d", 1, None, b'{"v":3}')]) == (7, 5))
    check("现行段代目录唯一", os.listdir(day_dir) == ["gen-000005"])
    m, s, orig = A.read(DAY)
    check("终账正确(账面/非空和/原件)",
          m == {"alpha": (5, "e11"), "beta": (1, "e8b"), "delta": (3, "e12")}
          and s == 9 and len(orig) == 8, f"{m} {s} {len(orig)}")
    check("原件按(实体,序号)排列",
          [o["id"] for o in orig] == ["e1", "e3", "e2", "e11",
                                      "e9b", "e8b", "e12", "e10"])
    A.close()

    print("=" * 60)
    if fails:
        print(f"核验失败 {len(fails)} 项: {fails}")
        return 1
    print("核验全部通过")
    return 0


# ------------------------------------------------------------------ 演示

HANDOVER = """\
交接说明 —— 实验室单日观测档案
================================
一、运行
  核验: python3 -B cli.py verify   (三版账/去重冲突/坏候选/提交前后中断, 失败非零)
  演示: python3 -B cli.py demo     (三版账 -> 压实删旧段 -> 重开复读三版账)

二、接口
  Archive(root)                打开或建档; 重开自动补删(旧段代目录、未入账孤儿段)
  ingest(req, day, events)     整批原子接纳, 返 (成果号, 段代); 同件去重,
                               坏件或冲突整批拒绝; 同号同参复领原票, 异参拒绝
  read(day, ver=None)          默认最新版, 返 ({实体:(值,生效id)}, 非空和, 本版原件列表)
  compact(req, day, gen)       合并该日全部现行载荷段, 候选独立读回核齐身份、字节、
                               出处才换代实删旧段; 不增成果; 返新段代

三、账目语义
  raw 为 UTF-8 JSON 原字节: {} 不改 / {"v":null} 清空 / {"v":整数} 赋值
  id 全库唯一; seq 同日同实体唯一; prev 限已收或同批的同日同实体更早序
  成果(版本)冻结发布时已收原件, 按(实体,序号)重放
  清空只出账面: 旧账原件仍在本版原件列表, 只验合计会漏账, 须连原件一起核

四、存储与恢复
  archive.db                   账: days / events / segments / requests
  days/<day>/gen-XXXXXX/*.jsonl 载荷段: 首行段头, 其余每行一件原件(raw 原字节)
  读账与续收只从现行段代取载荷, 逐段核 sha256
  过期 / 错漏 / 提交失败: 保留旧目录、段和账, 无新首执
  提交后中断: 重开补删旧段; 落段未入账中断: 重开清除孤儿段
"""


def _ls(day_dir):
    if not os.path.isdir(day_dir):
        return []
    out = []
    for g in sorted(os.listdir(day_dir)):
        for f in sorted(os.listdir(os.path.join(day_dir, g))):
            out.append(f"{g}/{f}")
    return out


def cmd_demo():
    root = os.path.join(os.getcwd(), "demo-archive")
    if os.path.isdir(root):
        shutil.rmtree(root)
    day_dir = os.path.join(root, "days", DAY)
    print("=" * 68)
    print("实验室单日观测档案 · 演示")
    print(f"日账: {DAY}    档案根: {root}")
    print("=" * 68)

    A = Archive(root)

    def show(tag):
        m, s, orig = A.read(DAY)
        ids = [o["id"] for o in orig]
        raws = [o["raw"].decode("utf-8") for o in orig]
        print(f"    {tag} 账面={m} 非空和={s}")
        print(f"    {tag} 本版原件={len(orig)}件 {ids} 原字节={raws}")

    print("[收] 同实体依次收 10赋7 / 30清空 / 20赋9 (号即序)")
    print("  批 req-001: e-001 alpha seq=10 raw={\"v\": 7}")
    print("    接纳 ->", A.ingest("req-001", DAY,
                               [ev("e-001", "alpha", "lab-obs-1", 10, None, b'{"v": 7}')]))
    show("一版")
    print('  批 req-002: e-002 alpha seq=30 prev=e-001 raw={"v": null}  (读数7清空)')
    print("    接纳 ->", A.ingest("req-002", DAY,
                               [ev("e-002", "alpha", "lab-obs-1", 30, "e-001",
                                   b'{"v": null}')]))
    show("二版")
    print("    ※ 合计已是0, 但旧账原件 e-001 仍在册; 只验合计会漏掉它")
    print('  批 req-003: e-003 alpha seq=20 prev=e-001 raw={"v": 9}')
    print("    接纳 ->", A.ingest("req-003", DAY,
                               [ev("e-003", "alpha", "lab-obs-1", 20, "e-001", b'{"v": 9}')]))
    show("三版")
    print("    ※ 按(实体,序号)重放: 10赋7 -> 20赋9 -> 30清空, 故合计0")

    print(f"[段] 压实前载荷段: {_ls(day_dir)}")
    new_gen = A.compact("req-c01", DAY, 1)
    print(f"[压实] compact(req-c01, {DAY}, gen=1) -> 新段代 {new_gen}")
    print("    合并 3 段为 1 段; 候选独立读回核齐身份/字节/出处后换代, 旧段实删")
    print(f"[段] 压实后载荷段: {_ls(day_dir)}  (gen-000001 已删, 成果号不增)")

    A.close()
    print("[重开] Archive(root) 重新打开, 自动补删检查完成")
    A = Archive(root)
    for v in (1, 2, 3):
        m, s, orig = A.read(DAY, v)
        print(f"    第{v}版: 账面={m} 非空和={s} "
              f"原件={len(orig)}件 {[o['id'] for o in orig]}")
    print("[复领] ingest(req-001, 同参) ->",
          A.ingest("req-001", DAY,
                   [ev("e-001", "alpha", "lab-obs-1", 10, None, b'{"v": 7}')]),
          " (原票)   compact(req-c01, 同参) ->", A.compact("req-c01", DAY, 1), " (原票)")
    try:
        A.compact("req-c02", DAY, 1)
    except StaleError as e:
        print(f"[拒] compact(req-c02, gen=1) 过期拒绝: {e}")
    A.close()

    print()
    print(HANDOVER)
    print(f"演示档案保留于: {root}")
    return 0


# ------------------------------------------------------------------ 入口

def main(argv):
    if len(argv) != 2 or argv[1] not in ("verify", "demo"):
        print("用法: python3 -B cli.py verify|demo", file=sys.stderr)
        return 2
    try:
        if argv[1] == "verify":
            return cmd_verify()
        return cmd_demo()
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
