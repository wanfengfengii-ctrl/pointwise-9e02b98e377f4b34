# 实验室单日观测档案

串行实现 `archive.Archive(root)`(Python + SQLite):单日观测档案的接纳、读账与压实。

## 接口

- `ingest(req, day, events)` → `(成果号, 段代)`:整批原子接纳 `{id,entity,source,seq,prev?,raw}`;
  `raw` 为 UTF-8 JSON 原字节,`{}` 不改 / `{"v":null}` 清空 / `{"v":整数}` 赋值。
- `read(day, ver=None)` → `({实体:(值,生效id)}, 非空和, 本版原件列表)`:默认最新版。
- `compact(req, day, gen)` → 新段代:合并该日全部现行载荷段,候选核齐才换代实删旧段,不增成果。

## 运行

```sh
python3 -B cli.py verify   # 核验: 三版账/去重冲突/坏候选/提交前后中断, 失败非零
python3 -B cli.py demo     # 演示: 三版账 -> 压实删旧段 -> 重开复读, 附交接说明
```

详见 [HANDOVER.md](HANDOVER.md)。
