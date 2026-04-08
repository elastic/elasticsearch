# ESQL Native Primitive Types — Three-Stage Plan

## Background

ESQL widens narrow numeric types before planning: `float→DOUBLE`, `short→INTEGER`, `byte→INTEGER`. This produces correct results but wastes memory (2-4x overhead per value) and loses schema fidelity. `FloatBlock` already exists in the compute engine (added for dense_vector in PR #109746). `ShortBlock`, `ByteBlock`, and `CharBlock` do not exist.

External data sources are new (no backward compatibility concern). ES indices have always widened (changing this is a breaking change in user-visible schema).

## Stage 1: Float for External Data Sources (MVP)

**Scope:** Stop widening float→DOUBLE for external sources only. ES indices continue widening.

**Why this works:** External sources use `ReferenceAttribute` (resolved via `ResolveExternalRelations`). ES indices use `FieldAttribute` (resolved via `Analyzer.resolveAttributes` which calls `widenSmallNumeric()`). The two paths are separate — no backward compatibility risk.

**Approach:**
- Format readers (Parquet, ORC, CSV, NDJSON) stop mapping FLOAT→DOUBLE, report native FLOAT
- `PlannerUtils.toElementType()` and `isRepresentable()` — add FLOAT support
- Implicit FLOAT→DOUBLE promotion for scalar functions/operators (single interception point, not 36+ function changes)
- Native float aggregation dispatch (SumFloat, MinFloat, etc. already exist in compute engine)
- BlockHash, MultivalueDedupe, BatchEncoder — add FLOAT handling
- Response serialization — add FLOAT support (2 files)

**What users see:** Parquet FLOAT columns show `float` in schema (not `double`). Expressions over float columns produce `double` (implicit promotion). Aggregations use native float. ES index float fields still show `double` (no change).

**Estimate:** ~4 weeks

**Parent issue:** #189 (MVP)

---

## Stage 2: Short/Byte for External Data Sources (Post-MVP)

**Scope:** Create `ShortBlock` and `ByteBlock` in the compute engine. Stop widening short→INTEGER and byte→INTEGER for external sources only. ES indices continue widening.

**Approach:**
- Generate ShortBlock/ByteBlock from existing templates (~50-60 generated files each)
- Add SHORT/BYTE to ElementType, BlockFactory, BlockHash, MultivalueDedupe, BatchEncoder
- Format readers report native SHORT/BYTE instead of widening
- Implicit SHORT→INT and BYTE→INT promotion for evaluators (same pattern as Stage 1)
- Response serialization for SHORT/BYTE

**Practical note:** Short and byte are rare in external formats. Parquet INT16 is annotated int32, not a first-class type. The primary benefit is schema fidelity, not memory savings. Float (Stage 1) covers the high-impact case.

**Estimate:** ~6-8 weeks per type, ~12-14 weeks total for short + byte

**Dependency:** Stage 1 (establishes the promotion pattern and plumbing)

---

## Stage 3: Native Types for All of ESQL (Post-MVP)

**Scope:** Stop widening float/short/byte for ES indices. This is a **breaking change** — user-visible schema changes from `double`/`integer` to `float`/`short`/`byte` for fields mapped with those types.

**Approach:**
- Remove `widenSmallNumeric()` calls in `Analyzer.java:430` for FLOAT, SHORT, BYTE
- Gate behind `TransportVersion` / capability flag (rolling upgrade safety)
- Lucene BlockLoader changes — `SortedNumericDoubleValues` currently widens float at the doc values layer; may need `FloatPointValues` path
- Comprehensive backward compatibility testing (Kibana dashboards, client libraries, saved queries)

**Breaking change mitigation:**
- Feature flag / transport version gating
- Possibly a cluster setting to opt in/out
- Deprecation period in minor release, enforce in next major

**Estimate:** ~4-6 weeks (incremental on Stages 1+2 infrastructure, but dominated by compatibility work)

**Dependency:** Stages 1 + 2

---

## Summary

| Stage | Scope | Types | Breaking? | Estimate | Priority |
|-------|-------|-------|-----------|----------|----------|
| 1 | Float for external sources | float | No | 4w | MVP |
| 2 | Short/byte for external sources | short, byte | No | 12-14w | Post-MVP |
| 3 | All types for ES indices | float, short, byte | **Yes** | 4-6w | Post-MVP |

**char** is omitted — no external format produces char columns and it has no practical use case.

Stage 1 is the high-value target: 50% memory savings for float columns (common in Parquet/ORC), FloatBlock already exists, zero backward compatibility risk.
