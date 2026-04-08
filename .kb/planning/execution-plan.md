# ES|QL Data Sources — Execution Plan

**Date**: 2026-03-11
**GA Target**: ES 9.5, Feature Freeze June 23, 2026 (15 calendar weeks from today)
**Tracking**: elastic/esql-planning#189

---

## Team & Velocity

### Individual Profiles

| Person | Allocation | Planning velocity | Evidence | Domain expertise |
|--------|-----------|------------------|----------|------------------|
| **costin** | 100% | 2.0 pw/cw | 37 PRs in 3.1 weeks (burst); 1.0 pw/cw averaged over 17 weeks. Currently in active burst. | Framework architect: entire external sources SPI, distributed execution, format readers, CSV, parallelism |
| **swallez** | 100% | 0.8 pw/cw | Bursty — 14-month gap then 3 features in 6 weeks. Currently engaged with massive Arrow PR (#142981, 5384 lines, approved). | Arrow (sole owner), NDJSON (sole owner) |
| **bpintea** | 100% | 1.2 pw/cw | 8 major PRs in 3 weeks (GCS, ORC, Azure, compression codecs). Steady, reliable output. | Storage plugins, format plugins, compression, optimizer, testing infrastructure |
| **quackaplop** | 40% | 0.8 pw/cw | 5 merged PRs + 3 design iterations in 6 weeks. At 40%, output exceeds allocation (AI-assisted). | Architecture, planner, design, JSON_EXTRACT, Connector SPI proposals |

### Team Capacity

| Metric | Value |
|--------|-------|
| **Combined velocity** | **4.8 pw per calendar week** |
| **Remaining work** | **42.8 person-weeks** (43.3w total − 1.0w #281 → 0w + 1.5w for #341/#342) |
| **Raw calendar time** | 42.8 ÷ 4.8 = **8.9 calendar weeks** |
| **With 25% buffer** (review, rework, scope creep, meetings) | **~11.9 calendar weeks** |
| **Available calendar weeks to FF** | **15 weeks** (Mar 11 → Jun 23) |
| **Margin** | **~3 weeks** of slack before FF |

### In-Flight Work (landing this week)

| PR | Author | What | Closes |
|----|--------|------|--------|
| #142981 | swallez | Arrow-native Block & Vector (5384 lines, approved) | #254, #281 |
| #143928 | costin | FormatReadContext consolidation | — |
| #143940 | costin | Push stats to external source via metadata | — |
| #143969 | swallez | NDJSON cleanup and bug fixes (aggregates all MVP type fixes) | Part of #335 |
| #143970 | bpintea | EXTERNAL commands in CsvTests suite | Testing infra |

---

## Domain Ownership & Assignment

Based on prior involvement and expertise:

| Person | Owns | Proposed new assignments |
|--------|------|--------------------------|
| **costin** | CSV type fixes (#334), Parquet type fixes (#337), parallel parsing (#341), Parquet row-group (#342) | Metadata cache (#286), Parquet-MR improvements (#285), CRUD API (#287) |
| **swallez** | Arrow blocks (#254), Arrow CB (#281), NDJSON type fixes (#335) | Parquet-MR CB (#282) or Arrow Dataset (#295) based on #284 decision, Schema discovery API (#288) |
| **bpintea** | EXTERNAL syntax (#251), Rally track (#252), ORC type fixes (#332), test data (#300), unsupported errors (#274) | Thread pool isolation (#290), JMH benchmarks (#283), distributed hardening (#291) |
| **quackaplop** | Secret scoping (#273), Parquet strategy (#284), union-by-name (#280) | Secret implementation (#296), byte-based memory safety (#289) |

Rationale:
- **costin** gets CRUD API (#287) because he built the framework that resolves datasource configs — he knows the resolution path best. Gets metadata cache (#286) because it wraps the resolve path he owns.
- **swallez** gets whichever Parquet CB path wins (#282 or #295) because the Arrow CB bridge is already his work, and the Parquet-MR CB shares the same circuit breaker integration pattern.
- **bpintea** gets testing/stability items because he built the storage plugins and knows the I/O paths that need isolation and hardening. His CsvTests PR (#143970) shows he's already building test infrastructure.
- **quackaplop** gets secrets and memory safety because these are architectural/design-heavy items that benefit from the 40% allocation pattern (think deeply, implement when ready).

---

## Decision Gate: Parquet Strategy (#284)

**Must resolve by: March 21** (end of week 2). Blocks 6-8w of downstream work.

| Path | Downstream items | Total effort | Risk |
|------|-----------------|-------------|------|
| **Parquet-MR** | #285 (improvements, 2w) → #282 (CB, 4w) | 6w | parquet-mr has no allocation interception; CB may require forking |
| **Native Arrow** | #295 (Dataset reader, 4w) → uses #254 (blocks, already done) | 4w | JNI loading in ES classloader; 74MB JAR; "early development" Java API |

Benchmarks are 4/5 done. Recommendation needed within 10 days.

---

## Common: March Foundations (all options)

All five options share the same March plan. In-flight PRs land this week; new tracked work starts immediately after.

| Person | Items | Total |
|--------|-------|-------|
| costin | #341 parallel parsing (0.5w), FormatReadContext + stats-via-metadata PRs land | 1.5w |
| swallez | #254 Arrow blocks PR merges (0w), #335 NDJSON type fixes start (1.5w) | 2.0w |
| bpintea | #251 EXTERNAL syntax (0.5w), #274 unsupported errors (0.5w), #300 test data (0.5w), #283 JMH start (0.5w) | 2.0w |
| quackaplop | #284 Parquet strategy decision (1.5w), #273 secret scoping (0.5w) | 2.0w |
| **March total** | | **7.5w** |

**March narrative**: EXTERNAL works in production builds. match/knn produce error messages instead of stack traces. Test datasets created. JMH benchmarks started. Parquet strategy decided (unblocks 6-8w downstream). Secret approach scoped. Arrow blocks merged. Parallel text parsing wired.

**Capacity**: 4.8 × 3 = 14.4w available, 7.5w planned. The remaining ~7w is absorbed by in-flight PRs landing, code review, and ramp-up.

---

## Option 1: "Speed to Market" — TP April 18, Functionality First

**Philosophy**: Ship TP as early as possible. Accept rough edges. Get user feedback sooner. The TP tells a complete product story: named datasources, credentials, caching — even if some types still crash.

**TP date**: April 18 (5.4 weeks from today)
**Feedback window**: 9.4 weeks (TP → FF)

### April 1–18 (2.7 weeks, capacity 13.0w)

| Person | Items | Total |
|--------|-------|-------|
| costin | #287 CRUD API (2.0w), #286 metadata cache (3.0w) | 5.0w |
| swallez | #335 NDJSON fixes complete (1.5w), #285 or #295 Parquet path start (1.0w) | 2.5w |
| bpintea | #252 Rally track start (1.0w), #332 ORC types (1.5w) | 2.5w |
| quackaplop | #296 secret implementation (2.5w) | 2.5w |
| **April (pre-TP) total** | | **12.5w** |

**Capacity shortfall**: 12.5w planned vs 13.0w available — just barely fits if nothing slips.

### TP Feature Set
- EXTERNAL syntax in production
- Named datasources via CRUD API
- Secure credential storage (secrets system index)
- Metadata caching (dashboard perf)
- Distributed execution across data nodes
- NDJSON + ORC type support fixed
- Helpful errors for Lucene-dependent commands

### TP Gaps
- CSV type crashes (headers, boolean, datetime)
- Parquet type issues (DECIMAL, timestamps, INT96)
- Thread pool isolation (external I/O can starve ES)
- Byte-based memory safety (no CB on source buffers)
- Union-by-name, schema discovery API
- Rally track incomplete, JMH incomplete
- Distributed hardening not started

### April 18 → GA (9.4 weeks)

| Person | April 18–30 | May | June (→ FF Jun 23) |
|--------|------------|-----|---------------------|
| costin | #286 cache complete | #334 CSV types (1.8w), #337 Parquet types (4.0w) | #342 row-group (1.0w), bug fixes |
| swallez | Parquet path (2.0w) | Parquet complete, #288 schema discovery (1.0w) | Bug fixes, TP feedback |
| bpintea | #290 thread pool (1.0w), Rally continue | #291 hardening (2.0w), #283 JMH complete | Bug fixes, Rally tuning |
| quackaplop | #289 memory safety (2.0w) | #280 union-by-name (3.0w) | Bug fixes, TP feedback |

### Trade-offs

| Pro | Con |
|-----|-----|
| **9.4 weeks of user feedback** — longest window of any option | April is very tight — zero margin for slip |
| Best demo story: "register, configure, query" end-to-end | CSV headers crash, Parquet DECIMAL wrong at TP |
| Earliest external validation of product direction | No thread pool isolation — external I/O can starve search |
| costin's fastest path (he built the framework) | No memory safety — large files can OOM |
| Metadata cache means dashboards are fast | Rally/JMH incomplete — perf story is unproven at TP |

**Biggest risk**: Zero buffer in April. If costin's CRUD or cache takes 1 extra week, TP slips or ships incomplete.

---

## Option 2: "Data Trust" — TP April 30, Correctness First

**Philosophy**: Data correctness is the TP headline. All four formats produce correct results for all supported types. Users trust the output. CRUD and secrets come from quackaplop (40% allocation — schedule risk accepted).

**TP date**: April 30 (7.1 weeks from today)
**Feedback window**: 7.7 weeks (TP → FF)

### April (4.3 weeks, capacity 20.6w)

| Person | Items | Total |
|--------|-------|-------|
| costin | #334 CSV types (1.8w), #337 Parquet types (4.0w), #342 Parquet row-group (1.0w) | 6.8w |
| swallez | #335 NDJSON fixes complete (1.5w), #285 or #295 Parquet path start (2.0w) | 3.5w |
| bpintea | #252 Rally track (2.0w), #283 JMH complete (0.5w), #332 ORC types (1.5w) | 4.0w |
| quackaplop | #287 CRUD API (2.0w), #296 secret implementation start (1.5w) | 3.5w |
| **April total** | | **17.8w** |

**Buffer**: 20.6 − 17.8 = 2.8w

### TP Feature Set
- EXTERNAL syntax in production
- **All four formats type-correct** (CSV, NDJSON, ORC, Parquet)
- CRUD API for named datasources
- Parquet row-group parallelism
- Distributed execution across data nodes
- Rally track running, JMH benchmarks complete
- Helpful errors for Lucene-dependent commands

### TP Gaps
- Secret implementation incomplete (quackaplop at 40%)
- No metadata cache (repeated queries re-infer schema)
- No thread pool isolation
- No byte-based memory safety
- Union-by-name, schema discovery API
- Distributed hardening not started

### May–June → GA

| Person | May | June (→ FF Jun 23) |
|--------|-----|---------------------|
| costin | #286 metadata cache (3.0w), #341 if remaining | Bug fixes, cache polish |
| swallez | Parquet path complete (1.0w), #288 schema discovery (1.0w) | Bug fixes, TP feedback |
| bpintea | #290 thread pool (1.0w), #291 hardening (2.0w) | Bug fixes, Rally tuning |
| quackaplop | #296 secrets complete (1.0w), #280 union-by-name (2.0w) | #289 memory safety (2.0w), #280 complete |

### Trade-offs

| Pro | Con |
|-----|-----|
| **Nothing crashes at TP** — all types work across all formats | No secrets at TP — credentials in config, not encrypted |
| costin fixes his own readers (fastest path to correctness) | Metadata cache deferred — dashboards are slow |
| 2.8w buffer in April — room to absorb surprises | CRUD API on quackaplop at 40% — schedule risk |
| Parquet row-group parallelism at TP | No thread pool or memory safety at TP |
| Rally + JMH running — perf is measured early | Memory safety (#289) pushed to June — late for GA |

**Biggest risk**: quackaplop at 40% building CRUD + starting secrets. If either slips, TP has no named datasource management.

---

## Option 3: "Complete Product" — TP April 30, Functionality First

**Philosophy**: The TP tells a complete product story: named datasources with credentials, metadata caching for dashboard performance, memory safety, and thread pool isolation. Two of four formats have full type support; the other two work for common types. This is the recommended option.

**TP date**: April 30 (7.1 weeks from today)
**Feedback window**: 7.7 weeks (TP → FF)

### April (4.3 weeks, capacity 20.6w)

| Person | Items | Total |
|--------|-------|-------|
| costin | #287 CRUD API (2.0w), #286 metadata cache (3.0w) | 5.0w |
| swallez | #335 NDJSON fixes complete (1.5w), #285 or #295 Parquet path start (2.0w) | 3.5w |
| bpintea | #252 Rally track (2.0w), #290 thread pool isolation (1.0w), #332 ORC types (1.5w) | 4.5w |
| quackaplop | #296 secret implementation (2.5w), #289 byte-based memory safety (1.5w) | 4.0w |
| **April total** | | **17.0w** |

**Buffer**: 20.6 − 17.0 = 3.6w

### TP Feature Set
- EXTERNAL syntax in production
- Named datasources via CRUD API
- Secure credential storage (secrets system index)
- Metadata caching (dashboard perf)
- Thread pool isolation (external I/O can't starve ES)
- Byte-based memory safety (no OOM from large files)
- NDJSON + ORC type support fixed
- Distributed execution across data nodes
- Rally track running
- Helpful errors for Lucene-dependent commands

### TP Gaps
- CSV type crashes (headers, boolean, datetime)
- Parquet type issues (DECIMAL, timestamps, INT96)
- Union-by-name, schema discovery API
- JMH benchmarks started but not complete
- Distributed hardening not started
- Parquet reader improvements in progress

### May–June → GA

| Person | May | June (→ FF Jun 23) |
|--------|-----|---------------------|
| costin | #334 CSV types (1.8w), #337 Parquet types (4.0w) | #342 row-group (1.0w), bug fixes |
| swallez | Parquet path complete (1.0w), #288 schema discovery (1.0w) | Bug fixes, TP feedback |
| bpintea | #291 distributed hardening (2.0w), Rally tuning (1.0w) | Bug fixes, Rally regression |
| quackaplop | #280 union-by-name (3.0w) | Bug fixes, TP feedback |

### Trade-offs

| Pro | Con |
|-----|-----|
| **Best demo**: "register, configure, query" end-to-end | CSV headers crash, Parquet DECIMAL wrong at TP |
| Metadata cache means dashboards are fast | Type fixes compressed into May |
| Thread pool + memory safety at TP — production-ready isolation | costin context-switches from functionality (Apr) to correctness (May) |
| 3.6w buffer — most comfortable of all April-30 options | No union-by-name until May |
| costin builds CRUD + cache (he knows both paths best) | JMH benchmarks not complete at TP |

**Biggest risk**: CSV and Parquet type gaps at TP. Mitigated by: (1) NDJSON + ORC are clean, (2) common Parquet types work, (3) type fixes are costin's May focus.

---

## Option 4: "Performance Flagship" — TP April 30, Performance First

**Philosophy**: The TP's headline is competitive performance with measured benchmarks. Metadata cache, Parquet reader improvements, Rally track, JMH microbenchmarks, and memory safety all land before TP. CRUD and secrets are secondary — users inline URIs at TP, named datasources come in May.

**TP date**: April 30 (7.1 weeks from today)
**Feedback window**: 7.7 weeks (TP → FF)

### April (4.3 weeks, capacity 20.6w)

| Person | Items | Total |
|--------|-------|-------|
| costin | #286 metadata cache (3.0w), #342 Parquet row-group (1.0w), #334 CSV types start (1.8w) | 5.8w |
| swallez | #285 or #295 Parquet path (3.0w), #335 NDJSON fixes complete (1.0w) | 4.0w |
| bpintea | #252 Rally track (2.0w), #283 JMH complete (0.5w), #332 ORC types (1.5w) | 4.0w |
| quackaplop | #289 byte-based memory safety (2.0w), #287 CRUD API start (1.5w) | 3.5w |
| **April total** | | **17.3w** |

**Buffer**: 20.6 − 17.3 = 3.3w

### TP Feature Set
- EXTERNAL syntax in production
- **Metadata caching** (dashboard perf — zero re-inference overhead)
- **Parquet row-group parallelism** (sub-file parallelism)
- **Parquet reader improvements** (significant progress on chosen path)
- **Rally track running** with results (regression detection live)
- **JMH microbenchmarks complete** (per-format throughput measured)
- **Byte-based memory safety** (circuit breaker on source buffers)
- NDJSON + ORC + CSV type support fixed
- Distributed execution across data nodes
- CRUD API started (partial — basic create/get may work)

### TP Gaps
- CRUD API incomplete — named datasources partially working
- No secrets — credentials in config, not encrypted
- Parquet type issues (DECIMAL, timestamps, INT96)
- No thread pool isolation
- Union-by-name, schema discovery API
- Distributed hardening not started

### May–June → GA

| Person | May | June (→ FF Jun 23) |
|--------|-----|---------------------|
| costin | #337 Parquet types (4.0w) | Bug fixes, type completion |
| swallez | Parquet path complete (1.0w), #288 schema discovery (1.0w) | Bug fixes, TP feedback |
| bpintea | #290 thread pool (1.0w), #291 hardening (2.0w), Rally tuning | Bug fixes |
| quackaplop | #287 CRUD complete (0.5w), #296 secrets (2.5w), #280 union-by-name start (1.0w) | #280 complete (2.0w), TP feedback |

### Trade-offs

| Pro | Con |
|-----|-----|
| **Competitive benchmarks at TP** — Rally results, JMH numbers | No secrets at TP — credentials in plaintext |
| Metadata cache + Parquet improvements + row-group = fast queries | Named datasources only partially working |
| Memory safety at TP — no OOM from large files | Weakest demo story: "it's fast but configure inline" |
| Data-driven tuning starts earliest of all options | Thread pool isolation deferred |
| CSV types fixed at TP (costin starts in April) | Parquet DECIMAL, timestamps still wrong |

**Biggest risk**: CRUD + secrets + union-by-name all on quackaplop at 40% in May/June. That's 6.0w of work in 7.7 calendar weeks at 0.8 pw/cw = 6.2w capacity. Tight but feasible.

---

## Option 5: "Near-GA Quality" — TP May 15, Full Breadth

**Philosophy**: The TP should be nearly feature-complete and high quality. Minimize gaps. Union-by-name works. Schema discovery enables Kibana. All types are fixed. The trade-off is less time for user feedback (5.5 weeks to FF).

**TP date**: May 15 (9.3 weeks from today)
**Feedback window**: 5.5 weeks (TP → FF)

### April (4.3 weeks, capacity 20.6w)

| Person | Items | Total |
|--------|-------|-------|
| costin | #287 CRUD API (2.0w), #286 metadata cache (3.0w), #334 CSV types (1.8w) | 6.8w |
| swallez | #335 NDJSON fixes complete (1.5w), #285 or #295 Parquet path (3.0w) | 4.5w |
| bpintea | #252 Rally track (2.0w), #290 thread pool (1.0w), #332 ORC types (1.5w), #283 JMH complete (0.5w) | 5.0w |
| quackaplop | #296 secret implementation (2.5w), #289 byte-based memory safety (1.5w) | 4.0w |
| **April total** | | **20.3w** |

**Buffer**: 20.6 − 20.3 = 0.3w (very tight for April, but May absorbs any slippage)

### May 1–15 (2.1 weeks, capacity 10.1w)

| Person | Items | Total |
|--------|-------|-------|
| costin | #337 Parquet types (3.0w of 4.0w) | 3.0w |
| swallez | Parquet path complete (0.5w), #288 schema discovery (1.0w) | 1.5w |
| bpintea | #291 distributed hardening start (1.0w), Rally tuning (0.5w) | 1.5w |
| quackaplop | #280 union-by-name (2.0w of 3.0w) | 2.0w |
| **May pre-TP total** | | **8.0w** |

**Buffer**: 10.1 − 8.0 = 2.1w

### TP Feature Set
- EXTERNAL syntax in production
- Named datasources via CRUD API
- Secure credential storage (secrets system index)
- Metadata caching (dashboard perf)
- Thread pool isolation
- Byte-based memory safety
- **All four formats type-correct** (CSV, NDJSON, ORC, Parquet mostly done)
- **Union-by-name** (schema-drifted files work)
- **Schema discovery API** (Kibana autocomplete)
- Parquet reader improvements (significant progress)
- Rally track running with results
- JMH benchmarks complete
- Distributed execution across data nodes
- Helpful errors for Lucene-dependent commands

### TP Gaps
- Some Parquet type edge cases (remaining 1.0w of #337)
- Distributed hardening started but not complete
- Parquet row-group parallelism (#342) not done
- Union-by-name at 2/3 completion (core works, edge cases may remain)

### May 15 → GA (5.5 weeks)

| Person | May 15–31 | June (→ FF Jun 23) |
|--------|-----------|---------------------|
| costin | #337 Parquet types complete (1.0w), #342 row-group (1.0w) | Bug fixes, TP feedback |
| swallez | Bug fixes, perf tuning | Bug fixes, TP feedback |
| bpintea | #291 hardening complete (1.0w), Rally tuning | Bug fixes, TP feedback |
| quackaplop | #280 union-by-name complete (1.0w) | Bug fixes, TP feedback |

**Post-TP capacity**: 4.8 × 5.5 = 26.4w, with ~5w of tracked items remaining = 21.4w for TP feedback and stabilization.

### Trade-offs

| Pro | Con |
|-----|-----|
| **Most complete TP** — nearly everything works | Only 5.5 weeks of user feedback before FF |
| Union-by-name handles real-world schema-drifted logs | April is very tight (0.3w buffer) — depends on May as overflow |
| Schema discovery enables Kibana autocomplete | A single 2-week slip puts FF in danger |
| All types fixed — maximum data trust | Less time to react to TP feedback |
| 21.4w of slack post-TP for feedback and stabilization | costin at 6.8w in April — maximum sustained output required |

**Biggest risk**: 5.5-week post-TP window. If users find a significant architectural issue (not just bugs), there may not be enough time to fix it before FF. The TP is high quality but the safety net is thin.

---

## Comparison Matrix

| Dimension | Option 1 | Option 2 | Option 3 | Option 4 | Option 5 |
|-----------|----------|----------|----------|----------|----------|
| | Speed to Market | Data Trust | Complete Product | Performance Flagship | Near-GA Quality |
| **TP date** | Apr 18 | Apr 30 | Apr 30 | Apr 30 | May 15 |
| **Feedback weeks** | 9.4 | 7.7 | 7.7 | 7.7 | 5.5 |
| **April buffer** | ~0.5w | 2.8w | 3.6w | 3.3w | 0.3w (+May) |
| **CRUD at TP** | Yes | Yes | Yes | Partial | Yes |
| **Secrets at TP** | Yes | No | Yes | No | Yes |
| **Cache at TP** | Yes | No | Yes | Yes | Yes |
| **All types fixed** | No | **Yes** | No (2 of 4) | No (3 of 4) | Mostly |
| **Thread pool at TP** | No | No | Yes | No | Yes |
| **Memory safety** | No | No | Yes | Yes | Yes |
| **Rally at TP** | Partial | Yes | Yes | **Yes + tuned** | Yes |
| **Union-by-name** | No | No | No | No | **Yes** |
| **Schema discovery** | No | No | No | No | **Yes** |
| **Demo quality** | Good | Good | **Best** | Weak | Very good |
| **Data trust** | Low | **Highest** | Medium | Medium-High | High |
| **Perf credibility** | Low | Medium | Medium | **Highest** | High |
| **Risk to GA** | Low | Low-Medium | **Low** | Medium | Medium-High |
| **May–June pressure** | High (types) | High (cache+safety) | Medium (types) | High (CRUD+secrets) | Low |

---

## Recommendation

**Option 3 (Complete Product, TP April 30)** is the best trade-off:

1. **Best demo story**: "Register a named datasource, store credentials, and query" — a complete product experience. Type gaps in CSV/Parquet are rough edges; missing CRUD + secrets is a missing product.
2. **3.6w buffer in April** — most comfortable schedule of the three April-30 options. Room for the unexpected.
3. **Thread pool isolation + memory safety at TP** — production-ready isolation. A TP where external I/O starves search, or large files OOM, damages trust.
4. **7.7 weeks of feedback** — enough time to find and fix integration issues before FF.
5. **costin on CRUD + cache** — he built the framework. This is his fastest path. Type fixes in May give him focused time on reader code without context-switching.
6. **2 of 4 formats clean at TP** (NDJSON + ORC). CSV and Parquet have gaps but don't crash on the most common types.

**When to choose a different option:**
- **Option 1** if the team needs external feedback urgently (e.g., validating product direction with design partners who need 10+ weeks of access)
- **Option 2** if the TP will be tested against diverse real-world data where type correctness matters more than the management UX
- **Option 4** if the TP audience cares most about competitive benchmarks (e.g., a comparison against DuckDB/Trino/Spark)
- **Option 5** if there is low confidence in GA readiness and you want the TP to be a near-final validation, not an early preview

---

## Emerging Work Signals

Based on issue comments and scope risk analysis:

| Signal | Source | Impact |
|--------|--------|--------|
| **Pricing/metering** (#292) | Comment from quackaplop: "If metering is needed, instrumentation touches the full execution path" | Could add 3-4w if metering required. Needs early resolution. |
| **NDJSON fixes are coupled** | swallez aggregating all 6 into one PR (#143969) | If the PR is too large, review becomes a bottleneck |
| **Parquet-MR CB has no clean path** | #282 body: "no allocation interception points" | Investigation may conclude CB is impossible without forking. Forces Arrow path. |
| **Parquet DECIMAL is 4 encodings** | #337 analysis | Single "DECIMAL fix" is actually 4 separate code paths. 4.0w estimate may be light. |
| **FormatReadContext refactor landing** | PR #143928 by costin | Changes the FormatReader API surface — all format reader work should sequence after this |

**Recommended buffer items** (create issues if they materialize):
- Bug fix buffer for TP feedback: 2-3w in June
- NDJSON PR too large → needs splitting: 0.5w overhead
- Parquet DECIMAL harder than expected: 1.0w additional
- Metering instrumentation (if required): 3-4w (would push items to post-GA)
