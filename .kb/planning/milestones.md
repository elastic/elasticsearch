# ES|QL External Data Sources — Milestones

**Date**: 2026-03-11
**GA Target**: ES 9.5, Feature Freeze June 23, 2026
**Tracking**: elastic/esql-planning#189

---

## Team

| Person | Allocation | Velocity (pw/cw) |
|--------|-----------|-----------------|
| costin | 100% | 2.0 |
| swallez | 100% | 0.8 |
| quackaplop | 40% | 0.8 |
| bpintea | 100% | 1.2 |
| **Combined** | | **4.8** |

---

## Swim Lanes

| Lane | Owner | Scope |
|------|-------|-------|
| **Infrastructure** | costin | Framework, distributed execution, thread pool, CSV, Parquet types (hard), Parquet-MR improvements, hardening |
| **Formats** | swallez | NDJSON correctness, Parquet types (remaining), Arrow blocks |
| **Schema & Secrets** | quackaplop | Secrets, CRUD, unsupported errors, union-by-name |
| **Scale, Performance & Infra Ramp-Up** | bpintea | EXTERNAL syntax, ORC, Rally, JMH, memory safety, test data. **Infra ramp-up**: working on memory safety (#289), Rally (full execution path), and ORC (FormatReader SPI) builds framework knowledge — reduces bus factor on costin's infrastructure. |

Lanes stay consistent across TP and post-TP. costin pulls hard/long items from other lanes when capacity allows.

---

## Phase 1: Foundations (March 11 → March 31)

*3 weeks. Product works in production builds. Doesn't crash on basic queries.*

| Infrastructure (costin) | Formats (swallez) | Schema & Secrets (quackaplop) | Scale, Perf & Ramp-Up (bpintea) |
|---|---|---|---|
| Parallel text parsing (#341, 0.5w) | Arrow blocks + CB land (#254/#281) | Secret scoping (#273, 0.5w) | EXTERNAL syntax ungated (#251, 0.5w) |
| Framework PRs land (FormatReadContext, stats-via-metadata) | NDJSON type fixes start (#335) | Secret implementation start (#296) | Unsupported errors (#274, 0.5w) |
| CSV types start (#334) | | | Test datasets (#300, 0.5w) |
| | | | JMH start (#283) |

---

## Phase 2: Feature Complete (April 1 → ~April 28)

*~4 weeks. All pre-TP features land.*

| Infrastructure (costin) | Formats (swallez) | Schema & Secrets (quackaplop) | Scale, Perf & Ramp-Up (bpintea) |
|---|---|---|---|
| CSV types complete (#334, 1.8w) | NDJSON types complete (#335, 3.0w) | Secrets complete (#296, 2.5w) | ORC types (#332, 1.5w) |
| Parquet types — DECIMAL (4 encodings), MICROS/NANOS, INT96 (#337, 3.0w) | Parquet types — UUID, FLOAT16, LIST (#337, 1.0w) | CRUD API (#287, 2.0w) | Rally track (#252, 2.0w) |
| Parquet-MR reader improvements (#285, 2.0w) | | | Memory safety (#289, 2.0w) |
| Thread pool isolation (#290, 1.0w) | | | JMH complete (#283, 1.0w) |
| Distributed hardening (#291, 2.0w) | | | |
| + ongoing framework infrastructure | | | |

### Per-lane completion estimates

| Lane | Tracked work | Completion |
|------|-------------|------------|
| Infrastructure (costin) | 10.3w + ~5w infra | ~April 17 (tracked), then infra + early stabilization |
| Formats (swallez) | 4.0w | ~April 14, then stabilization + CB investigation |
| Schema & Secrets (quackaplop) | 5.5w | ~April 28 |
| Scale, Perf & Ramp-Up (bpintea) | 7.5w | ~April 25 |
| **Feature complete** | | **~April 28** |

---

## Phase 3: TP Stabilization (~April 28 → ~May 12)

*2 weeks. Bug fixes, integration testing, scale validation with Rally.*

All lanes: fix bugs found during integration testing, run Rally at scale, harden edge cases. costin and swallez enter stabilization early (~April 14-17) and can start post-TP items or help other lanes.

---

## TP Ships (~May 12)

### Characterization: "Functional + Mostly Performance"

The product works correctly, it's safe to run, and performance is measured.

| Dimension | What's true at TP |
|-----------|-------------------|
| **Functional** | EXTERNAL command in production. Named datasources via CRUD API. Secure credential storage. Helpful errors for unsupported commands. |
| **Correctness** | All four formats type-correct (CSV, NDJSON, ORC, Parquet). |
| **Performance** | Parallel text parsing. Parquet-MR improvements. Rally track running. JMH benchmarks complete. Metadata performance measured. |
| **Stability** | Thread pool isolation. Byte-based memory safety. Distributed execution hardened. |

### Not in TP (deferred to GA)

| Item | Reason |
|------|--------|
| Metadata cache (#286) | Quality of life — queries work without it, just re-infer schema each time |
| Union-by-name (#280) | Quality of life — users can pre-normalize schemas |
| Row-group parallelism (#342) | Performance optimization — Parquet works without it |

---

## Phase 4: Post-TP → GA Feature Complete (~May 12 → ~June 9)

### Characterization: "Quality of Life + Complete Stability + Complete Performance"

The product is polished, handles edge cases, and performs at scale.

| Infrastructure (costin) | Formats (swallez) | Schema & Secrets (quackaplop) | Scale, Perf & Ramp-Up (bpintea) |
|---|---|---|---|
| Metadata cache (#286, 3.0w) | Format polish | Union-by-name (#280, 3.0w) | Rally tuning |
| Row-group splits (#342, 1.0w) | TP feedback fixes | TP feedback fixes | Scale testing findings |
| TP feedback fixes | Parquet-MR CB investigation (#282 — research only) | | Perf regression analysis |

### Per-lane post-TP estimates

| Lane | Work | Capacity (4 weeks) |
|------|------|---------------------|
| Infrastructure (costin) | ~5w tracked + buffer | 8.0w — comfortable |
| Formats (swallez) | ~2w feedback + investigation | 3.2w — comfortable |
| Schema & Secrets (quackaplop) | 3.0w union-by-name + feedback | 3.2w — tight but fits |
| Scale, Perf & Ramp-Up (bpintea) | ~3w findings + tuning | 4.8w — comfortable |

Note: costin takes metadata cache post-TP (he built the resolution framework it wraps). This frees quackaplop to focus entirely on union-by-name.

---

## Phase 5: GA Stabilization (June 9 → June 23 FF)

*2 weeks. Final bug fixes, documentation, release preparation.*

All lanes: stability fixes only. No new features. Rally regression runs. Release candidate validation.

---

## Deferred (Post-GA)

| # | Item | Est | Rationale |
|---|------|-----|-----------|
| #282 | Parquet-MR circuit breaker | 4.0w | #289 covers general case; investigation starts post-TP, implementation post-GA. Arrow path replaces long-term. |
| #288 | Schema discovery API | 1.0w | Kibana autocomplete — not required for GA product. |

---

## Summary Timeline

```
Mar 11          Mar 31     ~Apr 28    ~May 12         ~Jun 9     Jun 23
  |── Phase 1 ──|── Phase 2 ──|── Stab ──|── Phase 4 ──|── Stab ──|
  | Foundations  |  Features   |          |  Post-TP    |          |
  |              |             |  TP ────>|             | GA ────> |
  |              |             |  ships   |             | ships    |
```

| Phase | Window | Character |
|-------|--------|-----------|
| 1. Foundations | Mar 11 → Mar 31 | It works |
| 2. Feature Complete | Apr 1 → ~Apr 28 | It's correct and safe |
| 3. TP Stabilization | ~Apr 28 → ~May 12 | It's tested |
| **TP** | **~May 12** | **Functional + Mostly Performance** |
| 4. Post-TP → GA | ~May 12 → ~Jun 9 | Quality of life + complete perf |
| 5. GA Stabilization | Jun 9 → Jun 23 | Final validation |
| **GA (FF)** | **June 23** | **Complete** |

---

## Budget for Unknowns

| Category | Budget | Source |
|----------|--------|--------|
| Untracked infrastructure (costin) | 5-6w | Framework PRs, API stabilization, config validation |
| Scale testing findings | 4-5w | Rally + JMH will surface issues |
| TP feedback fixes | 3-4w | User-reported issues post-TP |
| **Total buffer** | **~13w** | |
| **Total capacity** | **72.0w** (4.8 × 15 weeks) | |
| **Total tracked** | **~37w** (TP: 28.3w + post-TP: 7.0w + post-GA: 5.0w) | |
| **Utilization** | **~69%** | Healthy margin |
