# ES|QL Command & Function Compatibility with External Data Sources

This document categorizes every ES|QL command by its compatibility with external data sources (`ExternalRelation` / `ExternalSourceExec`) on the current main branch.

**Key insight:** The compute engine is source-agnostic — all operators work on generic `Page`/`Block` streams. Compatibility issues arise in two places: (1) the **verifier**, which may require `EsRelation` in the plan tree, and (2) **optimizer pushdowns**, where ES-specific rules skip external sources, causing correct but slower execution.

---

## Fully Supported

These commands work on external data. The compute operators are generic — same code path as ES.

| Command | Performance vs ES | Notes |
|---------|------------------|-------|
| **WHERE** | **Slower without pushdown.** ES pushes filters into Lucene (sub-millisecond index lookups). External sources without `FilterPushdownSupport` do a full scan + post-filter. With pushdown (e.g., Parquet row group stats, Iceberg manifest pruning), gap narrows significantly. | Filter operator itself is identical. |
| **EVAL** | **Identical.** Same `@Evaluator` code runs on same block format. | All scalar functions — no overhead. |
| **STATS...BY** | **Identical compute, more I/O.** Aggregation operators are the same. But ES can sometimes resolve `COUNT(*)` from index metadata without reading data (`PushStatsToSource`); external sources always do a full scan. | Aggregate functions themselves: identical. |
| **SORT** | **Slower.** ES pushes SORT+LIMIT into Lucene as a priority queue on indexed fields. External sources read all data, then sort at coordinator. | For `SORT + LIMIT` specifically, this can be orders of magnitude slower on large datasets. |
| **LIMIT** | **Much slower.** ES pushes LIMIT to Lucene (stops after N hits). External sources read the entire dataset, then truncate. `LIMIT 10` on a 100GB Parquet dataset reads all 100GB. | Critical gap — addressed by milestone item 3. |
| **DROP / KEEP / RENAME** | **Identical.** Column projection at plan level. ORC uses native column projection (skips unneeded columns at I/O level). NDJSON skips unprojected fields during JSON parsing. CSV reads full rows (unavoidable for row format) but only builds blocks for projected columns. | **Parquet bug:** reads all columns despite supporting native projection — `projectedColumns` not wired into `ParquetFileReader`. ORC is correct. NDJSON/CSV are as good as row-oriented formats allow. |
| **DISSECT / GROK** | **Identical.** Same pattern extraction on text blocks. | No source dependency. |
| **MV_EXPAND** | **Identical.** Same expansion operator. | No source dependency. |
| **ROW** | **Identical.** Literal data, no I/O. | N/A |
| **SHOW / META** | **Identical.** Coordinator-only metadata. | N/A |
| **INLINESTATS** | **Identical.** Hash join on coordinator. | No source dependency in the join itself. |
| **SAMPLE** | **Slower.** ES pushes random sampling into Lucene. External sources read all data, then sample at coordinator. | Impact depends on dataset size. |
| **CHANGE_POINT** | **Identical compute.** Surrogate plan wraps input in OrderBy → Limit(1001) → ChangePoint → Limit(1000). All operators are generic. | Requires sortable key + numeric value. Max 1000 data points. Source-agnostic. |
| **COMPLETION** | **Identical.** Inference call goes to a configured inference endpoint, not to the data source. External data is sent to the inference service for processing. | Coordinator-only execution. Requires inference endpoint configured on the cluster. |

### Scalar Functions — Performance

**All 200+ scalar functions** (string, math, date, IP, geo, conditional, type conversion, spatial) run at **identical performance** on external data. They operate on blocks via `@Evaluator`-generated code with zero source dependency. The evaluator doesn't know or care where the data came from.

Functions with notable performance context:
- **CIDR_MATCH, IP_PREFIX** — same speed, but particularly valuable on external log data (common use case)
- **DATE_PARSE, DATE_FORMAT, DATE_TRUNC** — identical; these are the workhorse functions for log analytics
- **TO_* type conversions** — identical; critical for NDJSON where all values start as keyword/text
- **Spatial functions** (ST_DISTANCE, etc.) — identical compute, but ES can push spatial predicates into Lucene; external sources cannot

### Aggregate Functions — Performance

All aggregate functions (COUNT, SUM, MIN, MAX, AVG, PERCENTILE, MEDIAN_ABSOLUTE_DEVIATION, VALUES, TOP, etc.) run at **identical operator speed**. The aggregation happens on blocks in the compute engine — same code regardless of source.

The difference is **I/O volume**: ES can sometimes avoid reading data entirely (metadata-based COUNT, doc_values for sorted MIN/MAX), while external sources always require a full scan to aggregate. For large datasets this means external aggregations are I/O-bound rather than compute-bound.

---

## Supported with Limitations

These commands work but with reduced performance or restricted scope.

### Optimizer Pushdown Gaps

The following commands work correctly but **miss ES-specific optimizations** because the physical optimizer rules match only `EsQueryExec`. The performance column quantifies the gap.

| Command | What's Lost | Perf Impact | Where It Breaks |
|---------|-------------|-------------|-----------------|
| **LIMIT** | No early termination at source. Full dataset read for `LIMIT 10`. | **100-10000x slower** on large datasets. Dominates exploratory query cost. | `PushLimitToSource`: `instanceof EsQueryExec` |
| **SORT + LIMIT** | No TopN pushdown. Full read → sort → truncate. | **10-1000x slower.** Must materialize and sort entire dataset. | `PushTopNToSource`: `instanceof EsQueryExec` |
| **STATS COUNT(*)** | No count-from-metadata. Full scan required. | **10-100x slower** for simple counts. ES resolves from index stats in <1ms. | `PushStatsToSource`: `instanceof EsQueryExec` |
| **SAMPLE** | No probabilistic sampling at source. | **Proportional to dataset size.** Minor for small datasets. | `PushSampleToSource`: `instanceof EsQueryExec` |
| **WHERE** | Sources without `FilterPushdownSupport` get no L2 pushdown. Note: `PushFiltersToSource` has an explicit `ExternalSourceExec` path (line 54) — external sources DO get filter pushdown if they register a `FilterPushdownSupport` implementation. Sources that don't register one get no pushdown. | **Varies.** For selective filters on Parquet (with row group stats), 10-100x gap. For NDJSON, no pushdown possible regardless. | `PushFiltersToSource` line 50: `instanceof EsQueryExec` (Lucene path), line 54: `instanceof ExternalSourceExec` (SPI path via `FilterPushdownRegistry`) |

**Impact:** LIMIT and SORT+LIMIT are the critical gaps — they affect the most common workflow (exploratory queries). Addressed by milestone items 3 (LIMIT pushdown) and 11 (Parquet filter pushdown).

### Cross-Source Commands

| Command | Limitation | User-Facing Error | Perf Impact |
|---------|-----------|-------------------|-------------|
| **ENRICH** | Left side can be external data. Right side is **always an ES enrich policy** — cannot enrich from external sources. | If external source name used as policy: `cannot find enrich policy [name]` (400 Bad Request). Error is factual but doesn't explain that only ES enrich policies are supported. | **Identical** for the join itself when left=external, right=ES policy. |
| **LOOKUP JOIN** | Left side can be external data. Right side must be an **ES lookup index** (`index_mode: lookup`) or a configuration table. Cannot join external with external. | If external source used as right side: `Unknown table [name]` (400 Bad Request). Error doesn't explain that only ES lookup indices are supported. | **Identical** join performance when left=external, right=ES lookup index. |
| **FORK** | Works, but LIMIT/SORT pushdown into fork branches is skipped for non-ES sources. | **Silent.** No error, no warning. Query succeeds but each branch does a full scan. | **Slower per-branch** — multiplied by branch count. |

### Optimizer Pushdown Behavior

All pushdown gaps (LIMIT, SORT+LIMIT, STATS COUNT, SAMPLE, WHERE without `FilterPushdownSupport`) are **completely silent**. No error, no warning, no metadata in the query response indicating that optimization was skipped. Users can only detect the gap through performance observation. The ESQL query response includes `documentsFound` and `valuesLoaded` counts and optional `profile` data, but **no field reports whether pushdown was applied or skipped**.

---

## Not Supported

These commands fail with external data sources. The failure is either a verifier error (caught at planning time) or a missing evaluator (fails at execution time).

### Full-Text Functions

| Function | Failure Mode | User-Facing Error | HTTP Status |
|----------|-------------|-------------------|-------------|
| **KQL** | **Planning error (verifier).** The verifier walks the plan tree and requires reaching `EsRelation`. `ExternalRelation` is not in the allowed set (`Filter`, `OrderBy`, `EsRelation`). Caught before execution. | `Found 1 problem`<br>`line X:Y: [KQL] function cannot be used after EXTERNAL` | **400 Bad Request** (`VerificationException`) |
| **QSTR** (QueryString) | **Planning error (verifier).** Same check as KQL. | `Found 1 problem`<br>`line X:Y: [QSTR] function cannot be used after EXTERNAL` | **400 Bad Request** (`VerificationException`) |
| **MATCH** | **Runtime crash (bug).** MATCH passes the verifier (broader check at lines 270-280 doesn't require `EsRelation`). At execution, `FilterExec` calls `FullTextFunction.toEvaluator()` which returns `LuceneQueryExpressionEvaluator.Factory`. This evaluator searches the Page for a `DocBlock` (Lucene shard context) — external sources don't produce DocBlocks. Result: **NullPointerException**. | `{"error":{"type":"null_pointer_exception","reason":"null"},"status":500}` | **500 Internal Server Error** — **this is a bug, not a sane error.** Should be caught at verification time like KQL/QSTR. |
| **SCORE** | **Planning error (verifier).** Explicit check blocks SCORE in WHERE and LOOKUP JOIN ON conditions on ALL sources (including ES). In ORDER BY, SCORE requires Lucene relevance scores which external sources cannot produce. | `Found 1 problem`<br>`line X:Y: [SCORE] function can't be used in WHERE or LOOKUP JOIN ON conditions` | **400 Bad Request** (`VerificationException`) |

**Root cause:** Full-text search is architecturally coupled to Lucene. These functions produce `QueryBuilder` objects that are translated to Lucene queries — there is no expression-level evaluator that could run on external data blocks.

**Bug to fix:** MATCH on external sources produces a 500 NullPointerException instead of a clear 400 verification error. The fix is to add ExternalRelation to the verifier check for MATCH (same check as KQL/QSTR), or add a dedicated check that MATCH requires an EsRelation in the plan tree.

### Time-Series Commands

| Command | Failure Mode | User-Facing Error | HTTP Status |
|---------|-------------|-------------------|-------------|
| **TS_INFO** | **Planning error (verifier).** Explicit check requires `EsRelation` with `IndexMode.TIME_SERIES` in the plan subtree. | `Found 1 problem`<br>`line X:Y: TS_INFO can only be used with TS source command` | **400 Bad Request** (`VerificationException`) |

---

## Summary

| Category | Count | Commands |
|----------|-------|----------|
| **Fully supported** | 13 commands + all scalar functions | WHERE, EVAL, STATS, SORT, LIMIT, DROP/KEEP/RENAME, DISSECT/GROK, MV_EXPAND, ROW, SHOW/META, INLINESTATS, CHANGE_POINT, COMPLETION |
| **Supported, perf limitations** | 5 commands | LIMIT (no pushdown), SORT+LIMIT (no pushdown), STATS COUNT (no pushdown), SAMPLE (no pushdown), WHERE (partial pushdown) |
| **Supported, scope limitations** | 3 commands | ENRICH (right=ES only), LOOKUP JOIN (right=ES only), FORK (no pushdown into branches) |
| **Not supported** | 5 functions/commands | MATCH, KQL, QSTR, SCORE, TS_INFO |

**Bottom line:** ~95% of ES|QL commands and all 200+ scalar functions work on external data today. The gaps are full-text search (Lucene-coupled: MATCH, KQL, QSTR, SCORE) and TS_INFO (requires ES time-series index mode). The performance limitations from missing pushdowns are addressed in the milestone plan. The cross-source limitations (ENRICH/LOOKUP JOIN right side = ES only) are the key architectural barrier to full federation.
