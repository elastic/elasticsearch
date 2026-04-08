# ES|QL Command & Function Support on External Data Sources

Deep dive: every ES|QL command and function assessed for compatibility with external data sources (`ExternalRelation` / `ExternalSourceExec`). For each broken item: exact failure mode, graceful-error fix, and functional fix.

Commands grouped into delivery groups for incremental shipping.

---

## Executive Summary

| Command / Function | Verdict | Failure Mode | Graceful Fix | Functional Fix |
|---|---|---|---|---|
| **WHERE** | Works | — | — | — |
| **EVAL** | Works | — | — | — |
| **KEEP / DROP / RENAME** | Works | — | — | — |
| **LIMIT** | Works (perf gap) | — | — | Pushdown needed [1][2] |
| **SORT** | Works (perf gap) | — | — | TopN pushdown needed [3][4] |
| **STATS** | Works (perf gap) | — | — | Distributed aggregation [6][7] |
| **INLINESTATS** | Works | — | — | — |
| **DISSECT** | Works | — | — | — |
| **GROK** | Works | — | — | — |
| **MV_EXPAND** | Works | — | — | — |
| **ENRICH** (_any) | Works | — | — | — |
| **ENRICH** (_coordinator) | Works | — | — | — |
| **ENRICH** (_remote) | Blocked | Verifier error [8] | Already graceful | Requires distributed external sources [9] |
| **LOOKUP JOIN** (ext left) | Works | — | — | — |
| **LOOKUP JOIN** (ext right) | Blocked | Grammar + mapper [10][11][12] | Already blocked (grammar) | Grammar + mapper changes [10][11][12] |
| **SAMPLE** | Works (perf gap) | — | — | Sample pushdown possible [13] |
| **CHANGE_POINT** | Works | — | — | — |
| **FORK** | Works | — | — | — |
| **RERANK** | Works | — | — | — |
| **COMPLETION** | Works | — | — | — |
| **URI_PARTS** | Works | — | — | — |
| **REG_DOMAIN** | Works | — | — | — |
| **match()** | **BROKEN** | Runtime crash [14][15][16] | Add verifier check [17] | Requires non-Lucene text search |
| **match_phrase()** | **BROKEN** | Runtime crash [14][15][16] | Add verifier check [17] | Requires non-Lucene text search |
| **multi_match()** | **BROKEN** | Runtime crash [14][15][16] | Add verifier check [17] | Requires non-Lucene text search |
| **knn()** | **BROKEN** | Runtime crash [14][15][16][18] | Add verifier check [17] | Requires vector index |
| **score()** | **BROKEN** | Runtime crash [19][20] | Add verifier check [17] | Requires scoring infrastructure |
| **kql()** | Blocked | Verifier error [17] | Already graceful | Requires Lucene |
| **qstr()** | Blocked | Verifier error [17] | Already graceful | Requires Lucene |
| **LIKE / RLIKE** | Works | — | — | — |
| **contains/starts_with/ends_with** | Works | — | — | — |
| **All scalar functions** | Works | — | — | — |
| **All aggregate functions** | Works | — | — | — |
| **TS** | Blocked | Hard gate [21][22] | Already graceful | Requires time-series metadata |
| **METRICS_INFO** | Blocked | Hard gate [23] | Already graceful | Requires time-series metadata |
| **TS_INFO** | Blocked | Hard gate [24] | Already graceful | Requires time-series metadata |
| **COUNT(*) metadata** | Works (perf gap) | — | — | Pushdown possible [25] |
| **Column projection** | Works (perf gap) | — | — | Pushdown needed [26][27] |

**Bottom line:** 5 functions crash at runtime (match, match_phrase, multi_match, knn, score). Everything else either works or fails gracefully. Performance gaps exist for LIMIT, TopN, column projection, STATS distribution, and SAMPLE pushdown.

---

## Delivery Groups

### Group 1: Verifier Safety Net (P0 — prevents runtime crashes)

**What:** Add `ExternalRelation` check to the full-text function verifier so match/match_phrase/multi_match/knn/score produce clean analysis-time errors instead of runtime crashes.

**Commands:** match, match_phrase, multi_match, knn, score

**Scope:** S (single verifier method change)

**Why P0:** These are the only commands that crash at runtime. Every other incompatibility already has a clean verifier error. A user who writes `EXTERNAL "s3://logs.parquet" | WHERE match(message, "error")` gets an assertion failure or NPE instead of a helpful error message.

**Crash chain:** `FullTextFunction.toEvaluator()` [14] calls `toEvaluator.shardContexts()` which returns `EmptyIndexedByShardId.instance()` for coordinator-only execution. `toShardConfigs()` [15] maps over empty shards, producing empty config. `LuceneQueryEvaluator` constructor [16] asserts `shards.isEmpty() == false`. If assertions off, searches for `DocBlock` in page — external sources produce no DocBlock — NPE.

**Graceful fix:**

Extend the existing verifier check at `FullTextFunction.java:257-267` [17]. Currently only `kql()` and `qstr()` have the `EsRelation` ancestry check. Add the same check for all `FullTextFunction` subclasses when the plan ancestry includes `ExternalRelation`:

```java
// In FullTextFunction verifier — add ExternalRelation check for ALL full-text functions
plan.forEachDown(Filter.class, filter -> {
    filter.condition().forEachDown(FullTextFunction.class, ftf -> {
        if (plan.anyMatch(p -> p instanceof ExternalRelation)) {
            failures.add(fail(ftf, "[{}] function is not supported for external data sources", ftf.functionName()));
        }
    });
});
```

**Functional fix (make actually work):** Not feasible for MVP. Full-text functions are architecturally bound to Lucene — they require an `IndexSearcher`, `DocBlock` in pages, and shard contexts [14][16]. Making them work on external data would require either: (a) building an in-memory Lucene index from external data (expensive, defeats purpose), or (b) a completely different text matching engine. **Recommendation:** Post-MVP at earliest.

---

### Group 2: LIMIT & TopN Pushdown (P0 — critical for usability)

**What:** Push LIMIT and TopN (SORT + LIMIT) values down to `ExternalSourceExec` so external sources can stop reading early.

**Commands:** LIMIT, SORT

**Scope:** M

**Why P0:** Without LIMIT pushdown, `EXTERNAL "s3://10gb.parquet" | LIMIT 10` reads the entire 10GB file and discards all but 10 rows. This makes the most common exploratory query pattern unusably slow.

**Current behavior:**
- `PushLimitToSource` [1] only matches `EsQueryExec` children (line 21: `if (child instanceof EsQueryExec)`)
- `PushTopNToSource` [3] only matches `EsQueryExec` children (line 144: `if (child instanceof EsQueryExec queryExec`)
- External sources get `LimitOperator` / `TopNOperator` in the compute layer [2][4] — functionally correct but reads all data first
- Backpressure through `LimitOperator.needsInput()` [28] provides partial early termination, but the source may have buffered ahead
- Note: every ES|QL query has an implicit LIMIT (default 1000) added by `Analyzer.AddImplicitLimit` [43], so SORT always becomes TopN via `ReplaceLimitAndSortAsTopN` [44]. There is no truly "unbounded" SORT.

**Graceful fix:** Not applicable — LIMIT already works, it's just slow.

**Functional fix:**
1. Add `ExternalSourceExec.withLimit(Expression limit)` analogous to `EsQueryExec.withLimit()` [29]
2. Extend `PushLimitToSource` to match `ExternalSourceExec` children:
   ```java
   } else if (child instanceof ExternalSourceExec externalExec) {
       plan = externalExec.withLimit(limitExec.limit());
   }
   ```
3. The limit value flows through to the operator factory via `ExternalSourceExec.limit()`. Source operators (Parquet, NDJSON) can stop reading after N rows.
4. For TopN: extend `ExternalSourceExec` SPI to accept sort + limit. Parquet sources can use row-group-level min/max statistics for partial sort elimination.

---

### Group 3: Column Projection Pushdown (P1 — performance)

**What:** Push column selection down to `ExternalSourceExec` so external sources only read needed columns.

**Commands:** Affects all queries (implicit — any query that doesn't use all columns)

**Scope:** M

**Why P1:** Columnar formats (Parquet, ORC) can skip entire column chunks when columns aren't needed. Without projection pushdown, `EXTERNAL "s3://wide.parquet" | KEEP name, age` reads all 50+ columns.

**Current behavior:**
- `PruneColumns` [26] logical rule does not handle `ExternalRelation` — it falls to `default -> p` (no-op)
- `InsertFieldExtraction` [27] physical rule only processes `EsQueryExec` children (needs `_doc` attribute)
- `ProjectAwayColumns` only acts on `FragmentExec` — N/A for external sources
- `ProjectExec` downstream does filter columns, but only after they're already read from the source

**Graceful fix:** Not applicable — queries work, just read too much data.

**Functional fix:**
1. Add `ExternalRelation` case to `PruneColumns.java` switch (line 80-92) [26] to prune unused attributes from the logical plan
2. Add new physical optimizer rule `PushProjectionToExternalSource` that:
   - Finds `ProjectExec` above `ExternalSourceExec`
   - Creates `ExternalSourceExec.withProjectedColumns(List<Attribute>)` containing only the referenced columns
   - Source operators receive the pruned column list and skip unreferenced columns during file reads
3. For Parquet: column projection maps directly to `ReadOptions.includeColumns()` — native support

---

### Group 4: Distributed Aggregation for External Sources (P1 — scale)

**What:** Enable two-phase (INITIAL + FINAL) aggregation when external sources are distributed across data nodes.

**Commands:** STATS, STATS ... BY

**Scope:** M

**Why P1:** Currently, when external sources are distributed (splits sent to data nodes), each data node runs a SINGLE-mode aggregate and sends full results back to the coordinator. Two-phase aggregation (INITIAL on data nodes, FINAL on coordinator) would reduce data transfer for aggregations with grouping.

**Current behavior:**
- Non-distributed: SINGLE-mode aggregation on coordinator [6] — correct for coordinator-only
- Distributed: `ExchangeExec` wraps the plan but aggregation mode depends on whether `addExchangeForFragment` fires [7]. Since external sources don't produce `FragmentExec`, the exchange-based split doesn't create INITIAL/FINAL mode automatically
- `CATEGORIZE` grouping forces two-phase even without exchange [30]

**Graceful fix:** Not applicable — aggregation works, just less efficient when distributed.

**Functional fix:**
1. In `Mapper.mapUnary()` for `Aggregate`, detect when the child is `ExternalSourceExec` with a `SplitProvider` that returns multiple splits
2. When distributed: inject `ExchangeExec` and create INITIAL (data node) + FINAL (coordinator) aggregate nodes
3. The distribution infrastructure already exists for file-based sources — this extends it to the aggregation split

---

### Group 5: LOOKUP JOIN with External Right Side (P2 — future)

**What:** Allow external data sources as the right (lookup) side of LOOKUP JOIN.

**Commands:** LOOKUP JOIN

**Scope:** L

**Why P2:** Currently `FROM logs | LOOKUP JOIN external_parquet ON key` is impossible. The grammar hardcodes the right side as an ES index pattern [10], and the mapper checks `instanceof EsRelation` [11][12]. This is a significant feature but not blocking for MVP since the primary use case (external on left, enrich from ES) already works.

**Current behavior:**
- External on left: Works — `ExternalSourceExec` flows as left input to `LookupJoinExec` [31]
- External on right: Grammar blocks it (`visitIndexPattern` [10]), mapper blocks it (`isIndexModeLookup` checks `EsRelation` [11]), LocalMapper blocks it (`instanceof EsRelation` [12])

**Graceful fix:** Already graceful — grammar simply doesn't accept external sources in JOIN target position. No crash possible.

**Functional fix:**
1. Grammar: extend `joinTarget` production to accept external source expressions [10]
2. Mapper: add `ExternalRelation` path in `isIndexModeLookup` and `mapBinary` [11]
3. LocalMapper: add `ExternalRelation` path in `mapBinary` right-side handling [12]
4. Semantics: decide whether external lookup should be hash-join (materialize right side) or nested-loop. Hash-join is simpler — read external source into `LocalRelation`, then `HashJoinExec`

---

### Group 6: Time-Series Commands (P2 — future)

**What:** TS, METRICS_INFO, TS_INFO — currently blocked for external sources.

**Commands:** TS, METRICS_INFO, TS_INFO

**Scope:** L

**Why P2:** These commands are intrinsically tied to Elasticsearch's time-series index mode. They require `_tsid` metadata attributes, `IndexMode.TIME_SERIES`, and ES-specific metric field types [21][22][23][24]. Supporting them for external data would require external sources to declare time-series semantics — a significant design effort with no immediate customer demand.

**Graceful fix:** Already graceful:
- TS: Parser creates `UnresolvedRelation` with `IndexMode.TIME_SERIES` — can never resolve to `ExternalRelation` [21]
- METRICS_INFO/TS_INFO: Hard `instanceof EsRelation && indexMode == TIME_SERIES` verification [23][24]

**Functional fix:** Would require:
1. External sources declaring time-series metadata (tsid, timestamp, metric types)
2. `TranslateTimeSeriesAggregate` generalized beyond `EsRelation` [22]
3. New `ExternalRelation.indexMode()` or equivalent concept

---

### Group 7: Sample & Stats Pushdown (P2 — optimization)

**What:** Push SAMPLE probability and COUNT(*) metadata to external sources.

**Commands:** SAMPLE, COUNT(*)

**Scope:** S

**Why P2:** Both work correctly without pushdown. Optimization benefit exists but is smaller than LIMIT/column projection.

**Current behavior:**
- SAMPLE: `SampleOperator` filters rows randomly in compute layer [13]. `PushSampleToSource` only handles `EsQueryExec`.
- COUNT(*): `PushStatsToSource` [25] creates `EsStatsQueryExec` using Lucene's `docFreq` — inherently Lucene-specific. External sources compute COUNT via standard `CountAggregatorFunction`.

**Functional fix:**
- SAMPLE: extend SPI to accept sample probability; Parquet sources can skip row groups probabilistically
- COUNT(*): for metadata-rich formats (Parquet footer has row count, Iceberg manifests have record counts), push COUNT(*) to metadata read

---

## Detailed Per-Command Analysis

### Commands That Work (Source-Agnostic)

All 16 unary handlers in `MapperUtils.mapUnary()` [32] are source-agnostic. They accept any `PhysicalPlan` child and wrap it with the corresponding physical operator. The full list:

| Command | Mapping | Line in MapperUtils.java | Evidence |
|---|---|---|---|
| WHERE | Filter → FilterExec | 88 | [32] |
| EVAL | Eval → EvalExec | 96 | [32] |
| KEEP | Project → ProjectExec | 92 | [32] |
| DROP | (resolved to Project) | 92 | [32] |
| RENAME | (resolved to Project) | 92 | [33] |
| DISSECT | Dissect → DissectExec | 100 | [32] |
| GROK | Grok → GrokExec | 104 | [32] |
| MV_EXPAND | MvExpand → MvExpandExec | 148 | [32] |
| ENRICH (_any/_coord) | Enrich → EnrichExec | 130 | [32] |
| SAMPLE | Sample → SampleExec | 167 | [32] |
| CHANGE_POINT | ChangePoint → ChangePointExec | 152 | [32] |
| RERANK | Rerank → RerankExec | 108 | [32] |
| COMPLETION | Completion → CompletionExec | 119 | [32] |
| URI_PARTS | UriParts → UriPartsExec | 176 | [32] |
| REG_DOMAIN | RegisteredDomain → RegisteredDomainExec | 186 | [32] |

Additionally from `Mapper.mapUnary()`:

| Command | Notes | Evidence |
|---|---|---|
| STATS | SINGLE-mode on coordinator (correct) | [6][7] |
| LIMIT | LimitExec in compute layer + backpressure | [2][28] |
| SORT | Always becomes TopN (implicit LIMIT [43] + ReplaceLimitAndSortAsTopN [44]). TopNExec in compute layer | [4][43][44] |
| INLINESTATS | Via InlineJoin → HashJoinExec (right side pre-computed as LocalRelation) | [34] |
| LOOKUP JOIN (ext left) | ExternalSourceExec flows as left input to LookupJoinExec | [31] |

### String Matching Functions (Work)

| Function | Evaluator | Evidence |
|---|---|---|
| LIKE | `AutomataMatch` (pure automaton on BytesRef) | [35] |
| RLIKE | `AutomataMatch` (same) | [35] |
| contains() | `ContainsEvaluator` (pure string) | [36] |
| starts_with() | `StartsWithEvaluator` (pure byte comparison) | [37] |
| ends_with() | `EndsWithEvaluator` (pure byte comparison) | [38] |

### Aggregate Functions (All Work)

All aggregate function suppliers are pure compute — none reference `IndexSearcher`, `ShardContext`, or any Lucene API [39]:

count, sum, avg, min, max, values, top, percentile, median_absolute_deviation, st_centroid_agg, st_extent_agg, CATEGORIZE

Spatial aggregations (st_centroid, st_extent) default to `SourceValues` variant for external sources because `SpatialDocValuesExtraction` only matches `EsQueryExec` children [40].

### Full-Text Functions (Broken — Runtime Crash)

**match(), match_phrase(), multi_match():** Inherit `FullTextFunction.toEvaluator()` [14] which calls `toShardConfigs(toEvaluator.shardContexts())` [15]. For external sources, `shardContexts()` returns `EmptyIndexedByShardId.instance()`. `LuceneQueryEvaluator` constructor [16] asserts `shards.isEmpty() == false`. With assertions off, searches for `DocBlock` in page — NPE.

**knn():** Same crash path via inherited `FullTextFunction.toEvaluator()`. Additionally, `evaluatorQueryBuilder()` creates `ExactKnnQueryBuilder` requiring Lucene `IndexSearcher` [18].

**score():** Calls `ScoreMapper.toScorer()` [19] which passes `shardContexts` to child FullTextFunction's `toScorer()` [20]. Same empty-shards crash.

**kql(), qstr():** Already blocked at verifier [17] — `FullTextFunction.java:263` checks `lp instanceof EsRelation` in plan ancestry. `ExternalRelation` doesn't match. Clean error message.

### Blocked Commands (Clean Verifier Errors)

**ENRICH _remote:** `Enrich.java:284` [8] checks `ExecutesOn.Coordinator` in plan descendants. `ExternalRelation` implements `ExecutesOn.Coordinator` [9]. Error: "ENRICH with remote policy can't be executed after [EXTERNAL ...]"

**TS:** Parser creates `UnresolvedRelation` with `SourceCommand.TS` [21]. `TranslateTimeSeriesAggregate` searches for `EsRelation` [22] — never finds `ExternalRelation`. Additionally requires `_tsid` metadata attribute.

**METRICS_INFO:** Hard check `instanceof EsRelation && indexMode == TIME_SERIES` [23].

**TS_INFO:** Same hard check [24].

**LOOKUP JOIN (ext right):** Grammar restricts right side to ES index pattern [10]. Mapper checks `instanceof EsRelation` [11]. LocalMapper checks `instanceof EsRelation` [12].

### Performance Gaps (Work But Suboptimal)

**LIMIT not pushed:** `PushLimitToSource` only matches `EsQueryExec` [1][2]. External sources read all data; `LimitOperator` truncates via backpressure [28].

**TopN not pushed:** SORT always becomes TopN due to implicit LIMIT [43][44]. `PushTopNToSource` only matches `EsQueryExec` [3][4]. External sources sort entirely in memory.

**Column projection not pushed:** `PruneColumns` doesn't handle `ExternalRelation` [26]. `InsertFieldExtraction` only handles `EsQueryExec` [27]. External sources read all columns.

**COUNT(*) not pushed to metadata:** `PushStatsToSource` only matches `EsQueryExec` [25]. External sources compute COUNT row-by-row.

**SAMPLE not pushed:** Pure compute-layer random sampling [13]. No row-group-level skip.

**STATS single-mode:** Coordinator-only execution uses SINGLE aggregation [6][7]. For distributed external sources, two-phase split isn't automatic.

---

## Filter Pushdown (Already Implemented)

External sources have a dedicated filter pushdown path — this is NOT a gap.

`PushFiltersToSource.java:54-56` [41] detects `ExternalSourceExec` children and delegates to `planFilterExecForExternalSource()` [42]. This method:
1. Looks up `FilterPushdownSupport` from `FilterPushdownRegistry` by source type
2. Calls `pushdownSupport.pushFilters(filters)` — delegates to source-specific SPI
3. Creates `ExternalSourceExec.withPushedFilter(combinedFilter)` if pushdown succeeds
4. Wraps remainder in `FilterExec` if not all filters pushed

Three-level filter model: L1 partition pruning → L2 per-node pushdown → L3 engine remainder. Fully operational for file-based sources (Parquet, ORC).

---

## References

[1] `PushLimitToSource.java:21-25` — rule only matches `EsQueryExec` child: `if (child instanceof EsQueryExec queryExec)`

[2] `PushLimitToSource.java:16-28` — full rule implementation, no `ExternalSourceExec` path

[3] `PushTopNToSource.java:144-148` — `evaluatePushable` requires `EsQueryExec`: `if (child instanceof EsQueryExec queryExec && queryExec.canPushSorts()...)`

[4] `PushTopNToSource.java:152-153` — compound case also requires `EsQueryExec`: `if (child instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec)`

[5] `OrderBy.java:139-141` — `postOptimizationPlanVerification()` rejects unbounded SORT for all sources

[6] `Mapper.java:128-131` — when child is not `ExchangeExec`, creates `AggregatorMode.SINGLE` aggregate

[7] `Mapper.java:269-278` — `addExchangeForFragment()` only acts on `FragmentExec` children, no-op for `ExternalSourceExec`

[8] `Enrich.java:284` — `if (u instanceof ExecutesOn ex && ex.executesOn() == ExecuteLocation.COORDINATOR)` blocks _remote mode

[9] `ExternalRelation` implements `ExecutesOn.Coordinator` — all external sources execute on coordinator

[10] `LogicalPlanBuilder.java:829` — `visitIndexPattern(List.of(target.index))` forces right side of LOOKUP JOIN to ES index pattern

[11] `Mapper.java:235-244` — `isIndexModeLookup()` checks `fragment.fragment() instanceof EsRelation relation && relation.indexMode() == IndexMode.LOOKUP`

[12] `LocalMapper.java:143-151` — `binary.right() instanceof EsRelation esRelation` check; throws `EsqlIllegalArgumentException` otherwise

[13] `LocalExecutionPlanner.java:1404-1408` — `planSample()` creates `SampleOperator.Factory(probability)` as pure page-level operator

[14] `FullTextFunction.java:444-445` — `toEvaluator()` calls `toShardConfigs(toEvaluator.shardContexts())`

[15] `FullTextFunction.java:464-466` — `toShardConfigs()` maps over shardContexts to create `ShardConfig`

[16] `LuceneQueryEvaluator.java:58` — `assert shards != null && shards.isEmpty() == false`; line 73: `assert docBlock != null`

[17] `FullTextFunction.java:257-267` — verifier check for kql/qstr requires `EsRelation` in plan ancestry (line 263: `lp instanceof EsRelation`)

[18] `Knn.java:280-288` — `evaluatorQueryBuilder()` creates `ExactKnnQueryBuilder` requiring Lucene IndexSearcher

[19] `Score.java:90-92` — `ScoreMapper.toScorer(children().getFirst(), toEvaluator.shardContexts())`

[20] `FullTextFunction.java:460-461` — `toScorer()` creates `LuceneQueryScoreEvaluator.Factory(toShardConfigs(toScorer.shardContexts()))`

[21] `LogicalPlanBuilder.java:776-778` — `visitTimeSeriesCommand` creates `UnresolvedRelation` with `SourceCommand.TS`

[22] `TranslateTimeSeriesAggregate.java:167-170` — `aggregate.forEachDown(EsRelation.class, r -> ...)` searches for `_tsid` attribute

[23] `MetricsInfo.java:126-129` — `child().anyMatch(p -> p instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES)`

[24] `TsInfo.java:126-129` — identical gate: `instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES`

[25] `PushStatsToSource.java:49` — `if (aggregateExec.child() instanceof EsQueryExec queryExec)` — only matches ES sources

[26] `PruneColumns.java:80-92` — switch statement handles `Aggregate, InlineJoin, Eval, Project, EsRelation, Fork, RegexExtract` — no `ExternalRelation`

[27] `InsertFieldExtraction.java:48-61` — skips `LeafExec`, only processes children exporting `_doc` attribute (specific to `EsQueryExec`)

[28] `LimitOperator.java:68-70` — `needsInput()` returns false when `limiter.remaining() <= 0`, providing backpressure

[29] `EsQueryExec.withLimit()` — the pattern to follow: stores limit on the plan node, passed to Lucene `IndexSearcher.search(query, limit)`

[30] `Mapper.java:128-134` — CATEGORIZE grouping forces INITIAL mode at line 133 even without exchange

[31] `Mapper.java:198-215` — `mapBinary()`: when left is not `FragmentExec`, maps right separately. If right is `LocalSourceExec` → `HashJoinExec`; if right is `FragmentExec` with lookup mode → `LookupJoinExec`

[32] `MapperUtils.java:87-191` — all 16 unary handlers accept any `PhysicalPlan child`, source-agnostic

[33] `Analyzer.java:1538-1540` — RENAME resolved to Project during analysis

[34] `InlineStats.java:138-143` — `surrogate()` expands to `InlineJoin`; right side pre-computed as `LocalRelation`

[35] `RegexMatch.java:54-61` — `toEvaluator()` creates `AutomataMatch` evaluator (pure automaton on BytesRef)

[36] `Contains.java:117-121` — `process()` is pure `BytesRef` string comparison

[37] `StartsWith.java:116-121` — `process()` is pure byte array prefix comparison

[38] `EndsWith.java:112-124` — `process()` is pure byte array suffix comparison

[39] `Count.java:146-151`, `Sum.java`, `Avg.java`, `Min.java`, `Max.java` — all return pure `AggregatorFunctionSupplier` implementations, no Lucene references

[40] `SpatialDocValuesExtraction.java` — local optimizer rule only matches `AggregateExec` above `EsQueryExec`, never external sources

[41] `PushFiltersToSource.java:54-56` — `else if (filterExec.child() instanceof ExternalSourceExec externalExec)` — dedicated external source path

[42] `PushFiltersToSource.java:241-286` — `planFilterExecForExternalSource()` — SPI-based filter pushdown via `FilterPushdownRegistry`

[43] `Analyzer.java:1851-1875` — `AddImplicitLimit` adds default LIMIT of 1000 to every query during analysis (line 1862-1863)

[44] `ReplaceLimitAndSortAsTopN.java:15-25` — fuses `Limit` + `OrderBy` into `TopN`: `if (plan.child() instanceof OrderBy o) { new TopN(...) }`
