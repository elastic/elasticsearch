# Why LOOKUP JOIN is Lucene-Tied, and What Generalization Requires

An analysis of the historical and architectural reasons LOOKUP JOIN is deeply coupled to Lucene, and what it would take to support multi-shard indices and external data sources on the right side.

---

## Table of Contents

1. [Historical Evolution](#1-historical-evolution)
2. [Three Points of Lucene Coupling](#2-three-points-of-lucene-coupling)
3. [The Single-Shard Constraint](#3-the-single-shard-constraint)
4. [What Multi-Shard Would Require](#4-what-multi-shard-would-require)
5. [What External Sources on the Right Would Require](#5-what-external-sources-on-the-right-would-require)
6. [Comparison: How Other Systems Do It](#6-comparison-how-other-systems-do-it)
7. [Recommended Path Forward](#7-recommended-path-forward)

---

## 1. Historical Evolution

LOOKUP JOIN didn't start as a join. It evolved from ENRICH — and that ancestry explains almost every design decision.

### Timeline

| Date | Event | Significance |
|------|-------|--------------|
| June 2023 | `EnrichLookupService` created | Foundation: per-row Lucene queries against enrichment indices. The pattern of "send a page, get back matched rows" is established here. |
| June 2024 | `LOOKUP` command added | Reuses ENRICH infrastructure wholesale. `Lookup` node is a UnaryPlan with surrogate pattern. |
| Oct-Nov 2024 | `LOOKUP JOIN` grammar added | `LookupJoin` extends `Join` (BinaryPlan), but execution still goes through the same `LookupFromIndexService`. |
| Mid 2025 | Multi-field joins, remote index support | Extended matching capabilities, CCS routing for remote lookup indices. |
| Jan-Feb 2026 | Streaming refactor | `StreamingLookupFromIndexOperator` added for continuous bidirectional exchange, but underlying Lucene query pattern unchanged. |

### Key Insight: ENRICH Heritage

The ENRICH command works by:
1. Taking each page of data from the left side
2. For each row, building a Lucene query (TermQuery for the enrich key)
3. Searching the enrich index directly via IndexSearcher
4. Extracting fields from matched documents
5. Returning a response with positions mapping results to input rows

LOOKUP JOIN uses **the exact same execution pattern**. The `LookupFromIndexService` extends `AbstractLookupService` (shared with `EnrichLookupService`). The `EnrichQuerySourceOperator` is the shared execution engine. The `RightChunkedLeftJoin` is shared.

The design was never "how should a general-purpose join work?" — it was "how do we reuse the ENRICH infrastructure for a user-facing JOIN syntax?"

---

## 2. Three Points of Lucene Coupling

### Coupling Point 1: Query Formulation

**Where**: `LookupFromIndexService.queryList()` (lines 161-185), `ExpressionQueryList.java`

For each row in the input page, the service builds a **Lucene Query object**:
- Field-based join: `termQueryList()` → creates `TermQuery` or `TermInSetQuery`
- Expression-based join: `ExpressionQueryList` → creates `BooleanQuery` combining `TermQuery` and `RangeQuery`

This is hardcoded to produce `org.apache.lucene.search.Query` instances. The entire join condition evaluation strategy assumes Lucene's query model.

**What makes this Lucene-specific**: The query formulation uses `SearchExecutionContext.getFieldType()` to get Lucene field types, uses `MappedFieldType.termQuery()` to build queries, and relies on Lucene's BooleanQuery composition. None of this works against a non-Lucene store.

### Coupling Point 2: Query Execution

**Where**: `EnrichQuerySourceOperator` (the shared operator used by both ENRICH and LOOKUP JOIN)

The operator:
1. Creates an `IndexSearcher` directly from the shard's `Engine.Searcher`
2. Iterates over Lucene segments
3. Calls `bulkScorer` per segment for each input row's query
4. Collects matching document IDs

This is direct Lucene API usage — `IndexSearcher.search()`, `Weight.bulkScorer()`, segment-level iteration. There is no abstraction layer between the join execution and Lucene internals.

### Coupling Point 3: Field Reading

**Where**: `AbstractLookupService` field extraction, using `BlockDocValuesReader` and `ValuesSourceReaderOperator`

Matched documents need their field values extracted. This happens through:
- `BlockDocValuesReader` — reads doc values directly from Lucene segments
- `ValuesSourceReaderOperator` — the same operator used in regular ESQL queries to extract fields from Lucene docs

Both of these work exclusively with Lucene's doc values, stored fields, and field data infrastructure.

### Summary of Coupling

```
LookupFromIndexService.queryList()
    → Uses SearchExecutionContext, MappedFieldType     [Lucene coupling 1]
    → Produces org.apache.lucene.search.Query

EnrichQuerySourceOperator
    → Uses IndexSearcher, Engine.Searcher               [Lucene coupling 2]
    → Calls Weight.bulkScorer() per segment

AbstractLookupService field extraction
    → Uses BlockDocValuesReader, ValuesSourceReader      [Lucene coupling 3]
    → Reads from Lucene doc values / stored fields
```

All three coupling points live in the **data node execution path**. The coordinator-side code (Mapper, LookupJoinExec, LookupFromIndexOperator) is relatively clean — it deals with plan nodes, layout channels, and async communication. The Lucene coupling is concentrated in the service layer.

---

## 3. The Single-Shard Constraint

### How It's Enforced

**At index creation**: `IndexModeSettingsProvider` sets `index.number_of_shards: 1` for `IndexMode.LOOKUP` indices.

**At runtime**: `AbstractLookupService` (lines 254-259) validates:
```java
List<ShardIterator> shards = routing.searchShards(state, new String[]{indexName}, ...);
if (shards.size() != 1) {
    throw new EsqlIllegalArgumentException("expected exactly one shard for lookup index");
}
```

**At the mapper level**: `isIndexModeLookup()` (Mapper.java:235-245) only allows `LookupJoinExec` for LOOKUP-mode indices.

### Why Single-Shard?

The single-shard constraint is **pragmatic, not architectural**. There's even a TODO in the codebase:

> "lookup index mode doesn't seem to force a single shard"

The real reason is simplicity:
- **No coordination needed**: With one shard, the lookup request goes to exactly one node. No scatter-gather, no merging partial results, no handling of different position ranges from different shards.
- **Position ordering guaranteed**: `RightChunkedLeftJoin` requires positions to be non-decreasing. With one shard, this is naturally satisfied. With multiple shards, results would need to be merged and positions re-mapped.
- **ENRICH heritage**: Enrich indices are always single-shard (they're built from a snapshot of the enrich policy). The infrastructure was never designed for multi-shard.

### What Single-Shard Means in Practice

- Lookup indices are limited in size (one shard, typically ≤50GB recommended)
- No horizontal scaling of the lookup side
- The lookup shard becomes a hotspot under high query concurrency
- Users wanting to join against large datasets cannot use LOOKUP JOIN

---

## 4. What Multi-Shard Would Require

Supporting regular (non-LOOKUP) Elasticsearch indices on the right side of a join requires changes at three levels:

### 4a. Shard Routing and Scatter-Gather

**Current**: Request goes to exactly one shard on one node.
**Required**: Request must be scattered to all shards of the right-side index.

Options:
1. **Broadcast per page**: Send each input page to all shards, merge results. Simple but expensive (N shards × M pages network calls).
2. **Hash partition**: Hash the join key values, route each row to the correct shard. Requires knowing the routing strategy. Only works for term-equality joins.
3. **Replicate right side**: Materialize the lookup index data on each data node (like ENRICH does with enrich indices). Only practical for small right-side data.

The most practical approach for moderate-sized right-side indices is **broadcast per page** — the same input page is sent to all shards, each shard returns its matches, and the coordinator merges.

### 4b. Position Merging

**Current**: `RightChunkedLeftJoin` assumes a single stream of non-decreasing positions.
**Required**: Multiple shard responses produce independent position streams that must be merged.

A merge step would:
1. Collect response pages from all shards for the same input page
2. Merge position arrays (sort-merge or concatenate-and-sort)
3. Re-index positions to be globally non-decreasing
4. Feed merged pages to `RightChunkedLeftJoin`

### 4c. Validation Removal

- Remove `AbstractLookupService` single-shard assertion
- Remove `IndexMode.LOOKUP` requirement from `Mapper.isIndexModeLookup()`
- Accept any `EsRelation` on the right side of a join (with appropriate index resolution)

### 4d. Estimated Effort

The changes are concentrated in:
- `Mapper.mapBinary()` — relax the LOOKUP check (~1 day)
- `LocalExecutionPlanner.planLookupJoin()` — handle multi-shard routing (~2-3 days)
- New shard scatter-gather service — the biggest piece (~3-5 days)
- `RightChunkedLeftJoin` or a new merge layer — position merging (~2 days)
- Testing — multi-shard join correctness (~3-5 days)

**Total estimate: ~2-3 weeks for a senior engineer.**

---

## 5. What External Sources on the Right Would Require

Supporting external data sources (files, Flight connectors, JDBC) on the right side is a fundamentally different problem than multi-shard.

### The Core Problem

External sources don't have:
- A Lucene index to query
- `SearchExecutionContext` or `MappedFieldType`
- `IndexSearcher` or segment-level access
- `BlockDocValuesReader` for field extraction

The entire service-side execution path (coupling points 1, 2, and 3) is inapplicable.

### Option A: External Source as Hash Table

Load the entire right-side external source into memory as a hash table (like `HashJoinExec` does for `LocalRelation`). For each left-side row, probe the hash table.

**Pros**: Simple, reuses existing `HashJoinExec` infrastructure.
**Cons**: Right side must fit in memory. No streaming. No pushdown.

This already works today if the external source can be materialized as a `LocalRelation` (e.g., small CSV files).

### Option B: Adapter Service Pattern

Create an adapter that implements the same request/response contract as `LookupFromIndexService` but delegates to external sources:

```
ExternalLookupService implements AbstractLookupService
  queryList() → translate join conditions to external source query
  execute()   → run query against external source
  extract()   → convert results to Pages with positions
```

**Pros**: Reuses coordinator-side code (`LookupFromIndexOperator`, `RightChunkedLeftJoin`).
**Cons**: Requires each external source to support per-row point lookups efficiently. Most don't.

### Option C: Sort-Merge Join

For external sources that can produce sorted output, use a sort-merge join:

```
Left side (sorted by join key) ─→ Merge ←─ Right side (sorted by join key)
```

**Pros**: Scales to large datasets, streaming, no memory limitation.
**Cons**: Both sides must be sorted. External sources need ORDER BY pushdown. Requires a new join operator (not `RightChunkedLeftJoin`).

### Option D: Broadcast Right + Local Hash Join

Materialize the external source as pages, broadcast to all data nodes, and use local hash join:

```
1. External source → read all pages → ExchangeSource
2. Each data node: build hash table from right pages
3. Each data node: probe hash table with local left pages
```

**Pros**: Distributed. Right side reads once.
**Cons**: Right side must fit in data node memory. No predicate pushdown to external source.

### What the Code Would Need

Regardless of approach:

1. **Mapper relaxation**: `mapBinary()` currently requires `isIndexModeLookup()` for the right side. Need a third path:
   ```
   right is LocalSourceExec → HashJoinExec (in-memory)
   right is FragmentExec(LOOKUP) → LookupJoinExec (Lucene)
   right is ExternalSourceExec → ExternalLookupJoinExec (new)  // or hash join
   ```

2. **New operator or adapted service**: Depending on approach, either a new join operator or an adapter implementing the lookup service contract.

3. **Filter pushdown to right side**: For efficient operation, join key filters should be pushed to the external source. E.g., if left side has `user_id IN [alice, bob]`, the external source query should be `WHERE user_id IN ('alice', 'bob')`.

4. **Schema resolution**: External source metadata resolution must work for the right side (currently, `ExternalRelation` on the right would need its metadata pre-fetched during analysis).

### Current State

Today, external sources on the right side of LOOKUP JOIN are **hardcoded to fail**. The code in `Mapper.mapBinary()` checks `isIndexModeLookup()` which requires `EsRelation` with `IndexMode.LOOKUP` — `ExternalRelation` doesn't match.

### Estimated Effort

- **Option A (hash join for small sources)**: ~1-2 weeks (mostly plumbing to load external source into LocalRelation, then reuse HashJoinExec)
- **Option B (adapter service)**: ~3-4 weeks (new service, query translation per source type, testing)
- **Option C (sort-merge)**: ~4-6 weeks (new operator, sort pushdown to external sources, streaming protocol)
- **Option D (broadcast hash join)**: ~3-4 weeks (exchange infrastructure, per-node hash table, distributed testing)

---

## 6. Comparison: How Other Systems Do It

### DuckDB
- Joins work against any scan source (Parquet, CSV, HTTP, S3)
- Uses hash join by default, sort-merge for large datasets
- No requirement that right side be a specific index type
- Right-side pushdown via projection and filter pushdown to scan

### Trino/Presto
- Joins are fully generic across connectors
- Right side can be any connector (JDBC, Hive, Iceberg, etc.)
- Uses broadcast hash join for small right sides, distributed hash join for large ones
- Connector SPI provides `applyJoin()` for join pushdown to the source

### Spark
- Same pattern as Trino — joins are connector-agnostic
- Broadcast hash join (right side < 10MB default) or sort-merge join
- V2 DataSource API supports pushdown of filters, aggregation, joins

### The Pattern

All three systems separate the **join algorithm** from the **data access method**. The join operator works with pages/batches, not with index-specific APIs. The scan operator handles data access. LOOKUP JOIN fuses these two concerns.

---

## 7. Recommended Path Forward

### Short Term: Multi-Shard Support (GA-8 scope)

1. Relax `isIndexModeLookup()` to accept any `EsRelation` (not just LOOKUP mode)
2. Add scatter-gather layer for multi-shard routing
3. Add position-merging layer before `RightChunkedLeftJoin`
4. Keep the Lucene query execution path (it works well for ES indices)

This gives users the ability to join against regular indices, which is the most-requested feature.

### Medium Term: External Sources via Hash Join (Post-MVP)

1. For small external sources: materialize right side as `LocalRelation` → use existing `HashJoinExec`
2. This requires only the Mapper to recognize `ExternalSourceExec` on the right and route to the hash join path
3. No new operators or services needed

### Long Term: Generic Join Operator

1. Abstract the join execution behind an interface that both Lucene-backed and external sources implement
2. Strategy pattern: `LuceneLookupStrategy`, `HashJoinStrategy`, `SortMergeStrategy`
3. Planner chooses strategy based on right-side type and estimated size
4. This is the architecture needed for JOIN to be fully generic

### What NOT to Do

- Don't try to make external sources implement the `LookupFromIndexService` contract — it's too Lucene-specific
- Don't add a Lucene compatibility layer over external sources — it's the wrong abstraction
- Don't remove the LOOKUP-mode fast path — it's genuinely optimal for its use case (small, single-shard, point lookups)

The right design preserves the current Lucene path as a specialization while adding more general paths alongside it.
