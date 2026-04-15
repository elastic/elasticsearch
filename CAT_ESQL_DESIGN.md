# ES|QL CAT Source Command

Author: Luigi Dell'Aquila
Current status: Draft (PoC complete)

# List of mandatory reviewers

| Name | Team |
| :---- | :---- |
| TBD | ES|QL |
| TBD | Security |

# Goals

* Expose cluster and index metadata (health, indices, nodes, shards, aliases, allocation) as queryable tables in ES|QL.
* Allow users to filter, aggregate, and join cluster metadata using standard ES|QL syntax: `CAT indices | WHERE health == "red" | KEEP index, health`.
* Reuse existing transport actions for data fetching — no new APIs, no new privilege model.
* Phase 1 covers 6 endpoints: `health`, `indices`, `nodes`, `shards`, `aliases`, `allocation`.

**Non-goals:**
* Full coverage of all 22 CAT endpoints (future phases).
* Streaming execution or LIMIT pushdown (future optimization).
* Custom privilege model for CAT — relies on existing transport action authorization.

# Context and Scope

Today, cluster metadata is accessible via the `_cat` REST API or the underlying transport actions. Users who want to answer operational questions like "which indices have red health and more than 1M docs?" must script multiple API calls and post-process the results externally.

ES|QL is a natural fit: it already supports source commands (`FROM`, `ROW`, `SHOW`), filtering, aggregation, and projection. Adding `CAT` as a source command lets users query cluster metadata with the same language they use for data.

The implementation follows **Option B** from the original spec: pre-resolve CAT data in `EsqlSession.resolveIndicesAndAnalyze()` before analysis, converting `UnresolvedCatRelation` into `LocalRelation` nodes. This avoids the need for custom physical plan nodes, execution operators, or mapper changes.

# Design Proposal

## Architecture Overview

```
Parse time:     "CAT indices" --> UnresolvedCatRelation("indices")
Pre-analysis:   PreAnalyzer collects endpoint names from UnresolvedCatRelation nodes
Pre-resolution: CatDataResolver fetches data via transport actions --> Table objects
                ThreadedActionListener forks response back to SEARCH thread pool
Analysis time:  ResolveCatRelations infers schema from Table, converts to LocalRelation
                UnresolvedCatRelation --> LocalRelation(inferred schema, Page)
Optimization:   Standard ES|QL optimizations (column pruning, filter pushdown)
Execution:      LocalSourceExec -- data already in memory, no I/O at execution time
```

## Grammar

A new `DEV_CAT` lexer token is added via `lexer/Cat.g4`, gated with `{this.isDevVersion()}?` (snapshot-only). It pushes into the existing `FROM_MODE` which provides `UNQUOTED_SOURCE` for the endpoint name.

```antlr
// Cat.g4
DEV_CAT : {this.isDevVersion()}? 'cat' -> pushMode(FROM_MODE);
```

The parser adds `catCommand` to `sourceCommand`:

```antlr
catCommand : DEV_CAT UNQUOTED_SOURCE ;
```

This reuses `FROM_MODE` rather than introducing a dedicated `CAT_MODE`, keeping the grammar minimal.

## Data Flow

### 1. Pre-Analysis

`PreAnalyzer` collects endpoint names from `UnresolvedCatRelation` nodes into `PreAnalysis.catEndpoints`.

### 2. Data Resolution

`CatDataResolver` takes a `Client` and `ProjectResolver`. For each endpoint, it dispatches the appropriate transport actions in parallel using `RefCountingListener` (for endpoints requiring 2-3 transport calls) and builds a `Table` per endpoint.

Per-endpoint transport actions:

| Endpoint | Transport Actions |
|:---------|:------------------|
| `health` | `cluster:monitor/health` |
| `indices` | `cluster:monitor/state` + `indices:monitor/stats` |
| `nodes` | `cluster:monitor/state` + `cluster:monitor/nodes/info` + `cluster:monitor/nodes/stats` |
| `shards` | `cluster:monitor/state` + `indices:monitor/stats` |
| `aliases` | `indices:admin/aliases/get` |
| `allocation` | `cluster:monitor/state` + `cluster:monitor/nodes/stats` |

Cell values are stored as typed Java objects (`Integer`, `Long`, `Double`, `String`) rather than pre-stringified, so type information is preserved for schema inference.

### 3. Thread Transition

Transport action responses arrive on the `management` thread pool. The ES|QL analysis chain asserts it runs on the `search` thread. `preAnalyzeCatSources()` wraps the listener with `ThreadedActionListener` to fork back to `SEARCH` before continuing the analysis chain:

```java
var searchListener = new ThreadedActionListener<>(
    transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
    listener.map(result::withCatDataResolution)
);
catDataResolver.resolve(preAnalysis.catEndpoints(), searchListener);
```

### 4. Schema Inference

Schema is inferred dynamically from the `Table` at analysis time (not from a static registry). `CatTableConverter.inferSchema(Table)` walks the table headers and scans each column for the first non-null cell value:

| Java Type | ES\|QL DataType |
|:----------|:----------------|
| `Integer` | INTEGER         |
| `Long` | LONG            |
| `Double` | DOUBLE          |
| `Boolean` | BOOLEAN         |
| Anything else / all-null | KEYWORD         |

This approach means adding a new endpoint only requires implementing its `resolveXxx()` method in `CatDataResolver` — no schema class to maintain.

### 5. Analysis

`ResolveCatRelations` is a `ParameterizedAnalyzerRule` in the Initialize batch. It looks up the pre-fetched `Table` from `AnalyzerContext.catDataResolution()`, infers the schema, converts the table to a `Page`, and returns a `LocalRelation`:

```java
var table = context.catDataResolution().get(endpoint);
if (table == null) return plan; // unknown endpoint, leave unresolved for Verifier
var attributes = CatTableConverter.inferSchema(table);
var page = CatTableConverter.toPage(table, attributes);
return new LocalRelation(plan.source(), attributes, LocalSupplier.of(page));
```

After this point, the plan is a standard `LocalRelation` and goes through all normal ES|QL optimizations and execution.

## Dotted Field Names

CAT column names contain dots (e.g., `docs.count`, `store.size`, `heap.percent`). ES|QL resolves field names by exact string equality (`Objects.equals(name, a.name())`), **not** by nested path traversal. So dotted names work as literal field names with no backtick quoting needed:

```esql
CAT indices | WHERE docs.count > 1000
CAT nodes | SORT heap.percent DESC
```

## Examples

```esql
-- Cluster health overview
CAT health

-- Find red indices sorted by doc count
CAT indices | WHERE health == "red" | SORT docs.count DESC

-- Nodes with high heap usage
CAT nodes | WHERE heap.percent > 80 | KEEP name, ip, heap.percent, cpu

-- Unassigned shards by index
CAT shards | WHERE state == "UNASSIGNED" | STATS count = COUNT(*) BY index

-- Disk allocation per node
CAT allocation | SORT disk.percent DESC | KEEP node, disk.used, disk.avail, disk.percent

-- Aliases for a specific index pattern
CAT aliases | WHERE index LIKE "logs-*"
```

All standard ES|QL commands work downstream: `WHERE`, `KEEP`, `DROP`, `SORT`, `LIMIT`, `STATS`, `EVAL`, `RENAME`, `ENRICH`, `LOOKUP JOIN`, etc.

## User Interface (Kibana) and Client concerns

* **Grammar change:** Yes. Kibana depends on the ES|QL grammar and will need to update the editor and autocomplete. The `DEV_CAT` token reuses `FROM_MODE`, so the autocomplete for the endpoint name can follow the same pattern as index names in `FROM`.
* **Autocomplete:** Kibana should suggest the 6 supported endpoint names after `CAT`: `health`, `indices`, `nodes`, `shards`, `aliases`, `allocation`.
* **Column suggestions:** After `CAT indices |`, Kibana should suggest the available columns. Since the schema is dynamic, this would require either a static list in the client or a capabilities/metadata API call.
* **Capability gate:** Clients should check for the `CAT_COMMAND` capability before offering CAT in the UI.

## Alternative proposals

### Option A: CatSourceExec at Execution Time (from spec)

Fetch data at execution time via a custom `CatSourceExec` physical plan and `CatSourceOperator`. This would allow column pruning before data fetching and streaming large result sets.

**Rejected for Phase 1** because it requires new physical plan nodes, mapper changes, and custom operator implementations. Option B is simpler and sufficient for the 6 endpoints with their moderate result set sizes.

**Revisit for Phase 2** if memory becomes a concern (e.g., `CAT shards` on very large clusters).

### Static Schema Registry (CatSchemaRegistry)

The original spec proposed a hardcoded `CatSchemaRegistry` class mapping endpoint names to `List<Attribute>` with explicit DataType assignments.

**Rejected** in favor of dynamic schema inference from the `Table` data. This eliminates a maintenance burden: adding a new endpoint or column no longer requires updating a schema class. The tradeoff is that type inference depends on having at least one non-null value per column — all-null columns default to KEYWORD.

### Using REST CAT Handlers Directly

Reuse the existing `RestIndicesAction`, `RestHealthAction`, etc. to build the `Table` objects.

**Rejected** because REST handlers are tightly coupled to REST request/response handling. Calling transport actions directly is cleaner and avoids creating a dependency from the ES|QL plugin to REST handler internals.

# Open Questions

1. **Per-endpoint privilege granularity:** Should we add a CAT-specific privilege (e.g., `esql:cat/indices`) to allow granting CAT access independently of the underlying transport action privileges? Or is transport-level authorization sufficient?

2. **Empty table schema inference:** When a table has 0 rows (e.g., `CAT indices` on a cluster with no indices), schema inference defaults all columns to KEYWORD. Should we add a fallback to a minimal static schema for this edge case, or is KEYWORD-for-everything acceptable?

3. **Verifier error messages:** Currently, unknown endpoints leave `UnresolvedCatRelation` unresolved. Should we add a dedicated Verifier check that produces `Unknown CAT endpoint [foo]. Available: health, indices, nodes, shards, aliases, allocation`?

4. **Large result sets:** `CAT shards` on a cluster with millions of shards loads all data into memory. Should we add a default row limit or require `LIMIT` in the query? Or defer this to Option A migration?

5. **Column aliasing:** CAT REST API supports aliases (e.g., `h` for `health`, `dc` for `docs.count`). Should ES|QL honor these, or only support the canonical names?

# Complexity and Risk

## Estimated Effort

The PoC implementation touched ~15 files and created 5 new files. The core complexity is in `CatDataResolver` (async multi-request coordination per endpoint) and the threading transition. The rest is plumbing.

For production readiness beyond the PoC:
- Error handling and security exception translation
- Unit tests (`CatTableConverterTests`, `CatDataResolverTests`)
- Integration tests (YAML REST tests for each endpoint)
- Verifier error messages for unknown endpoints
- Documentation

## Risk Areas

### Schema Drift

CAT handler column definitions can change between Elasticsearch versions. Since we infer schema dynamically, new columns appear automatically. However, type changes (e.g., a column switching from `String` to `Long`) would silently change the inferred type.

**Mitigation:** Add a test that exercises each endpoint's `buildXxxTable()` method and asserts the expected column count and types.

### Large Result Sets

`CAT shards` loads all shards into a `LocalRelation` Page in memory. On a cluster with 1M shards, this is significant.

**Mitigation (Phase 1):** Document the limitation. Consider a configurable row limit.
**Mitigation (Phase 2):** Migrate to Option A with streaming execution.

### Cautionary Tale Comparison

This feature has **low interaction complexity** with the rest of ES|QL. Unlike `SORT` (which requires optimizer cooperation) or unmapped fields (which affect every query shape), `CAT` produces a `LocalRelation` that is a self-contained leaf — no downstream commands need special handling. The risk of scope escalation is low.

# Testing and rollout

* **Phase 1 (current):** Snapshot-only, gated behind `CAT_COMMAND` capability and `DEV_CAT` lexer token (`{this.isDevVersion()}?`).
* **Testing:** Unit tests for `CatTableConverter` and `CatDataResolver`. YAML REST tests for each endpoint covering basic queries, column projection, filtering, and aggregation.
* **Tech Preview:** Requires Kibana autocomplete support for `CAT` command and endpoint names. Documentation for supported endpoints and column schemas.
* **GA:** Requires cross-cluster search support (if applicable — CAT data is inherently local to a cluster). Remove `DEV_` gate.

# Cross cutting concerns

- [x] **Security concerns**
  * The `Client` (NodeClient) runs transport actions under the calling user's security context — no privilege escalation.
  * Users need `cluster:monitor/*` and/or `indices:monitor/*` privileges depending on the endpoint.
  * **Risk:** ES|QL provides a new programmatic attack surface for cluster metadata. A user with ES|QL access + monitor privileges can now systematically query and filter node IPs, heap/CPU/disk usage, index names, and shard allocation. This data was previously only accessible via direct `_cat` REST API calls.
  * **Risk:** Security exceptions from transport actions currently propagate as raw errors. Should be translated to clean ES|QL error messages.
  * **Recommendation:** Security team review required before Tech Preview.

- [ ] **Performance and load testing**
  * `CAT shards` and `CAT indices` can return large result sets on big clusters. Load testing needed with 10K+ indices / 100K+ shards.
  * All data is loaded into memory at pre-analysis time (Option B). No lazy/streaming evaluation.

- [ ] **Metrics and Monitoring**
  * `CAT` is tracked as a `FeatureMetric` in telemetry (matching on `UnresolvedCatRelation` in the pre-analysis plan).
  * Per-endpoint usage tracking is not implemented (all endpoints count as one `CAT` metric).

- [ ] **Cloud or multi-project/multi-tenant concerns**
  * Uses `ProjectResolver.getProjectMetadata(ClusterState)` for per-project index metadata access.
  * CAT data is scoped to the current project in multi-project clusters.
  * `CAT nodes` and `CAT health` expose cluster-wide data (not project-scoped). This may be a concern in multi-tenant environments where tenants should not see each other's node topology.

# Addendum

## Files Created (PoC)

| File | Purpose |
|:-----|:--------|
| `x-pack/plugin/esql/src/main/antlr/lexer/Cat.g4` | Lexer fragment for `DEV_CAT` token |
| `x-pack/.../plan/logical/UnresolvedCatRelation.java` | Unresolved leaf plan node |
| `x-pack/.../cat/CatDataResolution.java` | `record(Map<String, Table>)` holding pre-fetched data |
| `x-pack/.../cat/CatDataResolver.java` | Async data fetching via transport actions |
| `x-pack/.../cat/CatTableConverter.java` | Dynamic schema inference + Table-to-Page conversion |

## Files Modified (PoC)

| File | Change |
|:-----|:-------|
| `EsqlBaseLexer.g4` | Added `Cat` to import list |
| `EsqlBaseParser.g4` | Added `catCommand` rule to `sourceCommand` |
| `LogicalPlanBuilder.java` | Added `visitCatCommand()` |
| `PreAnalyzer.java` | Collect CAT endpoints into `PreAnalysis.catEndpoints` |
| `EsqlSession.java` | Added `preAnalyzeCatSources()` with `ThreadedActionListener` fork |
| `Analyzer.java` | Added `ResolveCatRelations` rule |
| `AnalyzerContext.java` | Added `CatDataResolution` field and getter |
| `EsqlCapabilities.java` | Added `CAT_COMMAND` capability |
| `TransportActionServices.java` | Added `Client` field |
| `TransportEsqlQueryAction.java` | Pass `client` to `TransportActionServices` |
| `FeatureMetric.java` | Added `CAT` metric |
| `ApproximationSupportTests.java` | Added `UnresolvedCatRelation` to excluded list |
| `EsqlTestUtils.java` | Updated `MOCK_TRANSPORT_ACTION_SERVICES` constructor |
