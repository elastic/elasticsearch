# ES|QL CAT Command - Design Specification

## Overview

This document specifies a new ES|QL source command `CAT` that wraps the existing Elasticsearch CAT REST API, making cluster/index metadata queryable via ES|QL. Example usage:

```esql
CAT indices | WHERE health == "red" | KEEP index, health, pri, rep, docs.count | SORT docs.count DESC
CAT nodes | WHERE heap.percent > 80
CAT shards | WHERE state == "UNASSIGNED" | STATS count = COUNT(*) BY index
```

The command delegates data fetching to the existing CAT API transport actions at execution time. Schema is inferred statically at analysis time from a hardcoded registry. Security is enforced by running the underlying transport actions under the calling user's identity.

---

## 1. Architecture

### Execution Pipeline

```
Parse time:     "CAT indices" → UnresolvedCatRelation("indices")
Pre-analysis:   PreAnalyzer collects CAT endpoint names from UnresolvedCatRelation nodes
Analysis time:  Analyzer.ResolveCatRelation resolves schema from CatSchemaRegistry
                UnresolvedCatRelation("indices") → CatRelation("indices", schema=[...])
                No I/O — schema is static per endpoint
Optimization:   Standard ES|QL optimizations (column pruning, filter pushdown to downstream)
Mapping:        CatRelation → LocalRelation (via optimizer rule, not Mapper)
                OR CatRelation → CatSourceExec (if we want execution-time data)
Execution:      CatSourceOperator calls NodeClient to invoke transport actions,
                builds Table, converts to ES|QL Page/Blocks
```

### Design Decision: Coordinator-Only Execution

CAT data is cluster-wide metadata, not per-shard data. The CAT command **must** execute on the coordinator node only. This is modeled the same way as `ExternalRelation` — the `CatRelation` logical plan implements `ExecutesOn.Coordinator`.

However, unlike `ExternalRelation`, CAT data cannot be resolved at analysis time (it requires async transport action calls). The recommended approach is:

**Option A (Recommended): CatRelation as a leaf plan, resolved at execution time**
- `CatRelation` is a `LeafPlan` with static schema (known at analysis time) but data fetched at execution time
- In the `Mapper`, `CatRelation` maps to `FragmentExec(CatRelation)` (like `ExternalRelation`)
- In the `LocalMapper`, `CatRelation` maps to `CatSourceExec`
- `CatSourceExec` creates a `CatSourceOperator` that calls the transport actions asynchronously

**Option B (Simpler but less efficient): Resolve data before analysis**
- Fetch CAT data in `EsqlSession.resolveIndicesAndAnalyze()` (alongside external sources)
- Convert to `LocalRelation` with a pre-computed `Page`
- No custom physical plan needed — reuses `LocalSourceExec`
- Downside: data is fetched before column pruning is known, always fetches all columns/rows

**Recommendation: Start with Option B** for simplicity (fewer new classes, leverages existing LocalRelation infrastructure). Migrate to Option A later if performance matters (large clusters with many shards/nodes).

---

## 2. Grammar

### Lexer

Create a new file `x-pack/plugin/esql/src/main/antlr/lexer/Cat.g4`:

```antlr
// Cat.g4 (lexer fragment)
CAT : 'cat' -> pushMode(CAT_MODE);

mode CAT_MODE;
CAT_WS : WS -> channel(HIDDEN);
CAT_ENDPOINT : UNQUOTED_IDENTIFIER;  // e.g., "indices", "nodes", "shards"
CAT_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
CAT_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);
// Pipe transitions back to default mode
CAT_PIPE : '|' -> type(PIPE), popMode;
```

### Parser

Add to `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`:

```antlr
// In sourceCommand rule (around line 86):
sourceCommand
    : explainCommand
    | fromCommand
    | rowCommand
    | timeSeriesCommand
    | catCommand           // NEW
    | externalCommand
    ;

// New rule:
catCommand
    : CAT CAT_ENDPOINT
    ;
```

### Capability Gate

Add to `EsqlCapabilities.Cap` enum in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java`:

```java
/**
 * Support for CAT command to query cluster metadata via ES|QL.
 */
CAT_COMMAND(Build.current().isSnapshot()),
```

Gate the grammar rule with this capability in `LogicalPlanBuilder`.

---

## 3. Logical Plan Nodes

### UnresolvedCatRelation

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedCatRelation.java`

```java
public class UnresolvedCatRelation extends LeafPlan implements Unresolvable {
    private final String endpoint;  // "indices", "nodes", "shards", etc.

    public UnresolvedCatRelation(Source source, String endpoint) {
        super(source);
        this.endpoint = endpoint;
    }

    public String endpoint() { return endpoint; }

    @Override
    public boolean expressionsResolved() { return false; }

    @Override
    public List<Attribute> output() { return List.of(); }

    @Override
    public String unresolvedMessage() {
        return "Unresolved CAT endpoint [" + endpoint + "]";
    }
}
```

### CatRelation

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/CatRelation.java`

```java
public class CatRelation extends LeafPlan {
    private final String endpoint;
    private final List<Attribute> output;

    public CatRelation(Source source, String endpoint, List<Attribute> output) {
        super(source);
        this.endpoint = endpoint;
        this.output = output;
    }

    public String endpoint() { return endpoint; }

    @Override
    public List<Attribute> output() { return output; }

    @Override
    public boolean expressionsResolved() { return true; }
}
```

---

## 4. Schema Registry

### CatSchemaRegistry

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/cat/CatSchemaRegistry.java`

This class defines the static schema for each supported CAT endpoint. The schema maps CAT Table column names to ES|QL `DataType`s.

#### Type Mapping Rules

The CAT API `Table.Cell.value` field is `Object` with no type annotation. The following rules derive ES|QL types:

| Java Runtime Type in Cell | ES|QL DataType | Conversion |
|---|---|---|
| `String` | `KEYWORD` | Direct |
| `int` / `Integer` | `INTEGER` | Direct |
| `long` / `Long` | `LONG` | Direct |
| `short` / `Short` | `INTEGER` | Widen to int |
| `double` / `Double` | `DOUBLE` | Direct |
| `boolean` / `Boolean` | `BOOLEAN` | Direct |
| `ByteSizeValue` | `LONG` | `.getBytes()` → long |
| `TimeValue` | `LONG` | `.millis()` → long |
| `null` | (column type) | ES|QL null |
| `"-"` sentinel | (column type) | ES|QL null |

#### Column Naming

CAT column names contain dots (e.g., `docs.count`, `store.size`, `pri.store.size`). ES|QL interprets dots as field path separators. Two options:

**Option A (Recommended): Backtick-quoting in user queries**
Keep original CAT names. Users write:
```esql
CAT indices | WHERE `docs.count` > 1000
```

**Option B: Rename to underscores**
Transform `docs.count` → `docs_count`. Simpler for users but breaks parity with CAT API naming.

**Recommendation:** Option A — preserve original names, document that backtick-quoting is needed for dotted names.

#### Supported Endpoints (Phase 1)

Start with these 6 endpoints. Each section below lists every column, its ES|QL type, and whether it is included by default.

##### `health`

Underlying transport actions:
- `cluster:monitor/health` (ClusterHealthAction)

| Column | ES|QL Type | Default | Source |
|---|---|---|---|
| `cluster` | KEYWORD | yes | `ClusterHealthResponse.getClusterName()` |
| `status` | KEYWORD | yes | `ClusterHealthResponse.getStatus().name().toLowerCase()` |
| `node.total` | INTEGER | yes | `ClusterHealthResponse.getNumberOfNodes()` |
| `node.data` | INTEGER | yes | `ClusterHealthResponse.getNumberOfDataNodes()` |
| `shards` | INTEGER | yes | `ClusterHealthResponse.getActiveShards()` |
| `pri` | INTEGER | yes | `ClusterHealthResponse.getActivePrimaryShards()` |
| `relo` | INTEGER | yes | `ClusterHealthResponse.getRelocatingShards()` |
| `init` | INTEGER | yes | `ClusterHealthResponse.getInitializingShards()` |
| `unassign` | INTEGER | yes | `ClusterHealthResponse.getUnassignedShards()` |
| `unassign.pri` | INTEGER | yes | `ClusterHealthResponse.getUnassignedPrimaryShards()` |
| `pending_tasks` | INTEGER | yes | `ClusterHealthResponse.getNumberOfPendingTasks()` |
| `max_task_wait_time` | LONG | yes | `ClusterHealthResponse.getTaskMaxWaitingTime().millis()` (or null if 0) |
| `active_shards_percent` | KEYWORD | yes | `String.format("%1.1f%%", percent)` |

##### `indices`

Underlying transport actions:
- `indices:monitor/settings/get` (GetSettingsAction)
- `cluster:monitor/state` (ClusterStateAction)
- `indices:monitor/stats` (IndicesStatsAction)

Default-visible columns only (there are ~120 total columns including `pri.*` siblings):

| Column | ES|QL Type | Source |
|---|---|---|
| `health` | KEYWORD | `ClusterHealthStatus` string, may have `"red*"` suffix |
| `status` | KEYWORD | `IndexMetadata.State.toString().toLowerCase()` |
| `index` | KEYWORD | Index name string |
| `uuid` | KEYWORD | `IndexMetadata.getIndexUUID()` |
| `pri` | INTEGER | `IndexMetadata.getNumberOfShards()` |
| `rep` | INTEGER | `IndexMetadata.getNumberOfReplicas()` |
| `docs.count` | LONG | `CommonStats.getDocs().getCount()` |
| `docs.deleted` | LONG | `CommonStats.getDocs().getDeleted()` |
| `store.size` | LONG | `StoreStats.size().getBytes()` (ByteSizeValue → long bytes) |
| `dataset.size` | LONG | `StoreStats.totalDataSetSize().getBytes()` |

Non-default columns (partial list — full list has ~110 more):
- `creation.date` (LONG, millis), `creation.date.string` (KEYWORD, ISO 8601)
- `pri.store.size` (LONG), `completion.size` (LONG), `pri.completion.size` (LONG)
- `fielddata.memory_size` (LONG), `fielddata.evictions` (LONG)
- `query_cache.memory_size` (LONG), `query_cache.evictions` (LONG)
- `request_cache.memory_size` (LONG), `request_cache.evictions` (LONG), `request_cache.hit_count` (LONG), `request_cache.miss_count` (LONG)
- `flush.total` (LONG), `flush.total_time` (LONG)
- `get.*` columns (LONG), `indexing.*` columns (LONG)
- `merges.*` columns (LONG), `refresh.*` columns (LONG)
- `search.*` columns (LONG), `segments.*` columns (LONG)
- `warmer.*` columns (LONG), `suggest.*` columns (LONG)
- `bulk.*` columns (LONG), `memory.total` (LONG)
- `dense_vector.value_count` (LONG), `sparse_vector.value_count` (LONG)

All sibling/pri columns follow the same pattern: `<metric>` (total) + `pri.<metric>` (primary only).

##### `nodes`

Underlying transport actions:
- `cluster:monitor/state` (ClusterStateAction)
- `cluster:monitor/nodes/info` (NodesInfoAction)
- `cluster:monitor/nodes/stats` (NodesStatsAction)

Default-visible columns:

| Column | ES|QL Type | Source |
|---|---|---|
| `ip` | KEYWORD | `node.getHostAddress()` |
| `heap.percent` | INTEGER | `jvmStats.getMem().getHeapUsedPercent()` |
| `ram.percent` | INTEGER | `osStats.getMem().getUsedPercent()` |
| `cpu` | INTEGER | `osStats.getCpu().getPercent()` |
| `load_1m` | KEYWORD | `osStats.getCpu().getLoadAverage()[0]` (formatted) |
| `load_5m` | KEYWORD | `osStats.getCpu().getLoadAverage()[1]` (formatted) |
| `load_15m` | KEYWORD | `osStats.getCpu().getLoadAverage()[2]` (formatted) |
| `node.role` | KEYWORD | `node.getRoleAbbreviationString()` |
| `master` | KEYWORD | `"*"` if master, `"-"` otherwise |
| `name` | KEYWORD | `node.getName()` |

Non-default columns (~58 more): `id`, `pid`, `port`, `http_address`, `version`, `type`, `build`, `jdk`, `disk.*`, `heap.*`, `ram.*`, `file_desc.*`, `completion.size`, `fielddata.*`, `query_cache.*`, `request_cache.*`, `flush.*`, `get.*`, `indexing.*`, `merges.*`, `refresh.*`, `script.*`, `search.*`, `segments.*`, `suggest.*`, `bulk.*`, `shard_stats.total_count`, `mappings.*`.

##### `shards`

Underlying transport actions:
- `cluster:monitor/state` (ClusterStateAction)
- `indices:monitor/stats` (IndicesStatsAction)

Default-visible columns:

| Column | ES|QL Type | Source |
|---|---|---|
| `index` | KEYWORD | `shard.getIndexName()` |
| `shard` | INTEGER | `shard.id()` |
| `prirep` | KEYWORD | `"p"` or `"r"` |
| `state` | KEYWORD | `shard.state().toString()` |
| `docs` | LONG | `CommonStats.getDocs().getCount()` |
| `store` | LONG | `StoreStats.size().getBytes()` |
| `dataset` | LONG | `StoreStats.totalDataSetSize().getBytes()` |
| `ip` | KEYWORD | `node.getHostAddress()` |
| `node` | KEYWORD | `node.getName()` |

Non-default columns (~65 more): `id`, `sync_id`, `unassigned.reason`, `unassigned.at`, `unassigned.for`, `unassigned.details`, `recoverysource.type`, `completion.size`, `fielddata.*`, `query_cache.*`, `flush.*`, `get.*`, `indexing.*`, `merges.*`, `refresh.*`, `search.*`, `segments.*`, `seq_no.*`, `warmer.*`, `path.*`, `bulk.*`, `dense_vector.value_count`, `sparse_vector.value_count`.

##### `aliases`

Underlying transport actions:
- `indices:admin/aliases/get` (GetAliasesAction)

| Column | ES|QL Type | Default | Source |
|---|---|---|---|
| `alias` | KEYWORD | yes | `AliasMetadata.alias()` |
| `index` | KEYWORD | yes | Index name string |
| `filter` | KEYWORD | yes | `"*"` if filtering, `"-"` otherwise |
| `routing.index` | KEYWORD | yes | `AliasMetadata.indexRouting()` or `"-"` |
| `routing.search` | KEYWORD | yes | `AliasMetadata.searchRouting()` or `"-"` |
| `is_write_index` | KEYWORD | yes | `AliasMetadata.writeIndex().toString()` or `"-"` |

##### `allocation`

Underlying transport actions:
- `cluster:monitor/state` (ClusterStateAction)
- `cluster:monitor/nodes/stats` (NodesStatsAction)

| Column | ES|QL Type | Default | Source |
|---|---|---|---|
| `shards` | INTEGER | yes | Shard count per node |
| `shards.undesired` | INTEGER | yes | `NodeAllocationStats.undesiredShards()` |
| `write_load.forecast` | DOUBLE | yes | `NodeAllocationStats.forecastedIngestLoad()` |
| `disk.indices.forecast` | LONG | yes | `NodeAllocationStats.forecastedDiskUsage()` (bytes) |
| `disk.indices` | LONG | yes | `StoreStats.size().getBytes()` |
| `disk.used` | LONG | yes | Calculated bytes |
| `disk.avail` | LONG | yes | Available bytes |
| `disk.total` | LONG | yes | Total bytes |
| `disk.percent` | INTEGER | yes | Calculated percentage |
| `host` | KEYWORD | yes | `node.getHostName()` |
| `ip` | KEYWORD | yes | `node.getHostAddress()` |
| `node` | KEYWORD | yes | `node.getName()` or `"UNASSIGNED"` |
| `node.role` | KEYWORD | yes | `node.getRoleAbbreviationString()` |

---

## 5. Security

### Threat Model

The primary risk is **inadvertent privilege escalation**: an ES|QL user who has `indices:data/read/esql` privileges but not `cluster:monitor/*` privileges could gain access to cluster metadata.

### Design: Delegated Security (No Escalation)

The CAT command executes the underlying transport actions **under the calling user's security context**. The `NodeClient` used in ES|QL (available via `ComputeService` or `PlanExecutor`) propagates the original user's `ThreadContext` headers, which include the authentication token. The `SecurityActionFilter` intercepts each transport action and enforces authorization.

#### Required Privileges per Endpoint

| CAT Endpoint | Required Transport Action Privileges |
|---|---|
| `health` | `cluster:monitor/health` |
| `indices` | `indices:monitor/settings/get`, `cluster:monitor/state`, `indices:monitor/stats` |
| `nodes` | `cluster:monitor/state`, `cluster:monitor/nodes/info`, `cluster:monitor/nodes/stats` |
| `shards` | `cluster:monitor/state`, `indices:monitor/stats` |
| `aliases` | `indices:admin/aliases/get` |
| `allocation` | `cluster:monitor/state`, `cluster:monitor/nodes/stats` |

These map to the standard Elasticsearch `monitor` cluster privilege and `monitor` index privilege. A user with the `monitor` cluster privilege and `monitor` index privilege on the relevant indices will have access.

#### Error Handling

If the user lacks required privileges, the transport action will throw a `SecurityException` (specifically `ElasticsearchSecurityException`). This exception should be caught and translated into an ES|QL-level error message:

```
Cannot execute CAT indices: user lacks required privilege [cluster:monitor/state]
```

**Important: Do NOT catch and swallow security exceptions.** Let them propagate as query failures. Do not fall back to partial results or empty results when security denies access — this would mask a configuration problem and could be misleading.

#### No System Context Escalation

The implementation must **never** use:
- `SecurityContext.executeAsInternalUser()`
- `SecurityContext.executeWithAuthentication()` with a different user
- `stashContext()` to strip security headers
- Any mechanism that bypasses the calling user's identity

All `NodeClient` calls must execute with the default thread context, which carries the original user's credentials.

---

## 6. Execution & Threading

### Constraint: No Network Thread Execution

ES|QL enforces that query execution does not happen on the network/transport thread pool. The existing flow:

1. `TransportEsqlQueryAction.doExecute()` forks to `SEARCH` thread pool (line 275 of `TransportEsqlQueryAction.java`)
2. Compute operators run on `esql_worker` thread pool (configurable via `xpack.esql.worker.thread_pool_size`)

### CAT Data Fetching Threading

The transport actions invoked by CAT (e.g., `ClusterStateAction`, `NodesStatsAction`) are **asynchronous** — they accept an `ActionListener` and callback on the transport thread. The response processing (Table building) then runs on whatever thread the callback arrives on (typically `MANAGEMENT` or `GENERIC` pool).

#### Implementation Strategy

**For Option B (pre-resolve in EsqlSession):**
- Add a `preAnalyzeCatSources()` step in `EsqlSession.resolveIndicesAndAnalyze()` (alongside `preAnalyzeExternalSources()`)
- This step runs on the `SEARCH` thread (already asserted)
- The NodeClient calls are async — the listener chain handles the result on the callback thread
- The `SubscribableListener` chain in `resolveIndicesAndAnalyze()` ensures proper ordering
- Once the Table is fetched, convert to `LocalRelation` with a pre-computed `Page`
- No execution-time I/O needed — all data is embedded in the plan

**For Option A (CatSourceOperator at execution time):**
- The `CatSourceOperator` would need to perform async I/O
- Use the `esql_worker` thread pool (via `externalSourceExecutor()`)
- The operator would block waiting for transport action responses
- This is acceptable since `esql_worker` is a dedicated pool (not the network thread)

### Recommendation for Threading

Option B is simpler for threading: the async transport calls fit naturally into the `SubscribableListener` chain in `resolveIndicesAndAnalyze()`, where external source resolution already happens. No custom operator threading needed.

---

## 7. Implementation Plan

### Phase 1: Minimal Viable Implementation (Option B)

#### Step 1: Grammar & Parser

1. Create lexer fragment `x-pack/plugin/esql/src/main/antlr/lexer/Cat.g4` with `CAT` keyword and `CAT_MODE`
2. Add `catCommand` rule to `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` under `sourceCommand`
3. Add `CAT_COMMAND` to `EsqlCapabilities.Cap` enum (gated on `Build.current().isSnapshot()`)
4. Regenerate ANTLR files: `./gradlew :x-pack:plugin:esql:regen`
5. Add `visitCatCommand()` to `LogicalPlanBuilder.java` that returns `UnresolvedCatRelation`

**Reference files:**
- Lexer pattern: `x-pack/plugin/esql/src/main/antlr/lexer/From.g4`
- Parser: `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` (line 86, sourceCommand rule)
- LogicalPlanBuilder: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java` (line 348, visitRowCommand as example)

#### Step 2: Logical Plan Nodes

1. Create `UnresolvedCatRelation.java` in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/`
   - Extends `LeafPlan`, implements `Unresolvable`
   - Fields: `String endpoint`
   - Register in `NamedWriteableRegistry` (see `LogicalPlan` writeable entries)

2. Create `CatRelation.java` (same directory)
   - Extends `LeafPlan`
   - Fields: `String endpoint`, `List<Attribute> output`
   - Does NOT need `ExecutesOn.Coordinator` for Option B (it becomes LocalRelation before mapping)
   - Register in `NamedWriteableRegistry`

**Reference:** `UnresolvedExternalRelation.java`, `ExternalRelation.java` in same directory.

#### Step 3: Schema Registry

1. Create `CatSchemaRegistry.java` in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/cat/`
2. Static method: `Optional<List<Attribute>> schema(String endpoint)` — returns null for unknown endpoints
3. Uses `FieldAttribute` (from `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/FieldAttribute.java`) for each column
4. Map each column to appropriate `DataType` per the type mapping rules in Section 4

**Implementation note:** Create attributes using:
```java
new FieldAttribute(Source.EMPTY, columnName, new EsField(columnName, dataType, Map.of(), false))
```

Or use `ReferenceAttribute` which is simpler and doesn't require an `EsField`:
```java
new ReferenceAttribute(Source.EMPTY, columnName, dataType)
```

**Reference:** See how `ExternalRelation` stores its `List<Attribute> output` and how `Row` creates attributes via `Alias` + `mergeOutputAttributes`.

#### Step 4: Pre-Analysis

1. Add to `PreAnalyzer.PreAnalysis` record: `List<String> catEndpoints`
2. Add collection logic in `PreAnalyzer.doPreAnalyze()`:
   ```java
   List<String> catEndpoints = new ArrayList<>();
   plan.forEachUp(UnresolvedCatRelation.class, p -> catEndpoints.add(p.endpoint()));
   ```
3. Pass `catEndpoints` through to `PreAnalysis` constructor

**Reference:** Lines 72-82 of `PreAnalyzer.java` (icebergPaths collection pattern).

#### Step 5: Data Resolution in EsqlSession

1. Create `CatDataResolver.java` in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/cat/`
   - Method: `void resolve(List<String> endpoints, NodeClient client, ActionListener<CatDataResolution> listener)`
   - For each endpoint, execute the required transport actions using `NodeClient`
   - Build a `Table` using the same logic as the CAT REST handlers
   - Return `CatDataResolution` containing `Map<String, Table>` (endpoint → data)

2. Add `preAnalyzeCatSources()` method to `EsqlSession`:
   ```java
   private void preAnalyzeCatSources(
       PreAnalyzer.PreAnalysis preAnalysis,
       PreAnalysisResult result,
       ActionListener<PreAnalysisResult> listener
   ) {
       if (preAnalysis.catEndpoints().isEmpty()) {
           listener.onResponse(result);
           return;
       }
       catDataResolver.resolve(
           preAnalysis.catEndpoints(),
           client,
           listener.map(result::withCatDataResolution)
       );
   }
   ```

3. Insert into `resolveIndicesAndAnalyze()` chain (after `preAnalyzeExternalSources`):
   ```java
   .<PreAnalysisResult>andThen((l, r) -> preAnalyzeCatSources(preAnalysis, r, l))
   ```

**Reference:** Lines 1013-1034 of `EsqlSession.java` (`preAnalyzeExternalSources` pattern).

#### Step 6: Analyzer Rule

1. Add `ResolveCatRelation` rule to `Analyzer.java`:
   ```java
   private static class ResolveCatRelation
       extends ParameterizedAnalyzerRule<UnresolvedCatRelation, AnalyzerContext> {
       @Override
       protected LogicalPlan rule(UnresolvedCatRelation plan, AnalyzerContext context) {
           String endpoint = plan.endpoint();
           // Validate endpoint exists
           Optional<List<Attribute>> schema = CatSchemaRegistry.schema(endpoint);
           if (schema.isEmpty()) {
               return plan.unresolvedMessage("Unknown CAT endpoint [" + endpoint + "]");
           }
           // Get pre-resolved data
           CatDataResolution resolution = context.catDataResolution();
           Table table = resolution.get(endpoint);
           if (table == null) {
               return plan; // Still unresolved
           }
           // Convert Table to LocalRelation
           List<Attribute> attributes = schema.get();
           Page page = CatTableConverter.toPage(table, attributes);
           return new LocalRelation(plan.source(), attributes, LocalSupplier.of(page));
       }
   }
   ```

2. Register in `Analyzer` batch rules list (alongside `ResolveTable`, `ResolveExternalRelations`).

**Reference:** Lines 297-363 of `Analyzer.java` (ResolveTable rule), lines 457-483 (ResolveExternalRelations rule).

#### Step 7: Table-to-Page Converter

1. Create `CatTableConverter.java` in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/cat/`
2. Method: `static Page toPage(Table table, List<Attribute> schema)`
3. For each column in schema:
   - Get the column cells from `table.getAsMap().get(columnName)`
   - Create an appropriate `Block` (via `BlockFactory`) based on the DataType
   - Apply type coercion:
     - `ByteSizeValue` → `long` (`.getBytes()`)
     - `TimeValue` → `long` (`.millis()`)
     - `"-"` sentinel → `null`
     - `null` → `null`
     - Numeric wrappers → appropriate primitive
     - Everything else → `.toString()` for KEYWORD columns
4. Assemble blocks into a `Page`

**Reference:** `BlockUtils.fromListRow()` in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java` for block construction patterns.

#### Step 8: Data Fetching (Per Endpoint)

Extract the data-fetching logic from each CAT REST handler into reusable components. The CAT REST handlers mix REST request handling with data fetching + Table building. We need only the latter.

**Approach: Call transport actions directly (don't reuse REST handlers)**

For each endpoint, create a method in `CatDataResolver` that:
1. Creates the appropriate transport action requests
2. Calls `client.execute(ActionType, Request, ActionListener)` for each
3. Uses `RefCountingListener` to coordinate multiple async calls (pattern from `RestIndicesAction`)
4. Builds a `Table` from the responses using `getTableWithHeader()` + `buildTable()` logic

Example for `health`:
```java
private void resolveHealth(NodeClient client, ActionListener<Table> listener) {
    ClusterHealthRequest request = new ClusterHealthRequest();
    client.admin().cluster().health(request, listener.map(response -> {
        Table t = healthTableWithHeader();
        t.startRow();
        t.addCell(response.getClusterName());
        t.addCell(response.getStatus().name().toLowerCase(Locale.ROOT));
        // ... all columns
        t.endRow();
        return t;
    }));
}
```

**Key consideration:** The `buildTable()` logic in each CAT handler is tightly coupled to the handler class. Rather than extracting it, consider **duplicating the table-building logic** in `CatDataResolver`. This avoids creating a dependency from the ES|QL plugin to the REST handler classes (which are in `server` module, so accessible, but coupling is undesirable).

Alternatively, refactor the CAT handlers to expose a static `buildTable()` method that can be called from both the REST handler and the ES|QL resolver. This is cleaner but requires touching the existing CAT handler code.

#### Step 9: Verifier Updates

Add validation in `Verifier.java`:
- Ensure the CAT endpoint is a recognized name from `CatSchemaRegistry`
- Produce a clear error for unknown endpoints:
  ```
  Unknown CAT endpoint [foo]. Available: [health, indices, nodes, shards, aliases, allocation]
  ```

**Reference:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Verifier.java`

#### Step 10: Tests

1. **Unit tests:**
   - `CatSchemaRegistryTests` — verify schema definitions are complete and correct
   - `CatTableConverterTests` — test type coercion (ByteSizeValue, TimeValue, nulls, sentinels)
   - Parser tests — verify `CAT indices` parses to `UnresolvedCatRelation("indices")`

2. **Integration tests:**
   - `CatCommandIT` extending `ESIntegTestCase`
   - Create some indices, then run `CAT indices | KEEP index, health`
   - Verify correct columns and types
   - Test security: run with a user that lacks `cluster:monitor` privileges, verify clean error

3. **YAML REST tests (preferred):**
   - Create YAML test files under `x-pack/plugin/esql/qa/`
   - Test each endpoint with basic queries
   - Test column pruning (KEEP), filtering (WHERE), sorting (SORT), aggregation (STATS)

**Reference:** Existing ES|QL tests in `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/`

---

## 8. Key Files Reference

### Files to Create

| File | Purpose |
|---|---|
| `x-pack/plugin/esql/src/main/antlr/lexer/Cat.g4` | Lexer fragment for CAT keyword |
| `x-pack/plugin/esql/src/main/java/.../plan/logical/UnresolvedCatRelation.java` | Unresolved logical plan node |
| `x-pack/plugin/esql/src/main/java/.../plan/logical/CatRelation.java` | Resolved logical plan node (may not be needed for Option B) |
| `x-pack/plugin/esql/src/main/java/.../cat/CatSchemaRegistry.java` | Static schema definitions |
| `x-pack/plugin/esql/src/main/java/.../cat/CatDataResolver.java` | Async data fetching via transport actions |
| `x-pack/plugin/esql/src/main/java/.../cat/CatTableConverter.java` | Table → Page conversion with type coercion |
| `x-pack/plugin/esql/src/test/java/.../cat/CatSchemaRegistryTests.java` | Schema unit tests |
| `x-pack/plugin/esql/src/test/java/.../cat/CatTableConverterTests.java` | Converter unit tests |
| `x-pack/plugin/esql/qa/.../CatCommandIT.java` | Integration tests |

### Files to Modify

| File | Change |
|---|---|
| `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` | Add `catCommand` rule to `sourceCommand` |
| `x-pack/plugin/esql/src/main/java/.../parser/LogicalPlanBuilder.java` | Add `visitCatCommand()` |
| `x-pack/plugin/esql/src/main/java/.../analysis/PreAnalyzer.java` | Collect CAT endpoints from `UnresolvedCatRelation` |
| `x-pack/plugin/esql/src/main/java/.../analysis/Analyzer.java` | Add `ResolveCatRelation` rule |
| `x-pack/plugin/esql/src/main/java/.../analysis/Verifier.java` | Validate CAT endpoint names |
| `x-pack/plugin/esql/src/main/java/.../session/EsqlSession.java` | Add `preAnalyzeCatSources()` to resolution chain |
| `x-pack/plugin/esql/src/main/java/.../action/EsqlCapabilities.java` | Add `CAT_COMMAND` capability |
| `x-pack/plugin/esql/src/main/java/.../session/PreAnalysisResult.java` | Add `CatDataResolution` field |
| `x-pack/plugin/esql/src/main/java/.../analysis/AnalyzerContext.java` | Expose `CatDataResolution` to analyzer |

### Files for Reference (Do Not Modify)

| File | Why it's relevant |
|---|---|
| `server/.../rest/action/cat/AbstractCatAction.java` | Base class showing `getTableWithHeader()` / `doCatRequest()` pattern |
| `server/.../rest/action/cat/RestIndicesAction.java` | Example of multi-request CAT handler with `RefCountingListener` |
| `server/.../rest/action/cat/RestHealthAction.java` | Example of single-request CAT handler |
| `server/.../common/Table.java` | Table class with Cell structure (Object value, Map attr) |
| `x-pack/.../plan/logical/ExternalRelation.java` | Reference for resolved leaf plan with `ExecutesOn.Coordinator` |
| `x-pack/.../plan/logical/UnresolvedExternalRelation.java` | Reference for unresolved leaf plan |
| `x-pack/.../plan/logical/local/LocalRelation.java` | Target plan node for Option B (pre-resolved data) |
| `x-pack/.../plan/logical/local/LocalSupplier.java` | Supplier interface for LocalRelation data |
| `x-pack/.../plan/logical/Row.java` | Simple source command reference |
| `x-pack/.../optimizer/rules/logical/ReplaceRowAsLocalRelation.java` | Pattern for converting to LocalRelation |
| `x-pack/.../planner/mapper/Mapper.java` | Logical → Physical plan mapping (lines 89-98 for leaf handling) |
| `x-pack/.../planner/mapper/LocalMapper.java` | Data-node plan mapping |
| `x-pack/.../planner/mapper/MapperUtils.java` | `mapLeaf()` handles LocalRelation → LocalSourceExec |

---

## 9. Risks and Mitigations

### Risk 1: Schema Drift

CAT handler column definitions can change between Elasticsearch versions (columns added, types changed). The static `CatSchemaRegistry` must be kept in sync.

**Mitigation:** Add a test that instantiates each CAT REST handler, calls `getTableWithHeader()`, and compares the column names/count against `CatSchemaRegistry`. This fails the build if they diverge.

### Risk 2: Large Result Sets

`CAT shards` on a large cluster can return millions of rows. Loading all rows into a `LocalRelation` Page at analysis time is memory-intensive.

**Mitigation (Phase 1):** Accept this limitation. Add a warning in docs that `CAT shards` on very large clusters may consume significant memory. Consider a default row limit (e.g., 10,000 rows) that can be overridden.

**Mitigation (Phase 2):** Migrate to Option A (CatSourceExec) with streaming operator, or push LIMIT into the transport action calls.

### Risk 3: Column Name Conflicts

CAT column names with dots (`docs.count`, `store.size`) may conflict with ES|QL's field path separator. Users must use backtick-quoting: `` `docs.count` ``.

**Mitigation:** Document this requirement. Consider adding a special Verifier hint when a user references `docs` (which doesn't exist) suggesting they might mean `` `docs.count` ``.

### Risk 4: Null and Sentinel Value Handling

CAT handlers use `"-"` as a sentinel for "not available" and `null` for missing data. Some columns mix types (e.g., `max_task_wait_time` can be `TimeValue` or `"-"`).

**Mitigation:** The `CatTableConverter` must handle all these cases uniformly:
- `null` → ES|QL null
- `"-"` → ES|QL null (for all non-KEYWORD columns) or `"-"` (for KEYWORD columns)
- Type mismatches → convert to string for KEYWORD, null for numeric types

### Risk 5: Concurrent CAT Requests

If a query references multiple CAT endpoints (not supported in Phase 1, but possible in the future), the data resolver should fetch them in parallel using `RefCountingListener`.

**Mitigation:** Design `CatDataResolver.resolve()` to accept a list of endpoints and resolve them concurrently.

---

## 10. Future Enhancements (Out of Scope for Phase 1)

1. **Column aliases:** Support CAT column aliases in ES|QL (e.g., `dc` for `docs.count`)
2. **Parameterized endpoints:** `CAT indices <pattern>` to filter indices at fetch time (like `/_cat/indices/my-index-*`)
3. **Option A migration:** Replace LocalRelation with CatSourceExec for streaming execution
4. **All 22 CAT endpoints:** Phase 1 covers 6; add the remaining progressively
5. **Default column set:** Only expose "default visible" columns unless the user requests non-default ones explicitly (mirror the `?h=` parameter behavior of CAT API)
6. **LIMIT pushdown:** If a LIMIT is applied, consider passing it to the transport actions to reduce data fetched

---

## 11. Summary of Transport Action Privileges

For quick reference, these are the exact transport action name strings that must be authorized:

| Action String | Used By |
|---|---|
| `cluster:monitor/health` | CAT health |
| `cluster:monitor/state` | CAT indices, nodes, shards, allocation |
| `indices:monitor/settings/get` | CAT indices |
| `indices:monitor/stats` | CAT indices, shards |
| `cluster:monitor/nodes/info` | CAT nodes |
| `cluster:monitor/nodes/stats` | CAT nodes, allocation |
| `indices:admin/aliases/get` | CAT aliases |

These are enforced by `SecurityActionFilter` (order `Integer.MIN_VALUE` — always first in the filter chain) at `x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/action/filter/SecurityActionFilter.java`.
