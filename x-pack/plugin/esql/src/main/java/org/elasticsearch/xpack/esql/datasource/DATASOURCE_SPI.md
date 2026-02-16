# DataSource SPI Control Flow

This document describes how ES|QL integrates with external data sources through the DataSource SPI.

## Data Source Syntax

The syntax is `where:what`:
- **where** — the data source (registered name or inline `EXTERNAL(...)`)
- **what** — the query/expression (data source-specific, opaque to ES|QL)

### Registered Data Sources

Data sources registered via ES CRUD API, referenced by name:

```sql
-- Unquoted shorthand (simple identifiers)
FROM s3_logs:logs
FROM s3_logs:events
FROM my_postgres:users
FROM my_iceberg:my_catalog.my_schema.orders

-- Quoted string (patterns, paths, SQL queries)
FROM s3_logs:"logs/*.parquet"
FROM s3_logs:"2024/01/*/events.parquet"
FROM my_postgres:"SELECT * FROM users WHERE active = true"

-- Query pseudo-function (equivalent to quoted)
FROM s3_logs:query("logs/*.parquet")
FROM my_postgres:query("SELECT * FROM users WHERE active = true")
```

### Inline Data Sources

Data sources defined inline using `EXTERNAL()` with a raw JSON literal:

```sql
-- EXTERNAL with unquoted expression
FROM EXTERNAL({"type": "s3", "configuration": {"bucket": "my-bucket", "access_key": "AKIA...", "secret_key": "..."}, "settings": {}}):logs

-- EXTERNAL with quoted expression
FROM EXTERNAL({"type": "s3", "configuration": {"bucket": "my-bucket", "access_key": "AKIA...", "secret_key": "..."}, "settings": {"max_files": 100}}):"logs/*.parquet"

-- EXTERNAL with query function
FROM EXTERNAL({"type": "postgres", "configuration": {"host": "db.example.com", "port": 5432, "database": "mydb", "username": "...", "password": "..."}, "settings": {}}):query("SELECT * FROM users")
```

### EXTERNAL JSON Structure

```json
{
  "type": "s3",
  "configuration": {
    "bucket": "my-bucket",
    "access_key": "AKIA...",
    "secret_key": "..."
  },
  "settings": {
    "max_files": 100
  }
}
```

| Field | Owner | Purpose |
|-------|-------|---------|
| `type` | ES | DataSource type to use |
| `configuration` | DataSource | Connection, auth, data source-specific |
| `settings` | ES | ES-controlled behavior settings |

### Expression Forms

| Form | Syntax | Use Case |
|------|--------|----------|
| Unquoted | `users` | Simple table names, identifiers |
| Unquoted dotted | `catalog.schema.table` | Qualified names |
| Quoted | `"logs/*.parquet"` | Patterns, paths, special characters |
| Query function | `query("SELECT ...")` | Explicit, equivalent to quoted |

---

## Core Abstractions

These are the types that define the DataSource SPI contract. Every data source must interact with them.

| Type | Description |
|------|-------------|
| [DataSource](DataSource.java) | Main SPI interface with all lifecycle hooks (including `capabilities()`) |
| [DataSourcePlan](DataSourcePlan.java) | Abstract base class for data source plan leaves (extends LeafPlan) |
| [DataSourceDescriptor](DataSourceDescriptor.java) | Parsed data source reference (type, configuration, settings, expression) |
| [DataSourcePartition](DataSourcePartition.java) | Interface for units of work in distributed execution |
| [DataSourceCapabilities](DataSourceCapabilities.java) | Execution mode flag (distributed vs coordinator-only), returned by `DataSource.capabilities()` |
| [DataSourceExec](DataSourceExec.java) | Physical plan node for all data sources (wraps DataSourcePlan) |

### Partitioning (`datasource/partitioning/`)

Types for the split → partition pipeline. Used by distributed data sources that discover
fine-grained units of work (files, key ranges, shards) and group them into balanced partitions.

| Type | Description |
|------|-------------|
| [SplitPartitioner](partitioning/SplitPartitioner.java) | Reusable discover → group → wrap helper for split-based partitioning (composed by data sources) |
| [DataSourceSplit](partitioning/DataSourceSplit.java) | Interface for discovered units of work with optional size/affinity |
| [NodeAffinity](partitioning/NodeAffinity.java) | Value type for hard (`require`) vs soft (`prefer`) node affinity |
| [DistributionHints](partitioning/DistributionHints.java) | Hints for partitioning (target parallelism, available nodes) |
| [SizeAwareBinPacking](partitioning/SizeAwareBinPacking.java) | Static utility for FFD bin-packing of splits (used by `SplitPartitioner`) |

### Helpers

Convenience classes built on top of the core abstractions. Data sources are free to use them or implement
the core abstractions directly.

| Type | Description |
|------|-------------|
| [DataSourcePushdownRule](DataSourcePushdownRule.java) | Convenience base for optimization rules that push operations into plan leaves |
| [DataSourceOptimizer](DataSourceOptimizer.java) | Collects and runs data source-provided optimization rules |

### Base Classes: Lakehouse

Reusable building blocks for lakehouse data sources. Data sources can extend these or implement the
core abstractions directly.

| Type | Description |
|------|-------------|
| [LakehouseDataSource](lakehouse/LakehouseDataSource.java) | Base for lakehouse data sources (composes [`SplitPartitioner`](partitioning/SplitPartitioner.java)`<FileTask>`, storage + format SPI types) |
| [LakehousePlan](lakehouse/LakehousePlan.java) | Abstract plan class with filter/limit support |

### Lakehouse SPI

Production-ready abstractions for lakehouse storage, format reading, table catalog integration,
and filter pushdown. Used by `LakehouseDataSource` and available for direct use by data sources.

**Storage access:**

| Type | Description |
|------|-------------|
| [StoragePath](lakehouse/StoragePath.java) | URI-like path for addressing objects in storage systems (scheme://host[:port]/path) |
| [StorageProvider](lakehouse/StorageProvider.java) | SPI for accessing files in a storage system (S3, GCS, HDFS) |
| [StorageObject](lakehouse/StorageObject.java) | Read handle for a single object (sync + async) |
| [StorageEntry](lakehouse/StorageEntry.java) | Metadata record from directory listing (path, length, lastModified) |
| [StorageIterator](lakehouse/StorageIterator.java) | Storage-specific iterator over entries (extends CloseableIterator) |
| [StorageProviderFactory](lakehouse/StorageProviderFactory.java) | Factory for creating StorageProvider instances |

**Format reading:**

| Type | Description |
|------|-------------|
| [FormatReader](lakehouse/FormatReader.java) | SPI for reading file formats (Parquet, ORC, CSV, Avro) |
| [FormatReaderFactory](lakehouse/FormatReaderFactory.java) | Factory for creating FormatReader instances |
| [CloseableIterator](lakehouse/CloseableIterator.java) | Generic closeable iterator for streaming data pages |

**Metadata:**

| Type | Description |
|------|-------------|
| [SourceMetadata](lakehouse/SourceMetadata.java) | Schema, location, and optional statistics |
| [SimpleSourceMetadata](lakehouse/SimpleSourceMetadata.java) | Immutable SourceMetadata implementation with builder |
| [SourceStatistics](lakehouse/SourceStatistics.java) | Row count, size, and per-column statistics |

**Filter pushdown:**

| Type | Description |
|------|-------------|
| [FilterPushdownSupport](lakehouse/FilterPushdownSupport.java) | Push filter expressions to the data source |

**Table catalog:**

| Type | Description |
|------|-------------|
| [TableCatalog](lakehouse/TableCatalog.java) | Integration with table formats (Iceberg, Delta Lake, Hudi) |
| [TableCatalogFactory](lakehouse/TableCatalogFactory.java) | Factory for creating TableCatalog instances |

**Operator creation:**

| Type | Description |
|------|-------------|
| [SourceOperatorFactoryProvider](lakehouse/SourceOperatorFactoryProvider.java) | Extension point for custom source operator creation |
| [SourceOperatorContext](lakehouse/SourceOperatorContext.java) | Context record for operator factory creation |

### Base Classes: SQL

| Type | Description |
|------|-------------|
| [SqlDataSource](sql/SqlDataSource.java) | Base for PostgreSQL, MySQL, Oracle data sources |
| [SqlPlan](sql/SqlPlan.java) | Abstract plan class with filter/limit/orderBy/aggregation support |

### Examples

| Type | Description |
|------|-------------|
| [JdbcDataSource](sql/JdbcDataSource.java) | SQL example: reads from a database via JDBC |
| [JdbcPlan](sql/JdbcPlan.java) | Plan node holding table name and built SQL |

### Core SPI: Component Composition

How core SPI concepts are used in each query phase:

```
  Phase              DataSource method                   Key concepts
  ═════              ════════════════                   ════════════

  RESOLVE            DataSource.resolve()
                     DataSourceDescriptor ─────────────────────► DataSourcePlan
                     (FROM clause: type + config + expression)        (schema + source info)

  OPTIMIZE           DataSource.optimizationRules()
                     DataSourcePushdownRule ──► pushDown(plan) ──────► DataSourcePlan'
                     (fold Filter/Limit/etc. into plan leaf)          (with pushed-down ops)

  PHYSICAL PLAN      DataSource.createPhysicalPlan()
                     DataSourcePlan' ────────────────────────────────► DataSourceExec
                     (optimized logical plan)                         (physical wrapper)

  PARTITION          DataSource.planPartitions()
                     DataSourceExec + DistributionHints ─────────────► DataSourcePartition[]
                     (plan + hints)                                   (units of parallel work)

  EXECUTE            DataSource.createSourceOperator()
                     DataSourcePartition ────────────────────────────► SourceOperator
                     (one unit of work)                               (produces Pages)
```

`capabilities()` determines which phases run — coordinator-only data sources
skip PARTITION (single `CoordinatorPartition` is used instead).

---

## Design Principles

**Minimal core SPI:** The core [`DataSourcePlan`](DataSourcePlan.java) abstract class only contains what's universal:
- `dataSource()` - identity
- `location()` - for error messages
- `output()` - schema

**Operation-specific state in base classes:** Operation handling is defined in base class abstract plans:
- [`LakehousePlan`](lakehouse/LakehousePlan.java) - filter + limit
- [`SqlPlan`](sql/SqlPlan.java) - full SQL pushdown

**Opaque expressions:** The expression after `:` is interpreted entirely by the data source. ES|QL passes it through without parsing or validation.

**Storage/Format separation:** Data lake data sources compose [`StorageProvider`](lakehouse/StorageProvider.java) (file access) with [`FormatReader`](lakehouse/FormatReader.java) (file reading) from the [`lakehouse`](lakehouse/) package. This allows any storage (S3, GCS, HDFS) to be combined with any format (Parquet, ORC, CSV). Optionally, a [`TableCatalog`](lakehouse/TableCatalog.java) provides catalog integration for table-based sources.

---

## Query Execution Phases

```
┌─────────────┐    ┌──────────┐    ┌─────────────────────┐    ┌───────────────┐    ┌────────┐    ┌─────────────┐    ┌───────────┐
│   Parser    │───>│PreAnalyzer│───>│ LogicalPlanOptimizer│───>│DataSourceOptim│───>│ Mapper │───>│  Physical   │───>│ Execution │
│             │    │          │    │                     │    │     izer      │    │        │    │  Planner    │    │           │
└─────────────┘    └──────────┘    └─────────────────────┘    └───────────────┘    └────────┘    └─────────────┘    └───────────┘
                        │                                            │                  │              │                  │
                   ┌────▼────┐                                  ┌────▼────┐        ┌────▼────┐    ┌────▼────┐       ┌────▼────┐
                   │ resolve │                                  │optimize │        │createPhy│    │ plan    │       │createSrc│
                   │         │                                  │Rules()  │        │sicalPlan│    │Partitions│      │Operator │
                   └─────────┘                                  └─────────┘        └─────────┘    └─────────┘       └─────────┘
                   DataSource                                    DataSource         DataSource     DataSource       DataSource
```

### Phase 1: Resolution (PreAnalyzer)

**What happens:** PreAnalyzer encounters an external source reference and needs schema.

**DataSource method:** [`DataSource.resolve()`](DataSource.java#L111) returns a [`DataSourcePlan`](DataSourcePlan.java)

**Input:** [`DataSourceDescriptor`](DataSourceDescriptor.java) containing:
- `type()` - data source type (e.g., "s3", "postgres")
- `configuration()` - connection/auth config (opaque to ES)
- `settings()` - ES-controlled settings
- `expression()` - what to resolve (table, pattern, query)

**Flow:**
1. PreAnalyzer sees `FROM s3_logs:"logs/*.parquet"` (or `s3_logs:logs` for shorthand)
2. Looks up registered data source `s3_logs`, gets type/configuration/settings
3. Creates [`DataSourceDescriptor`](DataSourceDescriptor.java) with expression `logs/*.parquet` (parser strips quotes)
4. Looks up data source by type, calls [`dataSource.resolve()`](DataSource.java#L111)
5. Data source interprets expression, reads schema, returns plan node

**Data lake resolution:** [`LakehouseDataSource.resolve()`](lakehouse/LakehouseDataSource.java) provides a default:
1. If `getTableCatalog()` returns a catalog that `canHandle(expression)`:
   - `catalog.metadata(expression, config)` — resolve schema from catalog
2. Otherwise (raw file fallback):
   - `storage.newObject(StoragePath.of(expression))` — open the file
   - `format.metadata(object)` — read schema from file metadata
3. `createPlan(...)` — build data source-specific plan node

### Phase 2: Logical Optimization (DataSourceOptimizer)

**What happens:** DataSource-provided rules push operations into data source plan leaves.

**DataSource method:** [`DataSource.optimizationRules()`](DataSource.java) returns `List<Rule<?, LogicalPlan>>`

**How it works:**
[`DataSource.applyOptimizationRules()`](DataSource.java) runs as a **separate pass** after the main `LogicalPlanOptimizer`:
1. Collects [`optimizationRules()`](DataSource.java) from all registered data sources
2. Runs the collected rules as a single batch (`Limiter.ONCE`)
3. Each rule pattern-matches on plan nodes it handles and skips nodes from other data sources

**Rule pattern:** Each rule pattern-matches on a standard ES|QL plan node (e.g., `Filter`, `Limit`) whose child is the data source's plan leaf. If the operation is translatable, the rule folds it into the leaf and removes the parent node. If not, the plan stays unchanged and ES|QL evaluates the operation. The [`DataSourcePushdownRule`](DataSourcePushdownRule.java) helper class handles the common guard logic (child type check + data source identity check) so rules only need to implement `pushDown()`.

**Base class rules:**
- [`LakehouseDataSource`](lakehouse/LakehouseDataSource.java): No rules by default (opt-in via `pushFilterRule()`, `pushLimitRule()`)
- [`SqlDataSource`](sql/SqlDataSource.java): `PushFilterToSql`, `PushLimitToSql`, `PushOrderByToSql`, `PushAggregateToSql`, `BuildSql`

### Phase 3: Physical Planning (Mapper)

**DataSource method:** [`DataSource.createPhysicalPlan()`](DataSource.java)

**What happens:** The `Mapper` converts the data source's logical plan node into a physical plan node.

**Default implementation:** `DataSource` provides a default `createPhysicalPlan()` that creates a
[`DataSourceExec`](DataSourceExec.java) wrapping the fully-optimized `DataSourcePlan`. This is shared by
all data source types — the data source-specific state (filters, SQL, file lists) lives inside the
`DataSourcePlan`, so no data source-specific physical plan node is needed.

Data sources can override `createPhysicalPlan()` if they need custom physical planning behavior.

### Phase 4: Work Distribution (distributed data sources only)

**DataSource method:** [`DataSource.planPartitions()`](DataSource.java#L171)

Only called if [`capabilities().distributed()`](DataSourceCapabilities.java#L39) is true.

**Three-phase pattern (discover → group → wrap):**
Cross-engine research (Spark, Trino, Flink) reveals a universal pipeline for work distribution.
Discovery is source-specific (files, shards, scan tasks), but grouping is reusable.
[`SplitPartitioner`](partitioning/SplitPartitioner.java) encapsulates this pipeline: data sources provide
discover, group, and wrap functions via its constructor.
The default grouping delegates to [`SizeAwareBinPacking`](partitioning/SizeAwareBinPacking.java)
(FFD algorithm with count-based round-robin fallback), so data sources only need
to provide discovery and wrapping. Splits implement [`DataSourceSplit`](partitioning/DataSourceSplit.java)
to provide optional size estimates and [`NodeAffinity`](partitioning/NodeAffinity.java).

**Node affinity** distinguishes two access patterns:
- **Required** ([`NodeAffinity.require`](partitioning/NodeAffinity.java)) — data only exists on one node (local filesystem, node-local resources). Always enforced; required-affinity splits from different nodes are never mixed.
- **Preferred** ([`NodeAffinity.prefer`](partitioning/NodeAffinity.java)) — data is faster to read locally but accessible from anywhere (HDFS replicas, S3 with warm cache). Honored when [`DistributionHints.preferDataLocality()`](partitioning/DistributionHints.java) is true; ignored otherwise.

### Phase 5: Execution (LocalExecutionPlanner)

**DataSource method:** [`DataSource.createSourceOperator()`](DataSource.java#L190)

---

## Lakehouse Architecture: Storage + Format Separation

The [`LakehouseDataSource`](lakehouse/LakehouseDataSource.java) is built on a pluggable architecture using types from
the [`lakehouse`](lakehouse/) package that separates storage access from format reading:

| Component | Responsibility | Examples |
|-----------|---------------|----------|
| [StorageProvider](lakehouse/StorageProvider.java) | Access files in storage | S3, GCS, HDFS, local FS |
| [FormatReader](lakehouse/FormatReader.java) | Read file formats | Parquet, ORC, CSV, Avro |
| [StorageObject](lakehouse/StorageObject.java) | Read handle for a single object | Sync + async I/O |
| [TableCatalog](lakehouse/TableCatalog.java) | (Optional) Table catalog integration | Iceberg, Delta Lake, Hudi |
| [FilterPushdownSupport](lakehouse/FilterPushdownSupport.java) | (Optional) Filter pushdown | Partition pruning, row-group filtering |

### Lakehouse: Component Composition

How lakehouse SPI concepts are used in each query phase:

```
  Phase              Components used                    Data flow
  ═════              ═══════════════                    ═════════

  RESOLVE            TableCatalog (if available)
                       catalog.canHandle() ──► catalog.metadata() ─────► SourceMetadata
                     StorageProvider + FormatReader (fallback)              (schema + stats)
                       storage.newObject() ──► format.metadata() ──────► SourceMetadata

  OPTIMIZE           FilterPushdownSupport (opt-in)
                       fps.pushFilters(expressions) ───────────────────► pushed + remainder
                                                                          (split filters)

  PARTITION          StorageProvider + SplitPartitioner.planPartitions()
                       storage.listObjects() ──► StorageEntry[] ───────► FileTask[]
                       partitioner.planPartitions(plan, hints) ────────► DataSourcePartition[]

  EXECUTE            StorageProvider + FormatReader
                       storage.newObject(path) ──► StorageObject
                       format.read(object, columns, filter) ──────────► Pages
```

**StorageProvider** and **FormatReader** participate in every phase — resolution, partitioning,
and execution. **TableCatalog** and **FilterPushdownSupport** are optional and add catalog
integration and filter pushdown for formats that support them (e.g., Iceberg).

### Combinations

Any storage can be paired with any format:

| Use Case | Storage | Format | Notes |
|----------|---------|--------|-------|
| Raw Parquet on S3 | S3 | Parquet | Direct file access |
| Iceberg tables | S3/GCS/HDFS | Parquet/ORC | Override `resolve()` for catalog |
| Delta Lake | S3/Azure Blob | Parquet | Override `resolve()` for catalog |
| Local CSV | Local FS | CSV | Development/testing |
| ORC on HDFS | HDFS | ORC | Hadoop ecosystem |

### How Storage + Format Are Used in Each Phase

| Phase | Component | What Happens |
|-------|-----------|-------------|
| **Resolution** | TableCatalog or StorageProvider + FormatReader | Catalog resolves schema, or `storage.newObject()` + `format.metadata()` infers schema |
| **Optimization** | FilterPushdownSupport | `fps.pushFilters()` determines which filters can be pushed to the source |
| **Partitioning** | StorageProvider + `SplitPartitioner` | `storage.listObjects()` lists files as `FileTask`s, partitioner bin-packs by size |
| **Execution** | StorageProvider + FormatReader | `storage.newObject()` opens files, `format.read()` produces Pages |

All phases have default implementations in the base class. Subclasses override when needed
(e.g., Iceberg overrides `resolve()` and `getFileTasks()` to use its catalog).

### Resolution Flow

**With catalog (Iceberg, Delta Lake) — via `getTableCatalog()`:**
1. `catalog.canHandle(expression)` — check if catalog manages this source
2. `catalog.metadata(expression, config)` — resolve `SourceMetadata` from catalog
3. `createPlan(...)` — build plan node with catalog-provided schema

**Without catalog (raw files) — fallback:**
1. `storage.newObject(StoragePath.of(expression))` — open the file
2. `format.metadata(object)` — read `SourceMetadata` from file metadata
3. `createPlan(...)` — build plan node with file-inferred schema

### Optimization Rules (Opt-In)

The base class provides **no optimization rules by default**. Not all lakehouse formats
support filter or limit pushdown. Subclasses opt in by overriding `optimizationRules()`:

```java
@Override
public List<Rule<?, LogicalPlan>> optimizationRules() {
    return List.of(pushFilterRule(), pushLimitRule());
}
```

- `pushFilterRule()` — uses `FilterPushdownSupport` from `getFilterPushdownSupport()`
  to determine which filters can be pushed. Handles full, partial, and no pushdown.
- `pushLimitRule()` — pushes limit into the plan node.

Subclasses using these rules must also implement `applyFilter()` and/or `applyLimit()`,
and override `getFilterPushdownSupport()` for filter pushdown.

### Filter Pushdown

Filter pushdown uses the [`FilterPushdownSupport`](lakehouse/FilterPushdownSupport.java) interface,
which provides a clean separation between ES|QL's filter expressions and source-native filters:

1. `pushFilterRule()` calls `getFilterPushdownSupport().pushFilters(filters)`
2. The implementation examines each expression and returns a `PushdownResult`:
   - `pushedFilter` — opaque source-native filter (null if nothing pushed)
   - `remainder` — expressions that couldn't be pushed (empty if all pushed)
3. The rule calls `applyFilter(plan, pushedFilter)` to store the pushed filter in the plan
4. Any remainder stays in the ES|QL `Filter` node for evaluation

This replaces the previous `FilterTranslation` approach with a more flexible model inspired by
Lucene's `translatable()` pattern and Spark's `SupportsPushDownFilters`.

### Partitioning Flow

Default `planPartitions()` implementation (discover → group → wrap via `SplitPartitioner`):
1. **discover:** calls `storage.listObjects(StoragePath)`, wraps each `StorageEntry` as a `FileTask`
2. **group:** size-aware bin-packing via `SizeAwareBinPacking` — respects [`NodeAffinity`](partitioning/NodeAffinity.java) (required splits grouped strictly by node, preferred splits grouped by node when `preferDataLocality` is true), FFD bin-packing when file sizes are available, count-based round-robin fallback
3. **wrap:** creates `DataSourcePartition` with aggregated size estimates

Subclasses with catalogs override `getFileTasks()` to use catalog file manifests
(which provide partition pruning and richer metadata like row counts).

---

## Example: JDBC Database

> Full implementation: [JdbcDataSource](sql/JdbcDataSource.java) + [JdbcPlan](sql/JdbcPlan.java)

```sql
-- Simple table reference
FROM my_db:users | WHERE age > 21 | SORT name | LIMIT 100

-- Native SQL query
FROM my_db:query("SELECT * FROM users WHERE active = true") | STATS avg(salary) BY dept
```

Where `my_db` is registered as:
```json
{
  "type": "jdbc",
  "configuration": {
    "url": "jdbc:mysql://db.example.com:3306/mydb",
    "username": "...",
    "password": "..."
  },
  "settings": {}
}
```

### SQL: Component Composition

How SQL SPI concepts are used in each query phase:

```
  Phase              Components used                    Data flow
  ═════              ═══════════════                    ═════════

  RESOLVE            SqlDataSource (database driver)
                       connect + read schema ──────────────────────────► SqlPlan
                                                                         (table + columns)

  OPTIMIZE           PushFilterToSql ──► translateFilter() ────────────► SqlPlan.filter
                     PushLimitToSql ───────────────────────────────────► SqlPlan.limit
                     PushOrderByToSql ──► translateOrderBy() ──────────► SqlPlan.orderBy
                     PushAggregateToSql ──► translateAggregates() ─────► SqlPlan.aggregation
                     BuildSql ──► buildSql(plan) ──────────────────────► SqlPlan.builtSql
                                                                         (complete SQL string)

  EXECUTE            SqlDataSource (coordinator-only)
                       execute(builtSql) ──────────────────────────────► Pages
                       (single query, no partitioning)                   (results)
```

Each optimization rule removes one ES|QL plan node and accumulates the operation into `SqlPlan`.
The final `BuildSql` rule translates all accumulated state into a single SQL string. There is no
PARTITION phase — SQL data sources are coordinator-only.

### Key classes

- [JdbcPlan](sql/JdbcPlan.java) — extends `SqlPlan`. Holds table name (or subquery), built SQL, filter, limit, orderBy, and aggregation. The `builtSql` field is populated by the `BuildSql` rule after all operations are translated.

- [JdbcDataSource](sql/JdbcDataSource.java) — extends `SqlDataSource`. Resolves table names or native SQL queries via JDBC, translates ES|QL operations to SQL, and executes on the coordinator. JDBC calls are stubbed with comments showing the real implementation.

### Query Flow (table reference)

| Phase | What Happens | Method |
|-------|--------------|--------|
| **Parse** | `FROM my_db:users \| WHERE age > 21 \| SORT name \| LIMIT 100` | |
| **Resolve** | Connect via JDBC, read `users` table schema | [`resolve()`](sql/JdbcDataSource.java) |
| **Optimize** | Push filter `age > 21` → SQL WHERE clause | [`translateFilter()`](sql/JdbcDataSource.java) |
| **Optimize** | Push ORDER BY `name` → SQL ORDER BY clause | [`translateOrderBy()`](sql/JdbcDataSource.java) |
| **Optimize** | Push limit 100 → SQL LIMIT clause | `PushLimitToSql` rule |
| **Optimize** | Build SQL: `SELECT name, age FROM users WHERE age > 21 ORDER BY name LIMIT 100` | `BuildSql` rule |
| **Execute** | Coordinator runs single JDBC query | [`createSourceOperator()`](sql/JdbcDataSource.java) |

### Query Flow (native SQL)

| Phase | What Happens | Method |
|-------|--------------|--------|
| **Parse** | `FROM my_db:query("SELECT * FROM users WHERE active = true") \| STATS avg(salary) BY dept` | |
| **Resolve** | Detect SQL query, wrap as subquery `(SELECT ...) AS _subq`, read schema | [`resolve()`](sql/JdbcDataSource.java) |
| **Optimize** | Push aggregation → SQL `SELECT dept, AVG(salary) FROM (...) GROUP BY dept` | [`translateAggregates()`](sql/JdbcDataSource.java) |
| **Execute** | Coordinator runs single JDBC query | |

---

## Key Differences: Lakehouse vs SQL

| Aspect | [LakehouseDataSource](lakehouse/LakehouseDataSource.java) | [SqlDataSource](sql/SqlDataSource.java) |
|--------|---------|------------|
| **Type examples** | iceberg, delta, hudi, parquet | postgres, mysql, oracle |
| **Architecture** | Composes StorageProvider + FormatReader | Monolithic SQL translation |
| **Expression** | File pattern, catalog path | Table name, SQL query |
| **Plan base class** | [`LakehousePlan`](lakehouse/LakehousePlan.java) | [`SqlPlan`](sql/SqlPlan.java) |
| **Operations** | Filter, limit | Filter, limit, ORDER BY, aggregation |
| **Execution** | Distributed across data nodes | Coordinator only |
| **Partitioning** | By files | Single partition |
| **Example** | Cluster Logs ([design](EXAMPLE_DATASOURCES.md#2-cluster-logs-logs)) | [JdbcDataSource](sql/JdbcDataSource.java) |

---

## Built-In Example Data Sources

Two data sources that run entirely within the cluster, demonstrating the full SPI without external dependencies.
See [EXAMPLE_DATASOURCES.md](EXAMPLE_DATASOURCES.md) for detailed design.

```
DataSource (interface) — capabilities()
├── LakehouseDataSource — distributed(), composes partitioning.SplitPartitioner
│   └── [logs data source]
└── SqlDataSource — coordinatorOnly()
    └── JdbcDataSource (example JDBC implementation)
```

| Data Source | Base Class | Capabilities | Purpose |
|-------------|-----------|-------------|---------|
| `cluster` (nodes/indices/shards) | (implements DataSource) | `coordinatorOnly()` | Queryable cluster metadata |
| `logs` (ES JSON log files) | `LakehouseDataSource` | `distributed()` (via `LakehouseDataSource`) | Queryable cluster-wide log files |
