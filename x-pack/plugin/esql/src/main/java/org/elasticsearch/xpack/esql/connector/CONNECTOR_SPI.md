# Connector SPI Control Flow

This document describes how ES|QL integrates with external data sources through the Connector SPI.

## Data Source Syntax

The syntax is `where:what`:
- **where** — the data source (registered name or inline `EXTERNAL(...)`)
- **what** — the query/expression (connector-specific, opaque to ES|QL)

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
| `type` | ES | Connector type to use |
| `configuration` | Connector | Connection, auth, connector-specific |
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

These are the types that define the Connector SPI contract. Every connector must interact with them.

| Type | Description |
|------|-------------|
| [Connector](Connector.java) | Main SPI interface with all lifecycle hooks |
| [ConnectorPlan](ConnectorPlan.java) | Abstract base class for connector plan leaves (extends LeafPlan) |
| [ConnectorSourceDescriptor](ConnectorSourceDescriptor.java) | Parsed data source reference (type, configuration, settings, expression) |
| [ConnectorPartition](ConnectorPartition.java) | Interface for units of work in distributed execution |
| [ConnectorCapabilities](ConnectorCapabilities.java) | Declares execution mode (distributed vs coordinator-only) |
| [ConnectorExec](ConnectorExec.java) | Physical plan node for all connectors (wraps ConnectorPlan) |
| [DistributionHints](DistributionHints.java) | Hints for partitioning (target parallelism, available nodes) |

### Helpers

Convenience classes built on top of the core abstractions. Connectors are free to use them or implement
the core abstractions directly.

| Type | Description |
|------|-------------|
| [ConnectorPushdownRule](ConnectorPushdownRule.java) | Convenience base for optimization rules that push operations into plan leaves |
| [ConnectorOptimizer](ConnectorOptimizer.java) | Collects and runs connector-provided optimization rules |
| [CoordinatorPartition](CoordinatorPartition.java) | Coordinator-only partition (never serialized across nodes) |

### Base Classes: Lakehouse

Reusable building blocks for lakehouse connectors. Connectors can extend these or implement the
core abstractions directly.

| Type | Description |
|------|-------------|
| [LakehouseConnector](lakehouse/LakehouseConnector.java) | Base for lakehouse connectors (composes storage + format SPI types) |
| [LakehousePlan](lakehouse/LakehousePlan.java) | Abstract plan class with filter/limit support |

### Lakehouse SPI

Production-ready abstractions for lakehouse storage, format reading, table catalog integration,
and filter pushdown. Used by `LakehouseConnector` and available for direct use by connectors.

**Storage access:**

| Type | Description |
|------|-------------|
| [StoragePath](lakehouse/StoragePath.java) | URI-like path for addressing objects in storage systems (scheme://host[:port]/path) |
| [StorageProvider](lakehouse/StorageProvider.java) | SPI for accessing files in a storage system (S3, GCS, HDFS) |
| [StorageObject](lakehouse/StorageObject.java) | Read handle for a single object (sync + async) |
| [StorageEntry](lakehouse/StorageEntry.java) | Metadata record from directory listing (path, length, lastModified) |
| [StorageIterator](lakehouse/StorageIterator.java) | Closeable iterator over storage entries |
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
| [SqlConnector](sql/SqlConnector.java) | Base for PostgreSQL, MySQL, Oracle connectors |
| [SqlPlan](sql/SqlPlan.java) | Abstract plan class with filter/limit/orderBy/aggregation support |

### Examples

| Type | Description |
|------|-------------|
| [JdbcConnector](sql/JdbcConnector.java) | SQL example: reads from a database via JDBC |
| [JdbcPlan](sql/JdbcPlan.java) | Plan node holding table name and built SQL |

### Architecture Overview

```
                              ┌──────────────────────────────────────────────┐
                              │              Core SPI (connector/)           │
                              │                                              │
                              │  Connector ─── ConnectorPlan ─── ConnectorExec
                              │      │              │                        │
                              │      │         ConnectorPartition            │
                              │      │                                       │
                              │  ConnectorSourceDescriptor                   │
                              │  ConnectorCapabilities                       │
                              │  ConnectorPushdownRule                       │
                              └──────────────┬──────────────┬────────────────┘
                                             │              │
                    ┌────────────────────────┐ ┌────────────────────────────┐
                    │  Lakehouse (lakehouse/) │ │      SQL (sql/)            │
                    │                        │ │                            │
                    │  LakehouseConnector     │ │  SqlConnector              │
                    │  LakehousePlan          │ │  SqlPlan                   │
                    │                        │ │                            │
                    │  StorageProvider ───┐   │ │  SqlBuilder                │
                    │  FormatReader ──────┤   │ │  PushFilter/Limit/OrderBy  │
                    │  TableCatalog ──────┤   │ │  PushAggregate + BuildSql  │
                    │  FilterPushdown ────┘   │ │                            │
                    └────────────┬───────────┘ └─────────────┬──────────────┘
                                 │                           │
                    ┌────────────┴───────────┐ ┌─────────────┴──────────────┐
                    │  Concrete connectors    │ │  Concrete connectors       │
                    │                        │ │                            │
                    │  IcebergConnector       │ │  JdbcConnector             │
                    │  DeltaLakeConnector     │ │  PostgresConnector         │
                    │  ParquetConnector       │ │  OracleConnector           │
                    └────────────────────────┘ └────────────────────────────┘
```

**Core SPI** defines the contract every connector must follow. **Lakehouse** and **SQL** are two
independent tracks built on top, each providing reusable base classes and optimization rules.
Concrete connectors extend one track and fill in the connector-specific details.

---

## Design Principles

**Minimal core SPI:** The core [`ConnectorPlan`](ConnectorPlan.java) abstract class only contains what's universal:
- `connector()` - identity
- `location()` - for error messages
- `output()` - schema

**Operation-specific state in base classes:** Operation handling is defined in base class abstract plans:
- [`LakehousePlan`](lakehouse/LakehousePlan.java) - filter + limit
- [`SqlPlan`](sql/SqlPlan.java) - full SQL pushdown

**Opaque expressions:** The expression after `:` is interpreted entirely by the connector. ES|QL passes it through without parsing or validation.

**Storage/Format separation:** Data lake connectors compose [`StorageProvider`](lakehouse/StorageProvider.java) (file access) with [`FormatReader`](lakehouse/FormatReader.java) (file reading) from the [`lakehouse`](lakehouse/) package. This allows any storage (S3, GCS, HDFS) to be combined with any format (Parquet, ORC, CSV). Optionally, a [`TableCatalog`](lakehouse/TableCatalog.java) provides catalog integration for table-based sources.

---

## Query Execution Phases

```
┌─────────────┐    ┌──────────┐    ┌─────────────────────┐    ┌───────────────┐    ┌────────┐    ┌─────────────┐    ┌───────────┐
│   Parser    │───>│PreAnalyzer│───>│ LogicalPlanOptimizer│───>│ConnectorOptimiz│───>│ Mapper │───>│  Physical   │───>│ Execution │
│             │    │          │    │                     │    │      er       │    │        │    │  Planner    │    │           │
└─────────────┘    └──────────┘    └─────────────────────┘    └───────────────┘    └────────┘    └─────────────┘    └───────────┘
                        │                                            │                  │              │                  │
                   ┌────▼────┐                                  ┌────▼────┐        ┌────▼────┐    ┌────▼────┐       ┌────▼────┐
                   │ resolve │                                  │optimize │        │createPhy│    │ plan    │       │createSrc│
                   │         │                                  │Rules()  │        │sicalPlan│    │Partitions│      │Operator │
                   └─────────┘                                  └─────────┘        └─────────┘    └─────────┘       └─────────┘
                   Connector                                     Connector          Connector      Connector        Connector
```

### Phase 1: Resolution (PreAnalyzer)

**What happens:** PreAnalyzer encounters an external source reference and needs schema.

**Connector method:** [`Connector.resolve()`](Connector.java#L111) returns a [`ConnectorPlan`](ConnectorPlan.java)

**Input:** [`ConnectorSourceDescriptor`](ConnectorSourceDescriptor.java) containing:
- `type()` - connector type (e.g., "s3", "postgres")
- `configuration()` - connection/auth config (opaque to ES)
- `settings()` - ES-controlled settings
- `expression()` - what to resolve (table, pattern, query)

**Flow:**
1. PreAnalyzer sees `FROM s3_logs:"logs/*.parquet"` (or `s3_logs:logs` for shorthand)
2. Looks up registered data source `s3_logs`, gets type/configuration/settings
3. Creates [`ConnectorSourceDescriptor`](ConnectorSourceDescriptor.java) with expression `logs/*.parquet` (parser strips quotes)
4. Looks up connector by type, calls [`connector.resolve()`](Connector.java#L111)
5. Connector interprets expression, reads schema, returns plan node

**Data lake resolution:** [`LakehouseConnector.resolve()`](lakehouse/LakehouseConnector.java) provides a default:
1. If `getTableCatalog()` returns a catalog that `canHandle(expression)`:
   - `catalog.metadata(expression, config)` — resolve schema from catalog
2. Otherwise (raw file fallback):
   - `storage.newObject(StoragePath.of(expression))` — open the file
   - `format.metadata(object)` — read schema from file metadata
3. `createPlan(...)` — build connector-specific plan node

### Phase 2: Logical Optimization (ConnectorOptimizer)

**What happens:** Connector-provided rules push operations into connector plan leaves.

**Connector method:** [`Connector.optimizationRules()`](Connector.java) returns `List<Rule<?, LogicalPlan>>`

**How it works:**
[`Connector.applyOptimizationRules()`](Connector.java) runs as a **separate pass** after the main `LogicalPlanOptimizer`:
1. Walks the plan tree to find all [`ConnectorPlan`](ConnectorPlan.java) leaf nodes
2. Collects [`optimizationRules()`](Connector.java) from each distinct connector
3. Runs the collected rules as a single batch (`Limiter.ONCE`)

**Rule pattern:** Each rule pattern-matches on a standard ES|QL plan node (e.g., `Filter`, `Limit`) whose child is the connector's plan leaf. If the operation is translatable, the rule folds it into the leaf and removes the parent node. If not, the plan stays unchanged and ES|QL evaluates the operation. The [`ConnectorPushdownRule`](ConnectorPushdownRule.java) helper class handles the common guard logic (child type check + connector identity check) so rules only need to implement `pushDown()`.

**Base class rules:**
- [`LakehouseConnector`](lakehouse/LakehouseConnector.java): No rules by default (opt-in via `pushFilterRule()`, `pushLimitRule()`)
- [`SqlConnector`](sql/SqlConnector.java): `PushFilterToSql`, `PushLimitToSql`, `PushOrderByToSql`, `PushAggregateToSql`, `BuildSql`

### Phase 3: Physical Planning (Mapper)

**Connector method:** [`Connector.createPhysicalPlan()`](Connector.java)

**What happens:** The `Mapper` converts the connector's logical plan node into a physical plan node.

**Default implementation:** `Connector` provides a default `createPhysicalPlan()` that creates a
[`ConnectorExec`](ConnectorExec.java) wrapping the fully-optimized `ConnectorPlan`. This is shared by
all connector types — the connector-specific state (filters, SQL, file lists) lives inside the
`ConnectorPlan`, so no connector-specific physical plan node is needed.

Connectors can override `createPhysicalPlan()` if they need custom physical planning behavior.

### Phase 4: Work Distribution (distributed connectors only)

**Connector method:** [`Connector.planPartitions()`](Connector.java#L171)

Only called if [`capabilities().distributed()`](ConnectorCapabilities.java#L39) is true.

### Phase 5: Execution (LocalExecutionPlanner)

**Connector method:** [`Connector.createSourceOperator()`](Connector.java#L190)

---

## Lakehouse Architecture: Storage + Format Separation

The [`LakehouseConnector`](lakehouse/LakehouseConnector.java) is built on a pluggable architecture using types from
the [`lakehouse`](lakehouse/) package that separates storage access from format reading:

| Component | Responsibility | Examples |
|-----------|---------------|----------|
| [StorageProvider](lakehouse/StorageProvider.java) | Access files in storage | S3, GCS, HDFS, local FS |
| [FormatReader](lakehouse/FormatReader.java) | Read file formats | Parquet, ORC, CSV, Avro |
| [StorageObject](lakehouse/StorageObject.java) | Read handle for a single object | Sync + async I/O |
| [TableCatalog](lakehouse/TableCatalog.java) | (Optional) Table catalog integration | Iceberg, Delta Lake, Hudi |
| [FilterPushdownSupport](lakehouse/FilterPushdownSupport.java) | (Optional) Filter pushdown | Partition pruning, row-group filtering |

### Component Composition

```
                         LakehouseConnector
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                  │
              ▼                 ▼                  ▼
      StorageProvider      FormatReader       TableCatalog
      (file access)       (file reading)      (optional)
              │                 │                  │
              │                 │                  │
  ┌───────────┴──┐    ┌────────┴───┐    ┌─────────┴──────┐
  │ newObject()  │    │ metadata() │    │ canHandle()    │
  │ listObjects()│    │ read()     │    │ metadata()     │
  │ exists()     │    │            │    │ planScan()     │
  └──────────────┘    └────────────┘    └────────────────┘
         │                  │                   │
         ▼                  ▼                   ▼
    S3, GCS,          Parquet, ORC,       Iceberg, Delta,
    HDFS, Local       CSV, Avro           Hudi
                                                │
                                                ▼
                                       FilterPushdownSupport
                                       (partition pruning,
                                        row-group filtering)
```

**StorageProvider** and **FormatReader** are always required — they provide file access and reading.
**TableCatalog** and **FilterPushdownSupport** are optional — they add catalog integration and
filter pushdown for formats that support them (e.g., Iceberg).

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
| **Partitioning** | StorageProvider | `storage.listObjects()` lists files via `StorageIterator`, wraps as `FileTask`s |
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

Default `planPartitions()` implementation:
1. `getFileTasks(plan)` — calls `storage.listObjects(StoragePath)`, wraps each `StorageEntry` as a `FileTask`
2. `partitionTasks(tasks, targetPartitions)` — groups files based on target parallelism
3. `createPartition(plan, taskGroup)` — creates `ConnectorPartition` with aggregated size estimates

Subclasses with catalogs override `getFileTasks()` to use catalog file manifests
(which provide partition pruning and richer metadata like row counts).

---

## Example: JDBC Database

> Full implementation: [JdbcConnector](sql/JdbcConnector.java) + [JdbcPlan](sql/JdbcPlan.java)

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

### SQL Pushdown Pipeline

```
ES|QL plan tree                    SqlConnector rules            JdbcPlan state
═══════════════                    ══════════════════            ══════════════

  Limit 100                         PushLimitToSql               limit: 100
     │                                   │
  TopN(name ASC)                    PushOrderByToSql             orderBy: [name ASC]
     │                                   │
  Filter(age > 21)                  PushFilterToSql              filter: age > 21
     │                                   │
  JdbcPlan(users)                        │                       tableName: "users"
                                         │
                                    BuildSql (final rule)
                                         │
                                         ▼
                                    builtSql: "SELECT name, age
                                      FROM users
                                      WHERE age > 21
                                      ORDER BY name ASC
                                      LIMIT 100"
```

Each rule removes one ES|QL plan node and accumulates the operation into `JdbcPlan`.
The final `BuildSql` rule translates the accumulated operations into a single SQL string.

### Key classes

- [JdbcPlan](sql/JdbcPlan.java) — extends `SqlPlan`. Holds table name (or subquery), built SQL, filter, limit, orderBy, and aggregation. The `builtSql` field is populated by the `BuildSql` rule after all operations are translated.

- [JdbcConnector](sql/JdbcConnector.java) — extends `SqlConnector`. Resolves table names or native SQL queries via JDBC, translates ES|QL operations to SQL, and executes on the coordinator. JDBC calls are stubbed with comments showing the real implementation.

### Query Flow (table reference)

| Phase | What Happens | Method |
|-------|--------------|--------|
| **Parse** | `FROM my_db:users \| WHERE age > 21 \| SORT name \| LIMIT 100` | |
| **Resolve** | Connect via JDBC, read `users` table schema | [`resolve()`](sql/JdbcConnector.java) |
| **Optimize** | Push filter `age > 21` → SQL WHERE clause | [`translateFilter()`](sql/JdbcConnector.java) |
| **Optimize** | Push ORDER BY `name` → SQL ORDER BY clause | [`translateOrderBy()`](sql/JdbcConnector.java) |
| **Optimize** | Push limit 100 → SQL LIMIT clause | `PushLimitToSql` rule |
| **Optimize** | Build SQL: `SELECT name, age FROM users WHERE age > 21 ORDER BY name LIMIT 100` | `BuildSql` rule |
| **Execute** | Coordinator runs single JDBC query | [`createSourceOperator()`](sql/JdbcConnector.java) |

### Query Flow (native SQL)

| Phase | What Happens | Method |
|-------|--------------|--------|
| **Parse** | `FROM my_db:query("SELECT * FROM users WHERE active = true") \| STATS avg(salary) BY dept` | |
| **Resolve** | Detect SQL query, wrap as subquery `(SELECT ...) AS _subq`, read schema | [`resolve()`](sql/JdbcConnector.java) |
| **Optimize** | Push aggregation → SQL `SELECT dept, AVG(salary) FROM (...) GROUP BY dept` | [`translateAggregates()`](sql/JdbcConnector.java) |
| **Execute** | Coordinator runs single JDBC query | |

---

## Key Differences: Lakehouse vs SQL

| Aspect | [LakehouseConnector](lakehouse/LakehouseConnector.java) | [SqlConnector](sql/SqlConnector.java) |
|--------|---------|------------|
| **Type examples** | iceberg, delta, hudi, parquet | postgres, mysql, oracle |
| **Architecture** | Composes StorageProvider + FormatReader | Monolithic SQL translation |
| **Expression** | File pattern, catalog path | Table name, SQL query |
| **Plan base class** | [`LakehousePlan`](lakehouse/LakehousePlan.java) | [`SqlPlan`](sql/SqlPlan.java) |
| **Operations** | Filter, limit | Filter, limit, ORDER BY, aggregation |
| **Execution** | Distributed across data nodes | Coordinator only |
| **Partitioning** | By files | Single partition |
| **Example** | *(coming soon)* | [JdbcConnector](sql/JdbcConnector.java) |
