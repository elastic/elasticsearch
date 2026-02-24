# DataSource SPI Control Flow

This document describes how ES|QL integrates with external data sources through the DataSource SPI.

## Architecture

Two SPI layers: a generic **Core DataSource SPI** that the ESQL pipeline calls, and a
**Lakehouse SPI** that provides one concrete implementation composed from pluggable storage,
format, and catalog components.

### Plan node lifecycle

How plan nodes transform through the pipeline — each stage produces the input for the next:

```
  UnresolvedDataSourceRelation            created by Parser
  │  has  DataSourceDescriptor              (type, config, settings, expression)
  │
  │  resolved by DataSource.resolve()       async, on ResolutionContext.executor()
  ▼
  DataSourcePlan (abstract)               schema is now known
  │  has  dataSource()                      back-pointer to owning DataSource
  │  has  location()                        for error messages
  │  has  output()                          projected columns
  │
  │  optimized by DataSourceOptimizer
  │    collects rules from all DataSources
  │    runs DataSourcePushdownRule[]        folds Filter / Limit into plan leaf
  ▼
  DataSourcePlan  (same type, with pushed operations)
  │
  │  wrapped by DataSource.createPhysicalPlan()
  ▼
  DataSourceExec                          physical plan node
  │  has  plan: DataSourcePlan              carries the optimized logical plan
  │
  │  split by DataSource.planPartitions()
  │    skipped for coordinator-only sources ── uses single CoordinatorPartition
  ▼
  DataSourcePartition[]                   units of parallel work
  │
  │  executed by DataSource.createSourceOperator()
  ▼
  SourceOperatorFactory ──► Pages
```

### Component ownership

How the Core SPI, Lakehouse SPI, and Plugin layers relate structurally:

```
  Core DataSource SPI  (datasource/spi/)
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                                                                          │
 │  Contracts (what implementors must use):                                 │
 │                                                                          │
 │  DataSource (interface)              DataSourceCapabilities              │
 │  │                                     distributed / coordinator-only    │
 │  │  produces                                                             │
 │  ├──► DataSourcePlan (abstract)      DataSourceDescriptor               │
 │  ├──► DataSourceExec                   type, config, settings, expr      │
 │  └──► DataSourcePartition                                                │
 │                                                                          │
 │  Helpers (optional conveniences):                                        │
 │                                                                          │
 │  DataSourcePushdownRule              DataSourceOptimizer                 │
 │    convenience base for rules          runs rules from all data sources  │
 │    (guards: child type + identity)                                       │
 │                                      SplitPartitioner<S>                │
 │  CloseableIterator                     discover ──► group ──► wrap      │
 │    Iterator + Closeable                uses SizeAwareBinPacking (FFD)   │
 │                                                                          │
 └──────────────────────────┬───────────────────────────────────────────────┘
                            │
                            │ implements
                            │
  Lakehouse SPI  (datasource/lakehouse/spi/)
 ┌──────────────────────────┼───────────────────────────────────────────────┐
 │                          │                                               │
 │  LakehouseDataSource ────┘  (concrete, final)                           │
 │  │                                                                       │
 │  │  owns                                                                 │
 │  ├──► LakehouseRegistry                                                 │
 │  │    ├── StorageProviderRegistry ──► StorageProvider (by URI scheme)    │
 │  │    ├── FormatReaderRegistry ────► FormatReader     (by name / ext)   │
 │  │    └── (future) ───────────────► TableCatalog     (by catalog type)  │
 │  │                                                                       │
 │  │  produces                                                             │
 │  ├──► LakehousePlan  (extends DataSourcePlan, concrete, final)          │
 │  │      expression   ── file path or pattern                            │
 │  │      formatName   ── for FormatReader lookup at execution time       │
 │  │      nativeFilter ── opaque, from FilterPushdownSupport              │
 │  │      limit        ── pushed from LIMIT node                          │
 │  │                                                                       │
 │  ├──► LakehousePartition  (implements DataSourcePartition)              │
 │  │      tasks: List<FileTask>                                            │
 │  │                                                                       │
 │  └──► ExternalSourceOperatorFactory  (SourceOperatorFactory)            │
 │                                                                          │
 │  StorageProvider ──► StorageObject  (read handle, sync + async)         │
 │                  ──► StorageEntry   (directory listing metadata)         │
 │  FormatReader    ──► SourceMetadata (schema + statistics)               │
 │  FilterPushdownSupport ──► nativeFilter (opaque pushdown result)        │
 │                                                                          │
 └──────────────────────────┬───────────────────────────────────────────────┘
                            │
                            │ populated by plugins at startup
                            │
  Plugin Implementations
 ┌──────────────────────────┼───────────────────────────────────────────────┐
 │                          │                                               │
 │  StoragePlugin ──► Map<scheme, StorageProviderFactory>                   │
 │    "s3"   → S3StorageProvider       ┐                                   │
 │    "gcs"  → GcsStorageProvider      ├── any storage + any format        │
 │    "file" → LocalFsStorageProvider  ┘                                   │
 │                                                                          │
 │  FormatPlugin ──► Map<name, FormatReaderFactory>                        │
 │    "parquet" → ParquetFormatReader  ┐                                   │
 │    "orc"     → OrcFormatReader      ├── mixed freely with any storage   │
 │    "csv"     → CsvFormatReader      ┘                                   │
 │                                                                          │
 │  CatalogPlugin ──► Map<type, TableCatalogFactory>   (future)            │
 │    "iceberg" → IcebergTableCatalog                                      │
 │    "delta"   → DeltaTableCatalog                                        │
 │                                                                          │
 └──────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

We have two parallel packages:
- **`datasource/`** (singular, this package) — generic `DataSource` SPI with full lifecycle hooks (resolve, optimize, plan, partition, execute), lakehouse base classes, and partitioning infrastructure
- **`datasources/`** (plural, PR #141678) — lakehouse-focused implementation with async operators, registries, glob expansion, plugin discovery, and pipeline wiring

**Goal**: Make `datasource/` self-sufficient, wire it into the ESQL pipeline, then delete `datasources/`.

### Class Mapping: `datasource/` ↔ `datasources/` + plan nodes

How classes in `datasource/` (this package) correspond to classes in `datasources/` (PR #141678)
and the existing plan nodes.

**Plan nodes** — these are the least obvious mappings:

| `datasource/` (this package) | `datasources/` / plan nodes | Notes |
|------|------|-------|
| [UnresolvedDataSourceRelation](UnresolvedDataSourceRelation.java) | `plan/logical/UnresolvedExternalRelation` | Same role: unresolved leaf from parser |
| [DataSourcePlan](spi/DataSourcePlan.java) | `plan/logical/ExternalRelation` | Abstract base vs concrete node with opaque maps |
| [LakehousePlan](lakehouse/spi/LakehousePlan.java) | `plan/logical/ExternalRelation` | Typed fields (expression, formatName, nativeFilter, limit) replace ExternalRelation's opaque `Map<String, Object>` config/metadata |
| [DataSourceExec](spi/DataSourceExec.java) | `plan/physical/ExternalSourceExec` | Generic physical wrapper; ExternalSourceExec also had opaque maps |

**Core SPI** — new contracts with no `datasources/` equivalent:

| `datasource/` (this package) | Replaces | Notes |
|------|------|-------|
| [DataSource](spi/DataSource.java) | *(new)* | Central lifecycle interface — `datasources/` had no unified data source concept |
| [DataSourceCapabilities](spi/DataSourceCapabilities.java) | *(new)* | Distributed vs coordinator-only execution mode |
| [DataSourcePartition](spi/DataSourcePartition.java) | *(new)* | Work distribution unit — `datasources/` was coordinator-only |
| [DataSourceDescriptor](spi/DataSourceDescriptor.java) | `ExternalSourceResolution` (partial) | Parsed FROM clause; replaces resolution record |
| [DataSourceFactory](spi/DataSourceFactory.java) | *(new)* | Per-type factory — `datasources/` plugin was monolithic |

**Helpers** — optional conveniences, not part of the core contract:

| `datasource/` (this package) | Replaces | Notes |
|------|------|-------|
| [DataSourcePushdownRule](spi/DataSourcePushdownRule.java) | *(new)* | Convenience base for pushdown rules (handles guard logic); data sources can use plain `Rule` instead |
| [DataSourceOptimizer](spi/DataSourceOptimizer.java) | `FilterPushdownRegistry` | Internal: collects and runs data source-provided rules; not called by implementors |
| Partitioning sub-package (5 types) | *(new)* | Split→partition pipeline for distributed execution; data sources can implement `planPartitions()` directly |

**Plugin discovery:**

| `datasource/` (this package) | `datasources/` | Notes |
|------|------|-------|
| [DataSourcePlugin](spi/DataSourcePlugin.java) | `spi/DataSourcePlugin` | Split: universal plugin returns `Map<String, DataSourceFactory>` |
| [StoragePlugin](lakehouse/spi/StoragePlugin.java) | `spi/DataSourcePlugin.storageProviders()` | Extracted from monolithic plugin |
| [FormatPlugin](lakehouse/spi/FormatPlugin.java) | `spi/DataSourcePlugin.formatReaders()` | Extracted from monolithic plugin |
| [CatalogPlugin](lakehouse/spi/CatalogPlugin.java) | `spi/DataSourcePlugin.tableCatalogs()` | Extracted from monolithic plugin |
| [DataSourceRegistry](DataSourceRegistry.java) | `DataSourceModule` | Universal factory registry vs module-level wiring |
| [LakehouseRegistry](lakehouse/LakehouseRegistry.java) | `DataSourceModule` (partial) | Lakehouse-specific plugin discovery |

**Mirrored lakehouse SPI** — same types, moved from `datasources/spi/` to `datasource/lakehouse/spi/`:

| `datasource/lakehouse/spi/` | `datasources/spi/` | Sync status |
|------|------|------|
| StorageProvider | StorageProvider | Identical |
| StorageObject | StorageObject | Identical |
| StoragePath | StoragePath | Synced (added `fileUri(Path)`) |
| FormatReader | FormatReader | Identical |
| FormatReaderFactory | FormatReaderFactory | Identical |
| StorageProviderFactory | StorageProviderFactory | Identical |
| SourceMetadata | SourceMetadata | Identical |
| SimpleSourceMetadata | SimpleSourceMetadata | Identical |
| SourceStatistics | SourceStatistics | Identical |
| FilterPushdownSupport | FilterPushdownSupport | Identical |
| TableCatalog | TableCatalog | **Deliberate divergence**: main extends `ExternalSourceFactory`; ours is standalone (our `DataSource` replaces that role) |
| TableCatalogFactory | TableCatalogFactory | Identical |
| SourceOperatorFactoryProvider | SourceOperatorFactoryProvider | Identical |
| SourceOperatorContext | SourceOperatorContext | Identical |

Also mirrored: StorageEntry, StorageIterator (identical), FileSet (synced: sentinel-aware equals/hashCode), CloseableIterator (identical).

**Mirrored lakehouse internals** — same types, moved from `datasources/` to `datasource/lakehouse/`:

| `datasource/lakehouse/` | `datasources/` | Sync status |
|------|------|------|
| StorageProviderRegistry | StorageProviderRegistry | Synced (lazy factory-based) |
| FormatReaderRegistry | FormatReaderRegistry | Synced (lazy factory-based) |
| StorageManager | StorageManager | Synced (GCS scheme) |
| AsyncExternalSourceOperator | AsyncExternalSourceOperator | Identical |
| AsyncExternalSourceBuffer | AsyncExternalSourceBuffer | Synced (notifyNotFull fix) |
| AsyncExternalSourceOperatorFactory | AsyncExternalSourceOperatorFactory | Synced (drain utils) |
| ExternalSourceDrainUtils | ExternalSourceDrainUtils | Synced |
| ExternalSourceOperatorFactory | ExternalSourceOperatorFactory | Identical |
| GlobExpander | GlobExpander | Synced (sorted results) |
| GlobMatcher | GlobMatcher | Identical |

**Connector SPI** — mirrored from main (PR #142667) for connection-oriented sources (Flight, JDBC):

| `datasource/connector/spi/` | `datasources/spi/` | Sync status |
|------|------|------|
| [Split](connector/spi/Split.java) | Split | Identical (package change only) |
| [QueryRequest](connector/spi/QueryRequest.java) | QueryRequest | Identical (package change only) |
| [ResultCursor](connector/spi/ResultCursor.java) | ResultCursor | Package change; imports `CloseableIterator` from `datasource.spi` |
| [Connector](connector/spi/Connector.java) | Connector | Identical (package change only) |
| [ConnectorFactory](connector/spi/ConnectorFactory.java) | ConnectorFactory | **Standalone** — removed `extends ExternalSourceFactory`; `DataSource` handles filter pushdown and operator creation |
| [ConnectorPlugin](connector/spi/ConnectorPlugin.java) | *(extracted from DataSourcePlugin)* | Follows `StoragePlugin`/`FormatPlugin`/`CatalogPlugin` pattern |

**Connector infrastructure** — mirrored internal helpers:

| `datasource/connector/` | `datasources/` | Sync status |
|------|------|------|
| [AsyncConnectorSourceOperatorFactory](connector/AsyncConnectorSourceOperatorFactory.java) | AsyncConnectorSourceOperatorFactory | Identical (package + import changes only) |

**`ExternalSourceFactory`** is NOT mirrored — replaced by `DataSource` + `DataSourceFactory` in our architecture.

**`datasources/` types replaced by DataSource lifecycle** — deleted in Phase 3:

| `datasources/` type | Replaced by |
|------|------|
| `ExternalSourceResolver` | `DataSource.resolve()` (async) |
| `ExternalSourceResolution` | `DataSourceDescriptor` + `ActionListener<DataSourcePlan>` |
| `ExternalSourceMetadata` | `SourceMetadata` (already in lakehouse SPI) |
| `FilterPushdownRegistry` | `DataSource.optimizationRules()` |
| `OperatorFactoryRegistry` | `DataSource.createSourceOperator()` |

| Phase | Goal | Scope |
|-------|------|-------|
| **Phase 1: Build + Absorb** | `datasource/` becomes self-sufficient | ~13 new files, 0 pipeline changes |
| **Phase 2: Wire** | ESQL pipeline uses `datasource/` at every stage | ~12 pipeline files modified |
| **Phase 3: Kill** | Delete `datasources/` (plural) | ~33 files deleted |

**Key design decisions:**
1. Plugin interfaces follow ES `Map<String, Factory>` pattern — one interface per capability
2. `DataSourcePlugin` (universal) lives in `datasource/spi/`; `StoragePlugin`/`FormatPlugin`/`CatalogPlugin` (lakehouse) live in `datasource/lakehouse/spi/`
3. Replace PR #141678's plan nodes with generic `DataSourcePlan` → `DataSourceExec`; add `UnresolvedDataSourceRelation`
4. Pipeline talks to `DataSource` at every stage — no framework-level registries for filter pushdown or operator creation

**Current status:** Phase 1 complete. Phases 2 and 3 are next.

---

## Data Source Syntax

> **Note:** The `SOURCE` command described below is a **temporary, development-only syntax**
> for wiring and testing the DataSource SPI. It will be replaced by `FROM` integration
> (e.g., `FROM s3_logs:"logs/*.parquet"`) once the data source registry and `FROM` colon syntax
> are implemented. The `SOURCE` command exists behind a dev feature flag and is not intended
> for end users.

### SOURCE Command (temporary)

```
SOURCE type expression [WITH {key: value, ...}]
```

A standalone source command that explicitly specifies the data source type, expression,
and optional configuration. Uses the same `WITH {mapExpression}` pattern as `RERANK`,
`COMPLETION`, and other ES|QL commands.

**Grammar:**

```antlr
sourceCommand
    : SOURCE identifier stringOrParameter commandNamedParameters
    ;
```

Where `commandNamedParameters` is the existing `(WITH mapExpression)?` rule.

**Examples:**

```sql
-- S3 with inline configuration
SOURCE s3 "logs/*.parquet" WITH {"bucket": "my-bucket", "region": "us-east-1"}

-- Iceberg table (no configuration needed if defaults suffice)
SOURCE iceberg "catalog.db.table"

-- Nested configuration
SOURCE s3 "events/*.parquet" WITH {"bucket": "my-bucket", "auth": {"access_key": "AKIA...", "secret_key": "..."}}

-- PostgreSQL with connection details
SOURCE postgres "SELECT * FROM users" WITH {"host": "db.example.com", "port": 5432, "database": "mydb"}
```

### Mapping to DataSourceDescriptor

| Grammar token | DataSourceDescriptor field | Notes |
|---|---|---|
| `identifier` | `type` | Bare identifier: `s3`, `iceberg`, `postgres` |
| `stringOrParameter` | `expression` | Quoted string or parameter reference |
| `WITH {...}` | `configuration` | JSON-style map expression, optional |
| *(not present)* | `settings` | Empty for now (ES-controlled, future) |
| *(not present)* | *(Registered variant)* | Always Specified — no registry yet |
| parser source location | `source` | For error reporting |

### Future: FROM Integration

Once the data source registry is implemented, the long-term syntax will use `FROM`
with colon notation to reference registered data sources by name:

```sql
FROM s3_logs:"logs/*.parquet"
FROM my_postgres:query("SELECT * FROM users")
FROM my_iceberg:catalog.schema.table
```

This requires:
1. A data source registration API (CRUD for named data sources with stored configuration)
2. Parser changes to `FROM` to distinguish `cluster:index` from `datasource:expression`
3. Migration of the `SOURCE` command users to `FROM` syntax

---

## Core Abstractions

These are the types that define the DataSource SPI contract. Every data source must interact with them.

| Type | Description |
|------|-------------|
| [DataSource](spi/DataSource.java) | Main SPI interface with all lifecycle hooks (including `capabilities()`) |
| [DataSourcePlan](spi/DataSourcePlan.java) | Abstract base class for data source plan leaves (extends LeafPlan) |
| [DataSourceDescriptor](spi/DataSourceDescriptor.java) | Sealed interface: Specified (type, config, settings, expression) or Registered (name, expression) |
| [DataSourcePartition](spi/DataSourcePartition.java) | Interface for units of work in distributed execution |
| [DataSourceCapabilities](spi/DataSourceCapabilities.java) | Execution mode flag (distributed vs coordinator-only), returned by `DataSource.capabilities()` |
| [DataSourceExec](spi/DataSourceExec.java) | Physical plan node for all data sources (wraps DataSourcePlan) |
| [UnresolvedDataSourceRelation](UnresolvedDataSourceRelation.java) | Unresolved plan leaf created by parser, resolved into DataSourcePlan |

### Plugin Discovery

Types for discovering and creating data source instances at startup.
Follows the standard Elasticsearch plugin pattern (`Map<String, Factory>`).

| Type | Description |
|------|-------------|
| [DataSourcePlugin](spi/DataSourcePlugin.java) | Extension point: `dataSources(Settings)` returns `Map<String, DataSourceFactory>` |
| [DataSourceFactory](spi/DataSourceFactory.java) | Factory for creating DataSource instances from configuration |
| [DataSourceRegistry](DataSourceRegistry.java) | Collects DataSourcePlugin factories, provides lookup by type |

### Partitioning (`datasource/spi/partitioning/`)

Types for the split → partition pipeline. Used by distributed data sources that discover
fine-grained units of work (files, key ranges, shards) and group them into balanced partitions.

| Type | Description |
|------|-------------|
| [SplitPartitioner](spi/partitioning/SplitPartitioner.java) | Reusable discover → group → wrap helper for split-based partitioning (composed by data sources) |
| [DataSourceSplit](spi/partitioning/DataSourceSplit.java) | Interface for discovered units of work with optional size/affinity |
| [NodeAffinity](spi/partitioning/NodeAffinity.java) | Value type for hard (`require`) vs soft (`prefer`) node affinity |
| [DistributionHints](spi/partitioning/DistributionHints.java) | Hints for partitioning (target parallelism, available nodes) |
| [SizeAwareBinPacking](spi/partitioning/SizeAwareBinPacking.java) | Static utility for FFD bin-packing of splits (used by `SplitPartitioner`) |

### Helpers

Convenience classes built on top of the core abstractions. Data sources are free to use them or implement
the core abstractions directly.

| Type | Description |
|------|-------------|
| [DataSourcePushdownRule](spi/DataSourcePushdownRule.java) | Convenience base for optimization rules that push operations into plan leaves |
| [DataSourceOptimizer](spi/DataSourceOptimizer.java) | Collects and runs data source-provided optimization rules |
| [CloseableIterator](spi/CloseableIterator.java) | Generic closeable iterator (Iterator + Closeable) |

### Lakehouse Data Source

Concrete, registry-driven data source for file-based storage. No subclassing needed —
all variation is handled by registered `StoragePlugin`, `FormatPlugin`, and `CatalogPlugin` implementations.

| Type | Description |
|------|-------------|
| [LakehouseDataSource](lakehouse/spi/LakehouseDataSource.java) | Concrete lakehouse data source (composes [`SplitPartitioner`](spi/partitioning/SplitPartitioner.java)`<FileTask>`, looks up storage/format from [`LakehouseRegistry`](lakehouse/LakehouseRegistry.java)) |
| [LakehousePlan](lakehouse/spi/LakehousePlan.java) | Concrete plan node with expression, formatName, nativeFilter, limit |

### Lakehouse SPI

Production-ready abstractions for lakehouse storage, format reading, table catalog integration,
and filter pushdown. Used by `LakehouseDataSource` and available for direct use by data sources.

**Storage access:**

| Type | Description |
|------|-------------|
| [StoragePath](lakehouse/spi/StoragePath.java) | URI-like path for addressing objects in storage systems (scheme://host[:port]/path) with glob support |
| [StorageProvider](lakehouse/spi/StorageProvider.java) | SPI for accessing files in a storage system (S3, GCS, HDFS); lists by prefix with recursive option |
| [StorageObject](lakehouse/spi/StorageObject.java) | Read handle for a single object (sync + async) |
| [StorageEntry](lakehouse/spi/StorageEntry.java) | Metadata record from directory listing (path, length, lastModified) |
| [StorageIterator](lakehouse/spi/StorageIterator.java) | Storage-specific iterator over entries (extends CloseableIterator) |
| [StorageProviderFactory](lakehouse/spi/StorageProviderFactory.java) | Factory for creating StorageProvider instances |

**Format reading:**

| Type | Description |
|------|-------------|
| [FormatReader](lakehouse/spi/FormatReader.java) | SPI for reading file formats (Parquet, ORC, CSV, Avro) with SchemaResolution strategy |
| [FormatReaderFactory](lakehouse/spi/FormatReaderFactory.java) | Factory for creating FormatReader instances |

**Metadata:**

| Type | Description |
|------|-------------|
| [SourceMetadata](lakehouse/spi/SourceMetadata.java) | Unified metadata output from schema discovery (schema, location, statistics, source-specific metadata) |
| [SimpleSourceMetadata](lakehouse/spi/SimpleSourceMetadata.java) | Immutable SourceMetadata implementation with builder |
| [SourceStatistics](lakehouse/spi/SourceStatistics.java) | Row count, size, and per-column statistics |
| [FileSet](lakehouse/spi/FileSet.java) | Resolved set of files from glob/path with sentinel states (UNRESOLVED, EMPTY) |

**Filter pushdown:**

| Type | Description |
|------|-------------|
| [FilterPushdownSupport](lakehouse/spi/FilterPushdownSupport.java) | Push filter expressions to the data source |

**Table catalog:**

| Type | Description |
|------|-------------|
| [TableCatalog](lakehouse/spi/TableCatalog.java) | Integration with table formats (Iceberg, Delta Lake, Hudi) |
| [TableCatalogFactory](lakehouse/spi/TableCatalogFactory.java) | Factory for creating TableCatalog instances |

**Operator creation:**

| Type | Description |
|------|-------------|
| [SourceOperatorFactoryProvider](lakehouse/spi/SourceOperatorFactoryProvider.java) | Extension point for custom source operator creation |
| [SourceOperatorContext](lakehouse/spi/SourceOperatorContext.java) | Context record for operator factory creation |

### Lakehouse Plugin Discovery

Plugin interfaces for lakehouse infrastructure. Discovered by `LakehouseRegistry` at startup.

| Type | Description |
|------|-------------|
| [StoragePlugin](lakehouse/spi/StoragePlugin.java) | Extension point: `storageProviders(Settings)` returns `Map<String, StorageProviderFactory>` keyed by URI scheme |
| [FormatPlugin](lakehouse/spi/FormatPlugin.java) | Extension point: `formatReaders(Settings)` returns `Map<String, FormatReaderFactory>` keyed by format name |
| [CatalogPlugin](lakehouse/spi/CatalogPlugin.java) | Extension point: `tableCatalogs(Settings)` returns `Map<String, TableCatalogFactory>` keyed by catalog type |
| [LakehouseRegistry](lakehouse/LakehouseRegistry.java) | Discovers StoragePlugin/FormatPlugin/CatalogPlugin, populates registries |

### Lakehouse Registries

Internal registries for scheme-based provider lookup and format-based reader lookup.
Both registries use **lazy factory-based creation** — heavy dependencies (S3 client, Parquet reader)
are only loaded when a query first targets that backend.

| Type | Description |
|------|-------------|
| [StorageProviderRegistry](lakehouse/StorageProviderRegistry.java) | Scheme-keyed lazy lookup: stores `StorageProviderFactory` per scheme, creates providers on first access via double-checked locking |
| [FormatReaderRegistry](lakehouse/FormatReaderRegistry.java) | Name-keyed lazy lookup: stores `FormatReaderFactory` per format, creates readers on first access; extension pre-registration via `registerExtension()` |
| [StorageManager](lakehouse/StorageManager.java) | Facade over StorageProviderRegistry for creating StorageObject from paths |

### Lakehouse Async Operators

Async execution infrastructure for reading from external storage. Provides backpressure
via buffer and dual-mode (sync wrapper / native async) operation.

| Type | Description |
|------|-------------|
| [AsyncExternalSourceOperatorFactory](lakehouse/AsyncExternalSourceOperatorFactory.java) | Dual-mode factory: auto-selects sync wrapper vs native async based on FormatReader capabilities |
| [AsyncExternalSourceOperator](lakehouse/AsyncExternalSourceOperator.java) | SourceOperator that polls from AsyncExternalSourceBuffer |
| [AsyncExternalSourceBuffer](lakehouse/AsyncExternalSourceBuffer.java) | Thread-safe ConcurrentLinkedQueue-based buffer with backpressure |
| [ExternalSourceOperatorFactory](lakehouse/ExternalSourceOperatorFactory.java) | Synchronous factory for simple format readers |
| [ExternalSourceDrainUtils](lakehouse/ExternalSourceDrainUtils.java) | Backpressure-aware page draining using `PlainActionFuture` blocking (5-min timeout) |

### Lakehouse Glob Expansion

Internal utilities for expanding glob patterns into resolved file sets.

| Type | Description |
|------|-------------|
| [GlobExpander](lakehouse/GlobExpander.java) | Expands glob patterns and comma-separated path lists into FileSet instances |
| [GlobMatcher](lakehouse/GlobMatcher.java) | Converts glob patterns to Java regex and matches relative paths |

### Connector SPI

Connection-oriented data source types for protocol-based sources (Flight, JDBC, gRPC).
While the lakehouse SPI handles file-based storage, the connector SPI handles sources
that require live connections.

| Type | Description |
|------|-------------|
| [ConnectorFactory](connector/spi/ConnectorFactory.java) | Factory for creating connectors; handles schema resolution and connection creation |
| [Connector](connector/spi/Connector.java) | Live connection to an external data source; discovers splits and executes queries |
| [QueryRequest](connector/spi/QueryRequest.java) | Immutable query descriptor with target, projected columns, config, and batch size |
| [ResultCursor](connector/spi/ResultCursor.java) | Streaming cursor over query results (extends CloseableIterator) |
| [Split](connector/spi/Split.java) | Unit of parallel work; simple connectors use `Split.SINGLE` |

### Connector Plugin Discovery

| Type | Description |
|------|-------------|
| [ConnectorPlugin](connector/spi/ConnectorPlugin.java) | Extension point: `connectors(Settings)` returns `Map<String, ConnectorFactory>` keyed by connector type |

### Connector Infrastructure

| Type | Description |
|------|-------------|
| [AsyncConnectorSourceOperatorFactory](connector/AsyncConnectorSourceOperatorFactory.java) | Single-split source operator factory; executes connector query on background thread, feeds pages into AsyncExternalSourceBuffer |

### Core SPI: Component Composition

How core SPI concepts are used in each query phase:

```
  Phase              DataSource method                   Key concepts
  ═════              ════════════════                   ════════════

  RESOLVE            DataSource.resolve(descriptor, context, listener)
                     DataSourceDescriptor ──────────────► ActionListener<DataSourcePlan>
                     (SOURCE command: type + config + expression) (async callback with schema)

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

**Minimal core SPI:** The core [`DataSourcePlan`](spi/DataSourcePlan.java) abstract class only contains what's universal:
- `dataSource()` - identity
- `location()` - for error messages
- `output()` - schema

**Concrete lakehouse plan:** [`LakehousePlan`](lakehouse/spi/LakehousePlan.java) adds lakehouse-specific state:
- `expression` — file path/pattern
- `formatName` — for FormatReader lookup at execution time
- `nativeFilter` — opaque filter from FilterPushdownSupport
- `limit` — pushed limit

**Opaque expressions:** The expression after `:` is interpreted entirely by the data source. ES|QL passes it through without parsing or validation.

**Storage/Format separation:** Data lake data sources compose [`StorageProvider`](lakehouse/spi/StorageProvider.java) (file access) with [`FormatReader`](lakehouse/spi/FormatReader.java) (file reading) from the [`lakehouse`](lakehouse/) package. This allows any storage (S3, GCS, HDFS) to be combined with any format (Parquet, ORC, CSV). Optionally, a [`TableCatalog`](lakehouse/spi/TableCatalog.java) provides catalog integration for table-based sources.

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

**DataSource method:** [`DataSource.resolve()`](spi/DataSource.java) — async via `ActionListener<DataSourcePlan>`

**Input:** [`DataSourceDescriptor`](spi/DataSourceDescriptor.java) containing:
- `type()` - data source type (e.g., "s3", "postgres")
- `configuration()` - connection/auth config (opaque to ES)
- `settings()` - ES-controlled settings
- `expression()` - what to resolve (table, pattern, query)

**Async contract:** Resolution involves I/O (reading schema from remote storage, connecting to a
database, querying a catalog). Implementations run blocking I/O on the executor from
`ResolutionContext.executor()`, then call `listener.onResponse()` or `listener.onFailure()`.

**Flow:**
1. PreAnalyzer sees `SOURCE s3 "logs/*.parquet" WITH {...}` (temporary syntax)
2. Parser creates [`DataSourceDescriptor`](spi/DataSourceDescriptor.java) with type, expression, and configuration
3. Looks up data source by type from [`DataSourceRegistry`](DataSourceRegistry.java)
4. Calls `dataSource.resolve(descriptor, context, listener)`
5. Data source runs I/O on `context.executor()`, calls `listener.onResponse(plan)` with resolved schema

**Lakehouse resolution:** [`LakehouseDataSource.resolve()`](lakehouse/spi/LakehouseDataSource.java) wraps sync I/O on the executor:
1. Looks up `StorageProvider` from `LakehouseRegistry` by URI scheme (from expression)
2. Looks up `FormatReader` from `LakehouseRegistry` by format config or file extension
3. `storage.newObject(StoragePath.of(expression))` — open the file
4. `format.metadata(object)` — read schema from file metadata
5. Creates concrete `LakehousePlan` with schema + format name
6. `listener.onResponse(plan)` or `listener.onFailure(e)`

### Phase 2: Logical Optimization (DataSourceOptimizer)

**What happens:** DataSource-provided rules push operations into data source plan leaves.

**DataSource method:** [`DataSource.optimizationRules()`](spi/DataSource.java) returns `List<Rule<?, LogicalPlan>>`

**How it works:**
[`DataSource.applyOptimizationRules()`](spi/DataSource.java) runs as a **separate pass** after the main `LogicalPlanOptimizer`:
1. Collects [`optimizationRules()`](spi/DataSource.java) from all registered data sources
2. Runs the collected rules as a single batch (`Limiter.ONCE`)
3. Each rule pattern-matches on plan nodes it handles and skips nodes from other data sources

**Rule pattern:** Each rule pattern-matches on a standard ES|QL plan node (e.g., `Filter`, `Limit`) whose child is the data source's plan leaf. If the operation is translatable, the rule folds it into the leaf and removes the parent node. If not, the plan stays unchanged and ES|QL evaluates the operation. The [`DataSourcePushdownRule`](spi/DataSourcePushdownRule.java) helper class handles the common guard logic (child type check + data source identity check) so rules only need to implement `pushDown()`.

**Lakehouse rules:**
- [`LakehouseDataSource`](lakehouse/spi/LakehouseDataSource.java): Built-in limit pushdown (`PushLimitToLakehouse`); filter pushdown via `FilterPushdownSupport` planned for future

### Phase 3: Physical Planning (Mapper)

**DataSource method:** [`DataSource.createPhysicalPlan()`](spi/DataSource.java)

**What happens:** The `Mapper` converts the data source's logical plan node into a physical plan node.

**Default implementation:** `DataSource` provides a default `createPhysicalPlan()` that creates a
[`DataSourceExec`](spi/DataSourceExec.java) wrapping the fully-optimized `DataSourcePlan`. This is shared by
all data source types — the data source-specific state (filters, file lists, etc.) lives inside the
`DataSourcePlan`, so no data source-specific physical plan node is needed.

Data sources can override `createPhysicalPlan()` if they need custom physical planning behavior.

### Phase 4: Work Distribution (distributed data sources only)

**DataSource method:** [`DataSource.planPartitions()`](spi/DataSource.java#L171)

Only called if [`capabilities().distributed()`](spi/DataSourceCapabilities.java#L39) is true.

**Three-phase pattern (discover → group → wrap):**
Cross-engine research (Spark, Trino, Flink) reveals a universal pipeline for work distribution.
Discovery is source-specific (files, shards, scan tasks), but grouping is reusable.
[`SplitPartitioner`](spi/partitioning/SplitPartitioner.java) encapsulates this pipeline: data sources provide
discover, group, and wrap functions via its constructor.
The default grouping delegates to [`SizeAwareBinPacking`](spi/partitioning/SizeAwareBinPacking.java)
(FFD algorithm with count-based round-robin fallback), so data sources only need
to provide discovery and wrapping. Splits implement [`DataSourceSplit`](spi/partitioning/DataSourceSplit.java)
to provide optional size estimates and [`NodeAffinity`](spi/partitioning/NodeAffinity.java).

**Node affinity** distinguishes two access patterns:
- **Required** ([`NodeAffinity.require`](spi/partitioning/NodeAffinity.java)) — data only exists on one node (local filesystem, node-local resources). Always enforced; required-affinity splits from different nodes are never mixed.
- **Preferred** ([`NodeAffinity.prefer`](spi/partitioning/NodeAffinity.java)) — data is faster to read locally but accessible from anywhere (HDFS replicas, S3 with warm cache). Honored when [`DistributionHints.preferDataLocality()`](spi/partitioning/DistributionHints.java) is true; ignored otherwise.

### Phase 5: Execution (LocalExecutionPlanner)

**DataSource method:** [`DataSource.createSourceOperator()`](spi/DataSource.java#L190)

---

## Lakehouse Architecture: Storage + Format Separation

The [`LakehouseDataSource`](lakehouse/spi/LakehouseDataSource.java) is a concrete, registry-driven data source
that composes pluggable components from the [`LakehouseRegistry`](lakehouse/LakehouseRegistry.java).
No subclassing needed — all variation is handled by registered plugins:

| Component | Responsibility | Examples |
|-----------|---------------|----------|
| [StorageProvider](lakehouse/spi/StorageProvider.java) | Access files in storage | S3, GCS, HDFS, local FS |
| [FormatReader](lakehouse/spi/FormatReader.java) | Read file formats | Parquet, ORC, CSV, Avro |
| [StorageObject](lakehouse/spi/StorageObject.java) | Read handle for a single object | Sync + async I/O |
| [TableCatalog](lakehouse/spi/TableCatalog.java) | (Optional) Table catalog integration | Iceberg, Delta Lake, Hudi |
| [FilterPushdownSupport](lakehouse/spi/FilterPushdownSupport.java) | (Optional) Filter pushdown | Partition pruning, row-group filtering |

### Lakehouse: Component Composition

How lakehouse SPI concepts are used in each query phase:

```
  Phase              Components used                    Data flow
  ═════              ═══════════════                    ═════════

  RESOLVE            (async on ResolutionContext.executor())
                     LakehouseRegistry lookup:
                       StorageProvider by URI scheme ◄── StorageProviderRegistry
                       FormatReader by config/extension ◄── FormatReaderRegistry
                     storage.newObject() ──► format.metadata() ──────► SourceMetadata
                     ──► LakehousePlan(schema, formatName) ──► listener.onResponse(plan)

  OPTIMIZE           FilterPushdownSupport (opt-in)
                       fps.pushFilters(expressions) ───────────────────► pushed + remainder
                                                                          (split filters)

  PARTITION          StorageProvider + SplitPartitioner.planPartitions()
                       storage.listObjects(prefix, recursive) ──► StorageEntry[] ► FileTask[]
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
| Raw Parquet on S3 | S3 | Parquet | StoragePlugin("s3") + FormatPlugin("parquet") |
| Iceberg tables | S3/GCS/HDFS | Parquet/ORC | + CatalogPlugin("iceberg") (future) |
| Delta Lake | S3/Azure Blob | Parquet | + CatalogPlugin("delta") (future) |
| Local CSV | Local FS | CSV | StoragePlugin("file") + FormatPlugin("csv") |
| ORC on HDFS | HDFS | ORC | StoragePlugin("hdfs") + FormatPlugin("orc") |

### How Storage + Format Are Used in Each Phase

| Phase | Component | What Happens |
|-------|-----------|-------------|
| **Resolution** | StorageProviderRegistry + FormatReaderRegistry | Registry looks up StorageProvider by scheme, FormatReader by name/extension; `format.metadata()` infers schema |
| **Optimization** | LakehouseDataSource | Built-in limit pushdown rule; filter pushdown via FilterPushdownSupport (future) |
| **Partitioning** | StorageProvider + `SplitPartitioner` | `storage.listObjects()` lists files as `FileTask`s, partitioner bin-packs by size |
| **Execution** | StorageProvider + FormatReader | Looks up from registry by scheme/format name; `format.read()` produces Pages |

All phases are implemented in the concrete `LakehouseDataSource` class using registry lookups.
No subclassing needed — all variation through StoragePlugin/FormatPlugin/CatalogPlugin.

### Resolution Flow

Resolution runs asynchronously on the `ResolutionContext.executor()` thread.

1. Look up `StorageProvider` from `StorageProviderRegistry` by URI scheme + per-data-source config
2. Look up `FormatReader` from `FormatReaderRegistry` by explicit `"format"` config or file extension
3. `storage.newObject(StoragePath.of(expression))` — open the file
4. `format.metadata(object)` — read `SourceMetadata` from file metadata
5. Create `LakehousePlan` with schema, expression, and format name
6. `listener.onResponse(plan)`

### Optimization Rules

`LakehouseDataSource` provides a built-in limit pushdown rule that stores the limit
in `LakehousePlan.withLimit()`. Filter pushdown via [`FilterPushdownSupport`](lakehouse/spi/FilterPushdownSupport.java)
is planned for future integration with FormatReader capabilities.

### Partitioning Flow

Default `planPartitions()` implementation (discover → group → wrap via `SplitPartitioner`):
1. **discover:** calls `storage.listObjects(prefix, recursive)`, wraps each `StorageEntry` as a `FileTask`
2. **group:** size-aware bin-packing via `SizeAwareBinPacking` — respects [`NodeAffinity`](spi/partitioning/NodeAffinity.java) (required splits grouped strictly by node, preferred splits grouped by node when `preferDataLocality` is true), FFD bin-packing when file sizes are available, count-based round-robin fallback
3. **wrap:** creates `DataSourcePartition` with aggregated size estimates

When a `TableCatalog` is integrated (future), discovery can use catalog file manifests
instead of `storage.listObjects()`, providing partition pruning and richer metadata like row counts.

