# ESQL: Unified data source SPI — Meta Issue

The external data sources framework on main has a working SPI, distributed execution with split-based parallelism (#142996), and 11+ concrete plugins across three source families: file-based (S3 Parquet, GCS NDJSON, etc.), catalog-managed (Iceberg), and query-based (Arrow Flight). This is a strong foundation. The next step is evolving the SPI so that **all source types participate uniformly in every pipeline stage** — resolution, optimization, partitioning, and execution — and so that **new source types are easy to build** by composing existing components rather than implementing everything from scratch.

## What this unlocks

Today, only file-based sources get the full benefit of the infrastructure: distributed execution across data nodes, local parallelism, split-based partitioning, and filter pushdown. Other source types are missing capabilities that the framework could provide:

- **Arrow Flight runs on coordinator only**, even though Flight's native model produces multiple endpoint tickets that map naturally to splits and distribution
- **SQL connectors (future JDBC) can't receive pushed-down filters or limits** — `QueryRequest` carries projected columns but no filter expressions, so all filtering happens engine-side after data is read
- **Iceberg has all the parts but they're disconnected** — schema resolution, manifest-based file discovery, and partition filter translation all work individually, but aren't wired together to produce distributed execution or data reading
- **Building a new source type requires understanding which of the three paths to take** and which infrastructure applies, rather than implementing a uniform contract and getting distribution, parallelism, and optimization for free

Beyond unblocking specific source types, this work establishes a composition model: a Delta Lake plugin should only need to provide a catalog (the resolution strategy) and reuse existing Parquet reading, S3 storage, and distribution infrastructure. A new SQL-speaking source should get filter/limit/aggregation pushdown by providing a SQL dialect. A plugin that only adds a file format (Avro) or a compression codec shouldn't need to touch the data source contract at all.

## Why it's structured this way today

The three source families each wire into the pipeline through different code paths. `OperatorFactoryRegistry` dispatches via `instanceof` — file-based sources go through one path (split provider, filter pushdown, custom operators), connectors go through another (async wrapper, single split), and catalogs through a third (metadata-only, with the remaining methods not yet connected). The distribution infrastructure from #142996 works for any source that provides splits, but only file-based sources currently do, because the other paths don't connect to it.

Additionally, file-based and catalog-based sources share the same underlying execution model: resolve a location into files + schema, then read those files. A glob pattern over S3 enumerates files and reads a footer for schema. Iceberg reads metadata JSON and walks manifests. Delta Lake reads a transaction log. The resolution strategy differs, but everything downstream — storage, format reading, distribution, parallelism — is the same. There's an opportunity to unify these under one path with a pluggable catalog.

## Approach

**One base contract, all sources.** We extend the SPI with methods every source type can implement: `createOperatorFactory()` (replaces the `instanceof` dispatch with polymorphism), `optimizationRules()` (lets any source provide optimizer rules — generalizing filter pushdown to arbitrary pushdown including SQL generation, PromQL translation, partition pruning), and a wired `splitProvider()` for connectors and catalogs. A `DataSourceOptimizer` collects per-source rules and runs them as a post-pass. This connects all source types to the existing distribution infrastructure without rebuilding it.

**Composable components and standard assemblies.** On top of the base contract, we introduce assembly patterns for common source families. `LakehouseDataSource` composes a pluggable catalog, a storage provider, and a format reader — unifying file-based and catalog-based paths. Glob enumeration, Iceberg manifest walking, and future Delta Lake catalogs are all just resolution strategies; the rest (storage, format, distribution, parallelism) comes from the assembly and existing components. A `CachingCatalog` decorator wraps any catalog for schema/file-list caching. `ConnectorDataSource` standardizes connection + async execution. `SqlDataSource` extends it with dialect-driven SQL pushdown via optimizer rules. Components — catalogs, format readers, storage providers, SQL dialects — are independently registrable.

**Hierarchy emerges from code.** We don't impose a rigid type taxonomy upfront. We build the first concrete sources, find shared behavior, and extract intermediate abstractions where the code is actually the same. If lakehouse and SQL sources share an optimization path, that becomes a shared base. If they don't, they stay separate.

## Target Shape

```
DataSourceFactory (base contract — what the framework calls)
  type(), canHandle(), resolveMetadata()
  splitProvider(), optimizationRules(), createOperatorFactory()

  LakehouseDataSource (assembly: catalog + storage + format)
    IcebergDataSource, S3ParquetDataSource, GcsNdjsonDataSource, ...

  ConnectorDataSource (assembly: connection + async)
    SqlDataSource (assembly: SQL pushdown via dialect)
      JdbcDataSource, FlightSqlDataSource
    FlightDataSource

  [any source can implement DataSourceFactory directly]

Pipeline (all sources, one path):
  RESOLVE   → factory.resolveMetadata()
  OPTIMIZE  → DataSourceOptimizer runs factory.optimizationRules()
  PARTITION → SplitDiscoveryPhase calls factory.splitProvider()
              → AdaptiveStrategy distributes (all source types)
  EXECUTE   → DataSourceRegistry calls factory.createOperatorFactory()
              → no instanceof, no branching
```

## Steps

- [ ] **Wire the base contract:** Add `createOperatorFactory(ctx)`, `optimizationRules()`, wired `splitProvider()` to `ExternalSourceFactory`. Add `DataSourceOptimizer` post-pass. Thread pushdown data through `QueryRequest`. Eliminate `instanceof` fork in `OperatorFactoryRegistry`.
- [ ] **Lakehouse assembly:** Extract `Catalog` interface (`GlobCatalog`). Create `LakehouseDataSource` (catalog + storage + format). Migrate file-based sources.
- [ ] **Connector assembly:** Create `ConnectorDataSource`, migrate Flight.
- [ ] **Clean up old interface:** Deprecate and remove `operatorFactory()` / `filterPushdownSupport()`.
- [ ] **Rename `External*` → `DataSource*`.**
- [ ] **Iceberg end-to-end:** Create `IcebergCatalog`, migrate Iceberg to `LakehouseDataSource` → splits, data reading, filter pushdown.
- [ ] **`FROM datasource::expression` syntax.**
- [ ] **SQL pushdown proof of concept:** `SqlDialect`, `SqlAst`, `SqlPushdownRule`, `SqlDataSource` — validates `optimizationRules()` end-to-end.
