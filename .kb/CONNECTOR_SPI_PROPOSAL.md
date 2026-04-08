# ES|QL Connector SPI: Unified Proposal

This document describes what exists on main today for external data sources, identifies the gap between the two SPI paths, and proposes changes to close that gap.

## 1. What Exists on Main Today

ES|QL on main has a complete external data sources framework with a **three-path SPI** — file-based sources (S3 Parquet, GCS NDJSON), catalog-managed sources (Iceberg), and query-based sources (Arrow Flight, future JDBC/PromQL). All three share resolution, plugin lifecycle, and async execution, but diverge at operator creation, distribution, and optimization — with the catalog path being the most incomplete (metadata-only, no data reading).

### 1.1 Existing Class Hierarchy

```
┌─────────────────────────────────────────────────┐
│          ExternalSourceFactory  «interface»       │  SPI base
│─────────────────────────────────────────────────│
│ + type(): String                                │
│ + canHandle(location): boolean                  │
│ + resolveMetadata(loc, cfg): SourceMetadata     │
│ + filterPushdownSupport(): FilterPushdownSupport│  optional, default null
│ + operatorFactory(): SourceOperatorFactoryProv. │  optional, default null ← FILE-ONLY
│ + splitProvider(): SplitProvider                │  default: SplitProvider.SINGLE
└──────────────────────┬──────────────────────────┘
                       │ extends
┌──────────────────────┴──────────────────────────┐
│          ConnectorFactory  «interface»            │  query-based SPI
│─────────────────────────────────────────────────│
│ + open(config): Connector                       │
│                                                 │
│   (inherits operatorFactory() — never uses it)  │  ← dead method
│   (inherits splitProvider() — returns SINGLE)   │  ← wasted
└──────────────────────┬──────────────────────────┘
                       │ implements
┌──────────────────────┴──────────────────────────┐
│       FlightConnectorFactory  «concrete»         │  only connector impl today
│─────────────────────────────────────────────────│
│ canHandle("flight://", "grpc://")               │
│ resolveMetadata() → FlightClient.getSchema()    │
│ open() → FlightConnector                        │
└─────────────────────────────────────────────────┘


┌─────────────────────────────┐    ┌──────────────────────────────┐
│  Connector  «interface»      │    │  ResultCursor  «interface»    │
│─────────────────────────────│    │──────────────────────────────│
│ + discoverSplits(request):  │    │  extends CloseableIterator   │
│     List<Split>             │    │    <Page>                    │
│     (default: [SINGLE])     │    │ + cancel()                   │
│ + execute(request, split):  │────│                              │
│     ResultCursor            │    └──────────────────────────────┘
│ + close()                   │
└─────────────────────────────┘

--- Third subtype: catalog-managed sources (implicit today) ---

┌──────────────────────────────────────────────────┐
│       TableCatalog  «abstract»                    │  extends ExternalSourceFactory
│──────────────────────────────────────────────────│
│ + resolveTable(location, cfg): TableMetadata     │  catalog-specific resolution
│ + planScan(table, filter): List<FileScanTask>    │  discovers data files
│                                                  │
│   operatorFactory() → null                       │  ← STUBBED
│   splitProvider() → SINGLE                       │  ← NOT WIRED to planScan()
│   filterPushdownSupport() → null                 │  ← NOT WIRED (code exists)
│                                                  │
│   Registered via TableCatalogFactory,            │
│   wrapped in LazyTableCatalogWrapper             │
│   (which implements ExternalSourceFactory)       │
└──────────────────────┬───────────────────────────┘
                       │ extends
┌──────────────────────┴───────────────────────────┐
│     IcebergTableCatalog  «concrete»               │  only catalog impl today
│──────────────────────────────────────────────────│
│ resolveTable() → reads metadata.json,            │
│   manifest lists, manifests → schema             │  ← WORKS
│ planScan() → FileScanTask (data file paths)      │  ← WORKS but disconnected
│ IcebergPushdownFilters.convert()                 │  ← EXISTS but not wired
│                                                  │
│ Status: METADATA-ONLY                            │
│   Schema discovery works.                        │
│   operatorFactory() → null (no data reading)     │
│   splitProvider() → SINGLE (planScan not wired)  │
│   Data reading throws UnsupportedOperationExc.   │
└──────────────────────────────────────────────────┘

--- Supporting SPI types ---

SourceMetadata               FilterPushdownSupport         ExternalSplit «interface»
├── schema()                 ├── pushFilters(exprs):        ├── sourceType()
├── sourceType()             │     PushdownResult           └── estimatedSizeInBytes()
├── location()               └── canPush(expr):
├── sourceMetadata(): Map         Pushability               SplitProvider
├── config(): Map                                           └── discoverSplits(ctx):
└── statistics()                                                List<ExternalSplit>

QueryRequest «record»        SourceOperatorContext          SourceOperatorFactoryProv.
├── target                   ├── sourceType                 └── create(ctx):
├── projectedColumns         ├── projectedColumns               SourceOperatorFactory
├── attributes               ├── config, sourceMetadata
├── config                   ├── pushedFilter
├── batchSize                ├── split, sliceQueue
└── blockFactory             └── fileSet, partitionColumns

--- Plan nodes (shared by both paths) ---

ExternalRelation «logical»   ExternalSourceExec «physical»
├── sourcePath: String        ├── sourcePath, sourceType: String
├── output: List<Attribute>   ├── attributes: List<Attribute>
├── metadata: SourceMetadata  ├── config: Map<String, Object>      ← generic
└── fileSet: FileSet          ├── sourceMetadata: Map<Str, Obj>    ← opaque
                              ├── pushedFilter: Object              ← opaque
                              ├── splits: List<ExternalSplit>
                              └── withSplits(), withPushedFilter()

--- Framework internals ---

OperatorFactoryRegistry                    SplitDiscoveryPhase
├── sourceFactories: Map<String,           └── resolveExternalSplits(plan, factories)
│     ExternalSourceFactory>                   walks plan tree,
└── factory(ctx):                              calls factory.splitProvider()
      if instanceof ConnectorFactory               .discoverSplits(ctx)
        → open() → AsyncConnector...              for each ExternalSourceExec
      else → operatorFactory().create()

ExternalDistributionStrategy               AsyncConnectorSourceOperatorFactory
├── AdaptiveStrategy                       ├── wraps Connector + QueryRequest
│   (1 split → coord; agg → distribute)   ├── executes on background Executor
└── RoundRobinStrategy                     └── feeds pages through async buffer

ExternalSliceQueue                         DataNodeComputeHandler
└── thread-safe split queue                └── .startExternalComputeOnDataNodes()
    (AtomicInteger-based claiming)             sends splits to data nodes via exchange
```

### 1.2 Component Responsibilities (Today)

Here's the story of how a query like `EXTERNAL "s3://logs/data.parquet" | WHERE status > 400 | STATS count = COUNT(*) BY host` flows through the system, told through the components that handle each step.

**ExternalSourceResolver — "Who can handle this URI?"**

The entry point. When the parser sees `EXTERNAL "s3://..."`, the resolver iterates all registered `ExternalSourceFactory` instances, calling `canHandle()` on each. The factory that claims the URI is asked to `resolveMetadata()` — returning the schema (column names and types), source type identifier, and any opaque source-specific data (e.g., Parquet footer metadata, Arrow schema). The resolver builds an `ExternalRelation` logical plan node from this.

**ExternalSourceFactory — "I am a data source. Here's what I can do."**

The base SPI interface that every data source implements. It defines the full contract: identify yourself (`type()`), claim URIs (`canHandle()`), discover schema (`resolveMetadata()`), and optionally declare capabilities: filter pushdown (`filterPushdownSupport()`), split discovery (`splitProvider()`), and operator creation (`operatorFactory()`). The problem today: `operatorFactory()` is only meaningful for file-based sources; connectors inherit it but never use it. And `splitProvider()` defaults to `SINGLE`, which means connectors opt out of distribution by default.

**ConnectorFactory — "I speak to a remote system."**

Extends `ExternalSourceFactory` for query-based sources. Adds one method: `open(config) → Connector`. This creates a live, stateful connection to the remote system (a JDBC connection, a Flight client, an HTTP session). The design separates factory (stateless, shared) from connector (stateful, per-query). Today, `ConnectorFactory` inherits `operatorFactory()` and `splitProvider()` but ignores both — the framework bypasses them via `instanceof` checks.

**Connector and ResultCursor — "Open, query, stream, close."**

The live execution model. A `Connector` is opened for the duration of a query. It has two capabilities: `discoverSplits()` (how can this query be parallelized?) and `execute(request, split)` (run a query and return results). `ResultCursor` extends `CloseableIterator<Page>` — the connector produces ES|QL Pages incrementally rather than materializing all results in memory. This is the right model for databases (JDBC result sets), Flight (streaming Arrow batches), and REST APIs (paginated responses). The issue: `discoverSplits()` exists on the interface but is never called by the framework.

**OperatorFactoryRegistry — "Create the right operator for this source."**

The dispatch layer that bridges the plan and the compute engine. It holds all registered factories and creates `SourceOperator` instances for execution. This is where the architecture fractures: the registry uses `instanceof ConnectorFactory` to decide which path to take. If it's a connector → `open()` → wrap in `AsyncConnectorSourceOperatorFactory`. If it has `operatorFactory()` → delegate to the custom operator factory. The two paths get different capabilities because the branching happens *before* infrastructure like `ExternalSliceQueue` is applied.

**SplitDiscoveryPhase — "Break the work into parallelizable units."**

Runs after physical planning, before distribution. It walks the physical plan tree looking for `ExternalSourceExec` nodes. For each one, it finds the factory by `sourceType`, calls `factory.splitProvider().discoverSplits(ctx)`, and replaces the exec with a copy carrying the discovered splits. It also collects ancestor filter expressions so that split providers can do partition pruning (e.g., skip files where `year=2023` when the query says `WHERE year = 2024`). Critical for parallelism — but `ConnectorFactory.splitProvider()` returns `SINGLE`, so connectors get zero splits and zero parallelism.

**ExternalDistributionStrategy / AdaptiveStrategy — "Should we distribute or stay on coordinator?"**

Takes the split count, query shape, and available nodes, and decides the execution topology. `AdaptiveStrategy` is the smart default: one split → coordinator-only (no point distributing). LIMIT-only query → coordinator-only (don't waste network for small results). Aggregation + many splits → distribute across data nodes for parallel processing. Connectors, having `SINGLE` split, always get coordinator-only — even when the remote source supports parallelism.

**AsyncConnectorSourceOperatorFactory — "Run the connector on a background thread."**

The wrapper that makes connectors work inside the compute engine. The compute engine's `Driver` is single-threaded and synchronous — it calls `needsInput()` / `getOutput()` in a loop. But connectors are I/O-bound (network calls, database queries). This factory spawns a background thread that calls `Connector.execute()`, drains the `ResultCursor`, and feeds pages through an `AsyncExternalSourceBuffer` that the driver reads from. Today this is the *only* way connectors execute — single background thread, no split awareness, no distribution.

**ExternalRelation and ExternalSourceExec — "The plan nodes: source-agnostic containers."**

`ExternalRelation` is the logical plan leaf. It carries: source path (the URI), resolved output schema, `SourceMetadata`, and `FileSet`. No source-specific types — it's the same node for S3 Parquet, Arrow Flight, and JDBC. `ExternalSourceExec` is the physical equivalent, adding: `Map<String, Object>` config (generic — no `S3Configuration` leaking into core), `Object` pushed filter (opaque — the framework doesn't interpret it), and `List<ExternalSplit>` splits. The plan nodes are the strongest part of the current design — fully generic, shared by both paths. We keep them.

**FilterPushdownSupport — "Which filters can the source handle natively?"**

A declarative interface: given a list of filter expressions, return which ones the source can handle (`PushdownResult` with pushed + remainder). The framework stores the pushed filter as an opaque `Object` on `ExternalSourceExec`. Today, only Iceberg implements this (translating ES|QL expressions to Iceberg predicates for manifest pruning). The interface exists on `ExternalSourceFactory`, so connectors *could* implement it — but there's no path to deliver the pushed filter to the connector at execution time, because `QueryRequest` doesn't carry it.

**TableCatalog and IcebergTableCatalog — "The implicit third subtype."**

There's a third path in the hierarchy that the type system doesn't name explicitly. `TableCatalog` extends `ExternalSourceFactory` — it's an abstract class for catalog-managed sources (Iceberg, Delta Lake, Hudi). Plugins register a `TableCatalogFactory`, and the framework wraps it in `LazyTableCatalogWrapper` which implements `ExternalSourceFactory`. So from the framework's perspective, Iceberg looks like any other `ExternalSourceFactory`.

But `TableCatalog` adds catalog-specific methods: `resolveTable()` reads catalog metadata (Iceberg metadata JSON, manifest lists, manifests) and `planScan()` discovers data files via `FileScanTask`. These represent a third dimension of composition beyond files (storage + format) and connectors (connection + query): catalogs compose **catalog + storage + format** — metadata management on top of file reading.

The problem: this composition isn't actually connected. `IcebergTableCatalog` can discover schemas (works) and discover data files via `planScan()` (works), but `planScan()` isn't wired to `splitProvider()` — so the discovered files don't become splits. `operatorFactory()` returns null — so there are no operators to read the files. `IcebergPushdownFilters.convert()` exists and can translate ES|QL expressions to Iceberg predicates, but isn't wired to `filterPushdownSupport()`. Iceberg is **metadata-only today**: schema resolution works, everything else is stubbed. Data reading throws `UnsupportedOperationException("not yet supported")`.

**ExternalSliceQueue — "Thread-safe split distribution within a node."**

The local parallelism mechanism. When a node receives N splits, `ExternalSliceQueue` holds them in a queue. Multiple drivers on the same node each claim splits via atomic operations — `nextSplit()` returns the next unclaimed split or null when empty. This enables `DATA_PARALLELISM = min(splitCount, taskConcurrency)` — multiple drivers per node, each processing different splits. File-based sources use this. Connectors don't, because they never have real splits.

### 1.3 Existing Interactions: How Classes Call Each Other

```
RESOLVE:
  ExternalSourceResolver
    → finds ExternalSourceFactory by canHandle(location)
    → calls factory.resolveMetadata(location, config)
    → returns SourceMetadata (schema, sourceType, opaque data)
    → builds ExternalRelation logical plan node

OPTIMIZE:
  LogicalPlanOptimizer runs generic rules
    → FilterPushdownRegistry
        → calls factory.filterPushdownSupport().pushFilters(exprs)
        → stores opaque pushedFilter on ExternalSourceExec
  *** ConnectorFactory has no hook here ***

PLAN:
  Mapper: ExternalRelation → ExternalSourceExec
    (generic, both paths identical)

PARTITION:
  SplitDiscoveryPhase
    → walks physical plan
    → for each ExternalSourceExec:
        finds factory by sourceType
        calls factory.splitProvider().discoverSplits(ctx)
        replaces exec with exec.withSplits(splits)
  *** ConnectorFactory.splitProvider() returns SINGLE — no splits ***
  *** TableCatalog.splitProvider() returns SINGLE — planScan() not wired ***

  ExternalDistributionStrategy (AdaptiveStrategy)
    → inspects split count, query shape
    → decides coordinator-only vs. distributed
  *** With SINGLE split → always coordinator-only ***

EXECUTE:
  OperatorFactoryRegistry.factory(ctx):
    ┌─ if factory instanceof ConnectorFactory:           ← instanceof fork
    │    cf.open(config) → Connector
    │    new AsyncConnectorSourceOperatorFactory(
    │        connector, request, executor, sliceQueue)
    │    → single async thread, no distribution
    │
    ├─ else if factory.operatorFactory() != null:
    │    factory.operatorFactory().create(ctx)
    │    → custom SourceOperator with ExternalSliceQueue
    │    → local parallelism, distribution works
    │
    └─ else (TableCatalog case):
         operatorFactory() returns null
         → no operator, no data reading
         *** Iceberg is metadata-only ***
```

### 1.4 Pipeline Stages × Classes (Today)

```
Stage       │ File-Based Path                 │ Connector Path               │ Catalog Path (Iceberg)
────────────┼─────────────────────────────────┼──────────────────────────────┼──────────────────────────
            │                                 │                              │
 RESOLVE    │ ExternalSourceFactory           │ ConnectorFactory             │ TableCatalog
            │   .resolveMetadata()            │   .resolveMetadata()         │   .resolveTable()
            │ → SourceMetadata                │ → SourceMetadata             │ → schema ← WORKS
            │                                 │                              │
────────────┼─────────────────────────────────┼──────────────────────────────┼──────────────────────────
            │                                 │                              │
 OPTIMIZE   │ FilterPushdownSupport           │                              │ IcebergPushdownFilters
            │   .pushFilters(exprs)           │ (nothing — no hook)          │   .convert() ← EXISTS
            │ → pushedFilter on Exec          │                              │   but NOT WIRED
            │                                 │                              │
────────────┼─────────────────────────────────┼──────────────────────────────┼──────────────────────────
            │                                 │                              │
 PLAN       │ ExternalRelation                │ ExternalRelation             │ ExternalRelation
            │   → ExternalSourceExec          │   → ExternalSourceExec       │   → ExternalSourceExec
            │ (identical)                     │ (identical)                  │ (identical)
            │                                 │                              │
────────────┼─────────────────────────────────┼──────────────────────────────┼──────────────────────────
            │                                 │                              │
 PARTITION  │ SplitDiscoveryPhase             │                              │
            │   → factory.splitProvider()     │ SplitProvider.SINGLE         │ SplitProvider.SINGLE
            │   → real splits (FileSplit)     │ → no splits                  │ → no splits
            │ AdaptiveStrategy                │ → coordinator-only           │ (planScan() not wired)
            │   → distribute to data nodes   │                              │
            │                                 │                              │
────────────┼─────────────────────────────────┼──────────────────────────────┼──────────────────────────
            │                                 │                              │
 EXECUTE    │ OperatorFactoryRegistry         │ OperatorFactoryRegistry      │ OperatorFactoryRegistry
            │   .operatorFactory().create()   │   instanceof ConnectorFact.  │   .operatorFactory()
            │ → custom SourceOperator         │   .open() → Connector        │   → null
            │                                 │   → AsyncConnectorSrcOp      │   ← NO DATA READING
            │ ExternalSliceQueue              │                              │
            │ (multi-driver parallelism)      │ (single async thread)        │ (metadata-only)
            │                                 │                              │
```

### 1.5 Three-Level Filter Model

```
L1: Partition pruning    — at split discovery time (SplitProvider evaluates path-derived values)
L2: Per-node pushdown    — FilterPushdownSupport translates to source-native format
L3: Engine remainder     — standard FilterExec in the plan
```

Iceberg has L1+L2 code (`IcebergPushdownFilters.convert()`) but it's not wired — only L3 actually works today. Parquet, ORC, NDJSON, CSV only have L3. Connectors have only L3.

---

## 2. The Problem: Two Disjointed Paths

The three-path SPI creates a **capability asymmetry**. The file-based path has rich infrastructure that neither the query-based path nor the catalog path can fully access:

### 2.1 What Each Path Gets

| Capability | File-Based Path | Query-Based (Connector) Path | Catalog Path (Iceberg) |
|---|---|---|---|
| **Schema resolution** | `ExternalSourceFactory.resolveMetadata()` | `ConnectorFactory.resolveMetadata()` | `TableCatalog.resolveTable()` — works |
| **Filter pushdown** | `FilterPushdownSupport` (L1+L2+L3) | None — no way to pass filters to `Connector.execute()` | Code exists (`IcebergPushdownFilters`) but not wired |
| **Split discovery** | `SplitProvider` → `SplitDiscoveryPhase` | `Connector.discoverSplits()` exists but disconnected from `SplitDiscoveryPhase` | `planScan()` discovers files but not wired to `SplitProvider` |
| **Distribution** | Full: `AdaptiveStrategy` → data-node dispatch → `ExternalSliceQueue` | `Split.SINGLE` default → coordinator-only, single-threaded | `Split.SINGLE` — no distribution |
| **Local parallelism** | Multiple drivers via `ExternalSliceQueue` | Single async thread via `AsyncConnectorSourceOperatorFactory` | None — `operatorFactory()` returns null |
| **Partition detection** | `HivePartitionDetector`, `TemplatePartitionDetector`, `VirtualColumnInjector` | None | Iceberg has native partition spec but not wired |
| **Custom operators** | `SourceOperatorFactoryProvider` — full control over operator creation | Fixed: `AsyncConnectorSourceOperatorFactory` wraps all connectors | None — `operatorFactory()` returns null |
| **Data reading** | Full | Full (via Connector/ResultCursor) | **Stubbed** — throws `UnsupportedOperationException` |
| **Streaming decompression** | `CompressionDelegatingFormatReader` (ZSTD, BZIP2) | N/A | N/A (catalog manages file access) |

### 2.2 The Fork Diagram

The disconnect happens at `OperatorFactoryRegistry`:

```
       ExternalRelation → ExternalSourceExec
                          │
                SplitDiscoveryPhase ←── Only calls factory.splitProvider()
                          │                  ConnectorFactory doesn't wire
                          │                  Connector.discoverSplits() here
                          │                  TableCatalog.planScan() not wired either
                          │
               OperatorFactoryRegistry
                          │
         ┌────────────────┼────────────────────┐
         │                │                    │
   File-based path   Connector path       Catalog path
         │                │                    │
   factory.split-    ConnectorFactory     TableCatalog
     Provider()        .open()              .resolveTable() ← works
   factory.operator-   → Connector          .planScan()     ← works
     Factory()         → .execute(request,  .operatorFactory() → null
   factory.filter-       Split.SINGLE)      .splitProvider() → SINGLE
     Pushdown()          │                    │
         │                │                    │
   SplitProvider     Split.SINGLE always  METADATA-ONLY
   + distribution    (no distribution)    Schema discovery works
   + local parallel  (no parallelism)     No data reading
   + filter pushdown (no filter pushdown) No splits, no operators
   + custom operators(fixed async wrapper)Pieces exist but disconnected
```

### 2.3 Concrete Consequences

1. **Arrow Flight runs on coordinator only.** Even if a Flight server can produce multiple endpoint tickets (Flight's native parallelism model), there's no way to use `SplitDiscoveryPhase` + `AdaptiveStrategy` to distribute them across data nodes.

2. **JDBC can't push SQL.** A JDBC connector implementing `Connector.execute()` receives the raw target expression but has no way to receive pushed-down filters, limits, or projections as structured SQL fragments. The `QueryRequest` carries `projectedColumns` but no filter expressions.

3. **PromQL can't push time ranges.** Same issue — a PromQL connector can't receive `WHERE @timestamp > X AND @timestamp < Y` as a pushed-down time range to include in the PromQL query.

4. **No connector-provided optimization rules.** File-based sources get `FilterPushdownSupport` which is called during optimization. Connectors have no equivalent hook — they can't participate in the optimizer.

5. **Iceberg can't read data.** The catalog path has working schema discovery and file discovery (`planScan()`) but `operatorFactory()` returns null and `splitProvider()` returns `SINGLE`. The `planScan()` → `FileScanTask` → data file paths are disconnected from `SplitProvider` → `SplitDiscoveryPhase`. Iceberg's filter pushdown code (`IcebergPushdownFilters.convert()`) exists but isn't wired.

6. **God interface.** `ExternalSourceFactory` has methods that only some subtypes use. `operatorFactory()` is dead for connectors — they inherit it but never implement it. `filterPushdownSupport()` exists but connectors can't benefit from it. `splitProvider()` defaults to `SINGLE`, which means connectors and catalogs silently opt out of distribution. The interface pretends to be universal but the contract is different for each subtype.

7. **File and catalog are artificially separate paths.** A glob pattern over S3 and an Iceberg table both resolve to files + schema, then read those files with storage + format. The resolution strategy differs (glob enumeration vs manifest walking), but the execution model is identical. Yet the type hierarchy treats them as different paths — `TableCatalog extends ExternalSourceFactory` directly, with most methods stubbed, rather than composing a catalog with existing file-reading infrastructure.

8. **No composition model.** You can't register just a format reader or just a catalog. Every plugin must implement the full `ExternalSourceFactory` interface or return null for methods it doesn't support. A Delta Lake plugin should only need to provide a catalog (the resolution strategy) and reuse existing S3/Parquet infrastructure. Today, it would need to build a complete factory from scratch.

---

## 3. Target Architecture

### 3.1 Design Principles

Four principles guide the design:

**The base contract is all the framework sees.** The query pipeline calls methods on one interface: `DataSourceFactory`. It doesn't know whether it's talking to Iceberg, JDBC, or S3 Parquet. No `instanceof` checks, no type-specific branching in framework code. Polymorphism and composition handle dispatch.

**Components are independently registrable.** A plugin that provides an Avro format reader registers a format reader. A plugin that provides a Delta Lake catalog registers a catalog. These pieces exist independently of the full data source contract. Not every plugin builds a complete data source.

**Assembly patterns compose components into the base contract.** Standard assemblies — lakehouse (catalog + storage + format), SQL connector (connection + dialect) — wire components into something the framework can call. These are conveniences for common patterns, not the only way. A plugin that wants full control implements the base contract directly.

**Abstractions are extracted from commonality, not imposed.** We don't declare "there are two families" upfront. We build real sources, find shared behavior, and extract intermediate abstractions. If SQL and lakehouse share an optimization path, that becomes a shared base. If they don't, they stay separate. The hierarchy emerges from code, not from taxonomy.

### 3.2 The Three Layers

The target architecture has three layers. They are not rigid tiers — layers 2 and 3 are intertwined, with each assembly pattern defining its own component abstractions.

**Layer 1: Base Contract** — what the framework calls:

```java
public interface DataSourceFactory {
    String type();
    boolean canHandle(String location);
    SourceMetadata resolveMetadata(String location, Map<String, Object> config);
    default SplitProvider splitProvider() { return SplitProvider.SINGLE; }
    default List<Rule<LogicalPlan, LogicalPlan>> optimizationRules() { return List.of(); }
    SourceOperator.SourceOperatorFactory createOperatorFactory(SourceOperatorContext ctx);
}
```

Every data source implements this. The framework calls the same methods regardless of source type. No dead methods — `createOperatorFactory()` replaces the old `operatorFactory()` that connectors never used. `optimizationRules()` replaces `filterPushdownSupport()` — source-driven, not framework-driven. `splitProvider()` works for all sources — connectors and catalogs override with real splits instead of defaulting to SINGLE.

**Layer 2: Component SPI** — individually registrable pieces:

| Component | Contract | Examples |
|---|---|---|
| **Catalog** | `location → (files, schema, optimizationRules)` | GlobCatalog, IcebergCatalog, DeltaCatalog, CachingCatalog |
| **StorageProvider** | `path → InputStream` | S3, GCS, Azure, HTTP, local |
| **FormatReader** | `InputStream → Pages` | Parquet, ORC, NDJSON, CSV |
| **CompressionCodec** | wraps FormatReader with decompression | ZSTD, BZIP2, Gzip |
| **Connector** | `(query, split) → ResultCursor` | JDBC, Flight, HTTP/REST |
| **SqlDialect** | quoting, type mapping, LIMIT syntax | Postgres, MySQL, FlightSql |

A plugin registers one or more components. A plugin that only provides a new format reader (Avro) doesn't touch the base contract. A plugin that only provides a new catalog (Delta Lake) registers a catalog — the lakehouse assembly picks it up.

The **catalog** abstraction deserves special attention. Today, "file-based" and "catalog-based" sources are separate paths, but they do the same thing: resolve a location into files + schema. A glob pattern over S3 is just the simplest catalog — it enumerates files and reads one footer for schema. Iceberg is a richer catalog — it reads metadata JSON, walks manifests, prunes partitions. Delta Lake reads a transaction log. They all produce the same output: a list of files to read and a schema. The difference is the resolution strategy, not the execution model.

`CachingCatalog` is a decorator that wraps any catalog, caching the resolved schema and file list. It works on top of glob, Iceberg, Delta — anything. Over time, this becomes the schema caching layer.

**Layer 3: Assembly Patterns** — standard compositions that implement the base contract:

Assembly patterns are abstract classes that know how to compose components. They expose their own abstractions for extensibility. A plugin that builds a lakehouse source extends `LakehouseDataSource` and provides a catalog. A plugin that builds a SQL source extends `SqlDataSource` and provides a dialect.

But assembly patterns are not the only way. A PromQL source might implement `DataSourceFactory` directly, providing its own optimization rules and execution logic without fitting into any standard assembly. The base contract is always the escape hatch.

Assembly patterns may also share intermediate abstractions with each other. If lakehouse and SQL sources share an optimization path or pushdown mechanism, that shared behavior gets extracted into a common base. We discover this by building, not by designing upfront — the hierarchy emerges from actual shared code.

### 3.3 Target Class Hierarchy

```
DataSourceFactory «interface»                     THE BASE CONTRACT
│
│ type(), canHandle(), resolveMetadata()
│ splitProvider(), optimizationRules()
│ createOperatorFactory()
│
├── LakehouseDataSource «abstract»                ASSEMBLY: catalog + storage + format
│   │
│   │ catalog(): Catalog
│   │ storageProvider(): StorageProvider
│   │ formatReader(): FormatReader
│   │
│   │ resolveMetadata() → delegates to catalog.resolve()
│   │ splitProvider()   → catalog file discovery → FileSplits
│   │ optimizationRules() → delegates to catalog.optimizationRules()
│   │ createOperatorFactory() → composes storage + format
│   │
│   ├── IcebergDataSource       catalog: IcebergCatalog
│   │                           (manifest pruning via optimization rules)
│   ├── [DeltaLakeDataSource]   catalog: DeltaCatalog (future)
│   └── S3ParquetDataSource     catalog: GlobCatalog (simplest — file enumeration)
│       GcsNdjsonDataSource     catalog: GlobCatalog
│       etc.
│
├── ConnectorDataSource «abstract»                ASSEMBLY: connection-based
│   │
│   │ openConnector(config): Connector
│   │
│   │ splitProvider()   → connector.discoverSplits()
│   │ createOperatorFactory() → open + AsyncConnectorSourceOperatorFactory
│   │
│   ├── SqlDataSource «abstract»                  ASSEMBLY: SQL pushdown
│   │   │
│   │   │ dialect(): SqlDialect
│   │   │ optimizationRules() → SqlPushdownRule
│   │   │   (translates Filter/Limit/TopN/Agg → SQL AST)
│   │   │
│   │   ├── JdbcDataSource         dialect: Postgres/MySQL/Oracle
│   │   │                          splits: range-based on partition key
│   │   └── FlightSqlDataSource    dialect: FlightSql
│   │                              splits: FlightInfo endpoints
│   │
│   └── FlightDataSource           raw Arrow Flight (no SQL)
│                                  splits: FlightInfo endpoints
│                                  optimizationRules(): Flight-specific
│
├── [PromQLDataSource]             DIRECT: implements base contract
│   │                              optimizationRules(): PromQL generation
│   │                              splits: per-time-range
│   └── (no standard assembly — custom)
│
└── [any custom source]            implements DataSourceFactory directly


--- Components (independently registrable) ---

Catalog «interface»                    Connector «interface»
├── resolve(loc, cfg): CatalogMeta     ├── discoverSplits(req): List<Split>
├── optimizationRules(): List<Rule>    ├── execute(req, split): ResultCursor
│                                      └── close()
├── GlobCatalog (file enumeration)
├── IcebergCatalog (manifest walking)  SqlDialect «interface»
├── DeltaCatalog (txn log) [future]    ├── quoteIdentifier(name)
├── CachingCatalog (decorator)         ├── typeName(esqlType)
                                       ├── limitSyntax()
StorageProvider «interface»            ├── functionName(esqlFn)
├── S3, GCS, Azure, HTTP, Local
                                       SqlPushdownRule «concrete»
FormatReader «interface»               └── walks plan above source,
├── Parquet, ORC, NDJSON, CSV             translates to SqlAst

CompressionCodec «interface»           SqlAst
├── ZSTD, BZIP2, Gzip                 └── SELECT/FROM/WHERE/ORDER/LIMIT
                                           render(dialect): String


--- Plan nodes (unchanged from today, renamed) ---

DataSourceRelation «logical»         DataSourceExec «physical»
  (from ExternalRelation)              (from ExternalSourceExec)
  same generic shape                   same generic shape

DataSourceRegistry                   DataSourceOptimizer «new»
  (from OperatorFactoryRegistry)       collects optimizationRules()
  → factory.createOperatorFactory()    from all factories,
  ← ONE method, NO instanceof fork    runs as post-pass
```

### 3.4 Component Responsibilities (Target)

Here's the same query — `FROM iceberg::db.logs | WHERE status > 400 | STATS count = COUNT(*) BY host` — flowing through the target architecture.

**DataSourceFactory — "The base contract."**

What the framework calls. Every method is meaningful for every source type. `resolveMetadata()` for resolution, `optimizationRules()` for pushdown, `splitProvider()` for partitioning, `createOperatorFactory()` for execution. The framework doesn't branch on source type — it calls these four methods and gets the right behavior via polymorphism. The old `operatorFactory()` (dead for connectors) and `filterPushdownSupport()` (framework-driven enumeration) are gone.

**DataSourceRegistry (renamed from OperatorFactoryRegistry) — "One method, no branching."**

The `instanceof ConnectorFactory` fork is gone. The registry calls `factory.createOperatorFactory(ctx)` and gets back a `SourceOperatorFactory`. What happens inside — composing a file reader, opening a connector, wrapping in async — is the factory's business. This is the most important structural change.

**LakehouseDataSource — "Catalog + storage + format."**

The assembly for file-based sources. It implements the base contract by composing three components: a **catalog** (location → files + schema), a **storage provider** (file path → bytes), and a **format reader** (bytes → Pages). The catalog does the location-dependent work — glob enumeration, Iceberg manifest walking, Delta transaction log parsing — and produces a uniform result. The storage + format composition handles the uniform part — reading bytes from files and decoding them into Pages.

Key insight: what used to be two separate paths (file-based and catalog-based) is one assembly with a pluggable catalog. A simple `s3://bucket/*.parquet` query uses `GlobCatalog` — file enumeration + Parquet footer schema. An Iceberg query uses `IcebergCatalog` — manifest walking + partition pruning. Both compose the same storage + format pipeline for actual data reading. Building a Delta Lake data source means implementing `DeltaCatalog` — just the resolution strategy. Everything else (S3 reading, Parquet decoding, distribution, local parallelism) comes from `LakehouseDataSource` and existing components. No reimplementation.

`splitProvider()` delegates to the catalog's file discovery. Catalog-discovered files become splits that flow through `SplitDiscoveryPhase` → `AdaptiveStrategy` for distribution.

`optimizationRules()` delegates to the catalog. `IcebergCatalog` provides rules for manifest pruning via Iceberg predicates. `GlobCatalog` may provide rules for Hive partition pruning. A catalog without optimization returns no rules.

**Catalog — "Location → files + schema."**

The pluggable component inside `LakehouseDataSource`. All catalogs do the same thing with different strategies:

- `GlobCatalog`: enumerate files by glob pattern, read one file footer for schema
- `IcebergCatalog`: read metadata JSON, walk manifest lists, discover data files, extract schema from table metadata
- `DeltaCatalog`: read `_delta_log`, replay transaction entries, discover data files
- `CachingCatalog`: wraps any catalog, caches resolved schema and file list

A `CachingCatalog` on top of `IcebergCatalog` means repeated queries skip manifest walking. On top of `GlobCatalog`, repeated queries skip file enumeration. The decorator doesn't know or care what it wraps. Over time, this becomes the schema caching layer.

**ConnectorDataSource — "Connection-based execution."**

The assembly for sources that speak to remote systems. It implements the base contract by opening a `Connector` and wrapping it for async execution. `splitProvider()` delegates to `connector.discoverSplits()` — so Flight endpoints and JDBC key-ranges flow through the same split infrastructure as file-based sources. `createOperatorFactory()` encapsulates the `open()` → `AsyncConnectorSourceOperatorFactory` logic that used to be behind an `instanceof` check.

**SqlDataSource — "SQL pushdown for free."**

Extends `ConnectorDataSource` for SQL-speaking sources. Provides `optimizationRules()` returning `SqlPushdownRule` — a rule that walks the ES|QL plan, translates Filter/Limit/TopN/Aggregate nodes into a `SqlAst`, and renders to dialect-specific SQL. A `JdbcDataSource` extends this, provides `PostgresDialect`, and inherits the full SQL pushdown without writing any optimization code. SQL sources can also have distribution: JDBC range-splits on a partition key, Flight SQL endpoint-based splits — all flowing through the same `SplitDiscoveryPhase`.

**DataSourceOptimizer — "All sources participate in optimization."**

Collects `optimizationRules()` from all registered factories, runs them as a post-pass after `LogicalPlanOptimizer`. Iceberg's catalog rules push partition predicates into manifest pruning. SQL rules push Filter+Limit+Agg into SQL generation. PromQL rules translate time ranges into PromQL expressions. The framework doesn't enumerate pushdown types — the source decides what it can handle.

This means `FilterPushdownSupport` moves from being an SPI method to an **internal utility class**. Iceberg's optimization rule internally uses it — but that's Iceberg's business, not the framework's. The base `DataSourceFactory` interface has one pushdown mechanism: `optimizationRules()`.

**Intermediate abstractions — "Extracted, not imposed."**

The hierarchy above shows `LakehouseDataSource` and `ConnectorDataSource` as separate branches. But they may share intermediate abstractions. If SQL and lakehouse share an optimization path, that becomes a common base class. If split discovery follows the same pattern across all sources, that logic gets extracted. We discover these by building the first few concrete sources — Iceberg, JDBC, Flight — and seeing where the code is actually the same. The hierarchy emerges from shared behavior, not from a predetermined taxonomy.

### 3.5 Target Interactions

```
RESOLVE:
  DataSourceResolver
    → finds DataSourceFactory by canHandle(location)
    → calls factory.resolveMetadata(location, config)
      Lakehouse: delegates to catalog.resolve() → files + schema
      Connector: connects, reads metadata
    → returns SourceMetadata
    → builds DataSourceRelation

OPTIMIZE:
  LogicalPlanOptimizer runs generic rules (unchanged)
    ↓
  DataSourceOptimizer runs per-source rules
    → collects factory.optimizationRules() from all factories
    → Iceberg: catalog provides manifest pruning rules
    → SQL: SqlPushdownRule translates plan → SQL AST
    → PromQL: custom rules translate time ranges → PromQL
    → rules transform plan subtrees, store opaque pushdown data

PLAN:
  Mapper: DataSourceRelation → DataSourceExec
    (generic, all sources identical — unchanged)

PARTITION:
  SplitDiscoveryPhase
    → walks physical plan
    → for each DataSourceExec:
        calls factory.splitProvider().discoverSplits(ctx)
        Lakehouse: catalog file discovery → FileSplits
        Flight: endpoint tickets as splits
        JDBC: range splits on partition key
        All flow through the same path — no branching
    → replaces exec with exec.withSplits(splits)

  AdaptiveStrategy
    → inspects split count, query shape
    → distributes across data nodes (works for ALL source types)

EXECUTE:
  DataSourceRegistry.factory(ctx):
    → factory.createOperatorFactory(ctx)                    ← ONE METHOD
      Lakehouse: composes storage + format reader
      Connector: open() → AsyncConnectorSourceOperatorFactory
    → ExternalSliceQueue for local parallelism (all sources)
```

### 3.6 Pipeline Stages × Classes (Target — contrast with 1.4)

```
Stage       │ All Sources (unified)
────────────┼──────────────────────────────────────────────────────────────
            │
 RESOLVE    │ DataSourceFactory.resolveMetadata()
            │   Lakehouse: catalog.resolve() → files + schema
            │   Connector: connect + metadata discovery
            │ → SourceMetadata (same type for all sources)
            │
────────────┼──────────────────────────────────────────────────────────────
            │
 OPTIMIZE   │ LogicalPlanOptimizer (generic rules — unchanged)
            │   ↓
            │ DataSourceOptimizer
            │   collects factory.optimizationRules()
            │   Iceberg: manifest pruning via catalog rules
            │   SQL: Filter+Limit+Agg → SQL AST via SqlPushdownRule
            │   PromQL: time ranges → PromQL via custom rules
            │
────────────┼──────────────────────────────────────────────────────────────
            │
 PLAN       │ DataSourceRelation → DataSourceExec
            │ (generic, unchanged)
            │
────────────┼──────────────────────────────────────────────────────────────
            │
 PARTITION  │ SplitDiscoveryPhase
            │   → factory.splitProvider().discoverSplits(ctx)
            │     Lakehouse: catalog → file paths as splits
            │     Flight: endpoint tickets as splits
            │     JDBC: range splits on partition key
            │   → AdaptiveStrategy → distribute across data nodes
            │   → works for ALL source types (no branching)
            │
────────────┼──────────────────────────────────────────────────────────────
            │
 EXECUTE    │ DataSourceRegistry
            │   → factory.createOperatorFactory(ctx)          ← ONE METHOD
            │     Lakehouse: storage + format reader
            │     Connector: open + async wrapper
            │   → ExternalSliceQueue (all sources)
            │
```

### 3.7 What Changes Structurally

1. **Replace `operatorFactory()` with `createOperatorFactory(ctx)` on the base.** Every source implements it. `DataSourceRegistry` calls one method — no `instanceof` fork.

2. **Replace `filterPushdownSupport()` with `optimizationRules()` on the base.** Source-driven, not framework-driven. No enumeration of pushdown types. `FilterPushdownSupport` survives as an internal utility (Iceberg's rules use it), not on the SPI.

3. **Add `DataSourceOptimizer`** to collect and run per-source optimization rules as a post-pass.

4. **Extract `Catalog` interface and `LakehouseDataSource` assembly.** Unifies the old file-based and catalog-based paths. Glob file enumeration, Iceberg manifest walking, and future Delta Lake all implement `Catalog`. `CachingCatalog` decorator wraps any of them. `LakehouseDataSource` composes catalog + storage + format.

5. **Extract `ConnectorDataSource` and `SqlDataSource` assemblies.** Connector splits wire into `SplitDiscoveryPhase` via `splitProvider()` delegation. `SqlDataSource` provides SQL pushdown for free via `SqlPushdownRule` + `SqlDialect`.

### 3.8 Renames

| Today | Target | Reason |
|---|---|---|
| `ExternalSourceFactory` | `DataSourceFactory` | Not "external" — just another data source |
| `ExternalRelation` | `DataSourceRelation` | Consistent naming |
| `ExternalSourceExec` | `DataSourceExec` | Consistent naming |
| `ExternalSplit` | `DataSourceSplit` | Consistent naming |
| `ExternalSourceResolver` | `DataSourceResolver` | Consistent naming |
| `OperatorFactoryRegistry` | `DataSourceRegistry` | Dispatches data sources, not just operator factories |
| `ConnectorFactory` | `ConnectorDataSource` | It's a data source that uses a connector, not a factory of connectors |
| `TableCatalog` | `Catalog` | Simplified — catalog component of lakehouse |

**Keep unchanged:** `Connector`, `ResultCursor`, `StorageProvider`, `FormatReader`, `SplitProvider`, `QueryRequest` — these are components, not SPI-level types, and their names are already correct.

Internal infrastructure names (`ExternalSliceQueue`, `ExternalDistributionStrategy`, `AsyncConnectorSourceOperatorFactory`) stay — implementation details, not SPI.

---

## 4. How We Get There

The transformation is structured as eight PRs. Each PR is independently reviewable, shippable, and useful. Within each PR, individual commits follow these rules:

- **Smallest unit that stands on its own.** Compiles, tests pass, no dead code dangling.
- **Obvious value.** Either a new capability or a cleanup that makes the code clearly better. No "part 1 of 2" where part 1 is confusing alone.
- **No mixed concerns.** Renames go in their own commit, never mixed with behavior changes. New interfaces go in their own commit, separate from migrations to use them.
- **Good state at every point.** You can stop after any commit and the codebase is better than before.

### PR 1: Wire the Base Contract

**Goal:** Every data source participates in every pipeline stage. No `instanceof` branching.

**Commit 1: Add `createOperatorFactory(ctx)` to `ExternalSourceFactory` and eliminate the instanceof fork**

Add the new method as a `default` on `ExternalSourceFactory` that delegates to the existing `operatorFactory().create(ctx)` — so all file-based sources get the right behavior without changes. Override it in `ConnectorFactory` to encapsulate the `open()` → `AsyncConnectorSourceOperatorFactory` wiring that currently lives in `OperatorFactoryRegistry`. Change the registry to call `factory.createOperatorFactory(ctx)` — one line, no `instanceof`. Deprecate `operatorFactory()`.

*Value: the instanceof fork is gone. The registry no longer knows the difference between file and connector sources. Every new source type automatically works.*

**Commit 2: Wire connector splits into `SplitDiscoveryPhase`**

Add a `default splitProvider()` implementation on `ConnectorFactory` that delegates to `Connector.discoverSplits()`. No framework changes needed — `SplitDiscoveryPhase` already calls `factory.splitProvider()`, it just used to get `SINGLE` from connectors. Now it gets real splits if the connector provides them.

*Value: Flight can return endpoint-based splits, JDBC can return range-based splits, and they flow through `AdaptiveStrategy` for cluster distribution — using infrastructure that already exists.*

**Commit 3: Add `optimizationRules()` to `ExternalSourceFactory` and `DataSourceOptimizer`**

Add `default List<Rule<LogicalPlan, LogicalPlan>> optimizationRules()` returning empty list to `ExternalSourceFactory`. Add `DataSourceOptimizer` (~50 lines) — collects rules from all factories, runs them as a post-pass after `LogicalPlanOptimizer`. Wire the call in `LogicalPlanOptimizer.optimize()`. No sources provide rules yet — this commit just adds the infrastructure.

*Value: the optimization hook exists and works. Any source can now provide pushdown rules. The mechanism is proven even though no source uses it yet — the next commit adds the first user.*

**Commit 4: Thread pushdown data through `QueryRequest`**

Add `@Nullable Object pushdownData` field to `QueryRequest`. Thread it from `ExternalSourceExec.pushedFilter` through `OperatorFactoryRegistry` into the request that connectors receive. This is a data-plumbing change — no behavior changes.

*Value: connectors can now receive the results of optimization rules at execution time. The full optimize → plan → execute pipeline is connected for connectors. Previously, even if a connector had filter pushdown, there was no way to deliver the pushed filter to `Connector.execute()`.*

### PR 2: Lakehouse Assembly

**Goal:** Unify file-based and catalog-based sources under a composable lakehouse model.

**Commit 5: Extract `Catalog` interface**

Create `Catalog` interface with `resolve(location, config) → CatalogMetadata` and `optimizationRules()`. Create `GlobCatalog` implementing it — extracts the file enumeration + schema-from-footer logic that currently lives scattered across file-based source factories. The new interface exists alongside the old code. No existing code is changed to use it yet.

*Value: the "location → files + schema" abstraction is now a named, testable component. `GlobCatalog` can be unit-tested independently of any source factory.*

**Commit 6: Create `LakehouseDataSource` abstract class**

Create `LakehouseDataSource` implementing `DataSourceFactory` by composing `catalog() + storageProvider() + formatReader()`. Wire `splitProvider()` → catalog file discovery, `createOperatorFactory()` → storage + format composition, `optimizationRules()` → catalog delegation. No sources extend it yet — the class exists and compiles.

*Value: the lakehouse composition model is real and testable. The three-dimensional composition (catalog × storage × format) is expressed in code, not just in documentation.*

**Commit 7: Migrate file-based sources to `LakehouseDataSource`**

Migrate existing file-based source factories (S3 Parquet, GCS NDJSON, etc.) to extend `LakehouseDataSource`, providing `GlobCatalog` as their catalog. Each migration is small — the source provides its storage provider and format reader, the base class handles the rest. Behavior is identical — this is a refactoring, not a feature change.

*Value: file-based sources are now expressed as catalog + storage + format. Adding a new storage provider (Azure) or format reader (Avro) works for all sources automatically.*

### PR 3: Connector Assembly

**Goal:** Standard assembly pattern for connection-based sources.

**Commit 8: Create `ConnectorDataSource` abstract class and migrate Flight**

Create `ConnectorDataSource` implementing `DataSourceFactory` by composing `openConnector()` with async wrapping. Wire `splitProvider()` → `connector.discoverSplits()`, `createOperatorFactory()` → open + `AsyncConnectorSourceOperatorFactory`. Migrate `FlightConnectorFactory` to extend it.

*Value: connector-based sources have a standard assembly pattern. The split and async wiring that was duplicated across the framework is now owned by the assembly class. Flight inherits distribution and optimization automatically.*

### PR 4: Clean Up Old Interface

**Goal:** Remove superseded methods, clean interface.

**Commit 9: Deprecate `operatorFactory()` and `filterPushdownSupport()` on `ExternalSourceFactory`**

Mark both methods `@Deprecated`. No source should be calling them directly anymore — `createOperatorFactory()` and `optimizationRules()` have replaced them. This is a documentation commit — annotations only, no behavior change.

*Value: clear signal to any external consumers that the old methods are going away. Compilation warnings guide migration.*

**Commit 10: Remove deprecated `operatorFactory()` and `filterPushdownSupport()`**

Remove the deprecated methods from `ExternalSourceFactory`. Remove any remaining call sites. `FilterPushdownSupport` stays as an internal utility class (used by Iceberg's optimization rule) but is no longer on the SPI interface.

*Value: `ExternalSourceFactory` has only methods that every subtype uses. No optional-return-null patterns.*

### PR 5: Rename `External*` → `DataSource*`

**Goal:** Names match reality.

**Commit 11: Rename `External*` → `DataSource*`**

Pure rename commit. `ExternalSourceFactory` → `DataSourceFactory`, `ExternalRelation` → `DataSourceRelation`, `ExternalSourceExec` → `DataSourceExec`, `ExternalSplit` → `DataSourceSplit`, `ExternalSourceResolver` → `DataSourceResolver`, `OperatorFactoryRegistry` → `DataSourceRegistry`, `ConnectorFactory` → `ConnectorDataSource`, `TableCatalog` → `Catalog`. Mechanical search-and-replace with compilation checks. No behavior changes whatsoever.

*Value: consistent naming across the SPI. These are data sources, not "external" things.*

### PR 6: Iceberg End-to-End

**Goal:** Connect Iceberg's existing pieces into a fully functional data source. By this point the SPI is clean, renamed, and proven with both lakehouse and connector assemblies.

**Commit 12: Create `IcebergCatalog` and migrate Iceberg to `LakehouseDataSource`**

Create `IcebergCatalog` wrapping the existing `IcebergTableCatalog` logic — `resolve()` delegates to `resolveTable()`, file discovery delegates to `planScan()`. Wire `IcebergPushdownFilters.convert()` into `optimizationRules()` as an Iceberg-specific optimization rule. Migrate the Iceberg data source to extend `LakehouseDataSource` with `IcebergCatalog`. This is the commit that makes Iceberg go from metadata-only to fully functional — `splitProvider()` now produces real splits from `planScan()`, `createOperatorFactory()` composes storage + format for actual data reading.

*Value: Iceberg can read data. Iceberg filter pushdown is wired. Iceberg gets distributed execution via splits. This is a major capability unlock — the pieces existed, they just weren't connected.*

### PR 7: `FROM datasource::expression` Syntax

**Goal:** Production query syntax for data sources.

**Commit 13: `FROM datasource::expression` syntax**

Parser change from `EXTERNAL "uri"` to `FROM type::expression`. Wire `DataSourceResolver` to handle the new syntax. Keep backward compatibility with `EXTERNAL` during transition.

*Value: production syntax. Users write `FROM s3::"s3://bucket/path"` or `FROM iceberg::mydb.logs` instead of `EXTERNAL "..."`. This is the user-visible change that makes data sources first-class.*

### PR 8: SQL Pushdown Proof of Concept

**Goal:** Validate the `optimizationRules()` mechanism end-to-end by building reusable SQL pushdown as the first non-trivial consumer.

**Commit 14: Add `SqlDialect` interface and `SqlAst`**

Create `SqlDialect` (quoting, type mapping, function names, LIMIT syntax) and `SqlAst` (minimal SQL AST: SELECT, FROM, WHERE, ORDER BY, LIMIT, GROUP BY, with `render(dialect)` producing SQL strings). No pushdown rule yet — just the building blocks.

*Value: SQL generation infrastructure exists and is testable. Validates that the building blocks for source-specific optimization are clean and composable.*

**Commit 15: Add `SqlPushdownRule`**

Create `SqlPushdownRule` — an optimization rule that walks the plan above a SQL source's `DataSourceRelation`, translates Filter/Limit/TopN/Aggregate/Project into `SqlAst`, and replaces the subtree with a node carrying the generated SQL. Uses `SqlDialect` for rendering.

*Value: first real optimization rule flowing through `DataSourceOptimizer`. Proves the `optimizationRules()` mechanism works end-to-end — from rule registration through plan transformation to execution.*

**Commit 16: Create `SqlDataSource` abstract class**

Create `SqlDataSource` extending `ConnectorDataSource`. It provides `optimizationRules()` → `SqlPushdownRule` and requires concrete subclasses to provide `dialect(): SqlDialect` and connection logic. This is the reusable base class for SQL-speaking sources.

*Value: validates the assembly pattern for connectors. Any SQL-speaking source gets full pushdown for free by extending this class and providing a dialect — proving that the layered composition model works in practice.*

### Summary

```
PR 1 — Wire the Base Contract (4 commits)
  1.  createOperatorFactory + eliminate instanceof fork
  2.  Wire connector splits into SplitDiscoveryPhase
  3.  optimizationRules() + DataSourceOptimizer
  4.  Thread pushdownData through QueryRequest

PR 2 — Lakehouse Assembly (3 commits)
  5.  Extract Catalog interface
  6.  Create LakehouseDataSource
  7.  Migrate file-based sources to LakehouseDataSource

PR 3 — Connector Assembly (1 commit)
  8.  Create ConnectorDataSource + migrate Flight

PR 4 — Clean Up Old Interface (2 commits)
  9.  Deprecate old methods
  10. Remove deprecated methods

PR 5 — Rename External* → DataSource* (1 commit)
  11. Rename External* → DataSource*

PR 6 — Iceberg End-to-End (1 commit)
  12. Create IcebergCatalog + migrate Iceberg (major capability unlock)

PR 7 — FROM datasource::expression Syntax (1 commit)
  13. FROM datasource::expression syntax

PR 8 — SQL Pushdown Proof of Concept (3 commits)
  14. SqlDialect + SqlAst
  15. SqlPushdownRule
  16. SqlDataSource abstract class
```

Each commit compiles, tests pass, delivers value, and doesn't mix concerns. You can stop after any PR and the codebase is in a good state. The most impactful single PR is #6 — that's where Iceberg goes from metadata-only to fully functional, building on a clean, renamed SPI with both assembly patterns already proven.

---

## 5. What This Proposal Does NOT Change

- **Plan node generics**: `ExternalRelation`/`ExternalSourceExec` stay generic (renamed but same shape). `Map<String, Object>` config, `Object` pushed filter, `List<ExternalSplit>` splits.
- **Distribution infrastructure**: `SplitDiscoveryPhase`, `AdaptiveStrategy`, `ExternalSliceQueue`, `DataNodeComputeHandler` — all stay. We wire all sources into existing infrastructure, not rebuild it.
- **Existing components**: `StorageProvider`, `FormatReader`, `Connector`, `ResultCursor` — unchanged. These are already the right abstractions at the component level.
- **Plugin lifecycle**: `DataSourceModule`, `DataSourcePlugin`, lazy loading — unchanged.
- **Existing sources**: Flight connector and all file-based sources continue to work through all phases. Changes are additive.

The theme: **one base contract, composable components, standard assemblies for common patterns — with the hierarchy emerging from shared behavior, not imposed taxonomy.**
