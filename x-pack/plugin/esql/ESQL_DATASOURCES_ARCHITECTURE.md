# ES|QL External Data Sources — System Design

**Author:** Costin Leau | **Date:** March 2026 | **Status:** Living Document

---

## 1. Architecture Overview

ES|QL external data sources allow queries to read directly from files (Parquet, CSV, ORC, NDJSON) and table systems (Iceberg, Flight) alongside native Elasticsearch indices. The architecture is built on a pluggable SPI that keeps the core query engine format-agnostic.

```
                           ┌──────────────────────────────────────────────────────┐
                           │                    COORDINATOR                       │
                           │                                                      │
  FROM "s3://…/*.parquet"  │  ┌─────────┐    ┌──────────┐    ┌───────────────┐   │
  WHERE ts > now() - 1d    │  │  Parse & │───▶│ Metadata │───▶│   Physical    │   │
  | STATS count(*)         │  │ Analyze  │    │ Resolve  │    │   Planning    │   │
                           │  └─────────┘    └──────────┘    └───────┬───────┘   │
                           │                                         │           │
                           │                 ┌───────────────────────▼────────┐   │
                           │                 │     Split Discovery &          │   │
                           │                 │     Distribution Strategy      │   │
                           │                 └──┬────────────┬────────────┬──┘   │
                           └────────────────────┼────────────┼────────────┼──────┘
                                    ┌───────────▼──┐  ┌──────▼──────┐  ┌─▼──────────┐
                                    │  Data Node 0 │  │ Data Node 1 │  │ Data Node 2│
                                    │              │  │             │  │            │
                                    │ Local Optim. │  │ Local Optim.│  │ Local Opt. │
                                    │ Filter Push  │  │ Filter Push │  │ Filter Push│
                                    │ Read Splits  │  │ Read Splits │  │ Read Splits│
                                    │              │  │             │  │            │
                                    └──────┬───────┘  └──────┬──────┘  └─────┬──────┘
                                           │                 │               │
                                           └────────────┬────┘               │
                                      Exchange ────────▶│◀───────────────────┘
                                                        ▼
                                                   ┌─────────┐
                                                   │  Final   │
                                                   │  Merge   │
                                                   └─────────┘
```

The system is decomposed into **four functional areas**, each described below.

---

## 2. Metadata Resolution

Metadata resolution runs **asynchronously on the coordinator**, in parallel with ES index field-caps resolution, so neither blocks the other.

```
  ┌─────────────────────┐          ┌───────────────────────┐
  │   Index Resolver     │          │ External Source        │
  │ (FieldCapsRequest)  │  async   │ Resolver               │
  │                     ├──────────┤                        │
  │  ES index schemas   │  parallel│  Glob expand paths     │
  │                     │          │  Open first file        │
  └─────────┬───────────┘          │  Read footer/schema    │
            │                      │  Extract statistics     │
            │                      └───────────┬────────────┘
            └──────────┬───────────────────────┘
                       ▼
                Unified Schema (List<Attribute>)
```

**How it works.** The `ExternalSourceResolver` iterates registered `ExternalSourceFactory` implementations to find one that can handle the given path. The matching factory delegates to a `FormatReader` which opens the file (e.g., reads the Parquet footer), extracts the schema and column/row-group statistics, and returns a `SourceMetadata` object.

For multi-file sources (globs, comma-separated paths), a `GlobExpander` resolves all matching files via the `StorageProvider`. Schema is taken from the first file (future: strict/union modes). Hive partition columns discovered from directory structure are appended to the schema.

**Trade-offs.**
- *First-file-wins schema* is fast (one file open) but can miss schema drift across files — acceptable for the common case; strict mode planned for safety-critical workloads.
- *Parallel resolution* adds complexity but eliminates the latency penalty of external I/O on the planning critical path.
- *Statistics at planning time* enable aggregate pushdown (see §5) but require reading file metadata eagerly — a worthwhile cost since the footer is small relative to data.

---

## 3. Query Execution Pipeline

A query flows through three plan representations, each adding execution detail:

```
  Logical Plan                Physical Plan              Operators
  ─────────────              ──────────────              ─────────
  ExternalRelation    ──▶    ExternalSourceExec    ──▶   AsyncExternalSourceOperator
    sourcePath                 + splits                    + FormatReader
    schema                     + config                    + StorageProvider
    statistics                 + pushed filter/limit       + SliceQueue
```

**ExternalSourceExec** is the single physical node for all external sources. It carries generic `Map<String, Object>` for both configuration and source-specific metadata — this keeps the core planner decoupled from format libraries (Parquet, Arrow, AWS SDK).

**Key design decision: local-only optimization hints.** Pushed filters and limits are **not serialized** across nodes. They are opaque objects (e.g., Parquet `FilterPredicate`) created during `LocalPhysicalPlanOptimizer` on each data node. This avoids serialization of format-specific types and ensures each node applies optimizations independently.

**Operator model.** The operator factory creates either:
- **Sync operators** — for simple formats (CSV, NDJSON) where the reader returns `Iterator<Page>` directly.
- **Async operators** — for formats with expensive I/O (Parquet, remote storage). A background thread reads pages into an `AsyncExternalSourceBuffer` with byte-based backpressure; the driver polls without blocking.

**Performance characteristics.**
- Backpressure buffer defaults to ~640KB (10 × page size), bounding memory per operator while keeping the pipeline fed.
- For multi-split queries, a `SliceQueue` lets multiple driver threads claim splits work-stealing style — no central scheduling overhead.

---

## 4. Dispatch & Distribution

After planning, the coordinator decides **where** to execute external source reads.

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │                    Distribution Decision                             │
  │                                                                      │
  │   Splits discovered  ──▶  Strategy evaluates:                       │
  │                           • Split count vs. node count              │
  │                           • Query shape (has aggregates? limit?)    │
  │                           • Split sizes (weighted assignment?)      │
  │                                                                      │
  │   ┌─────────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
  │   │  Coordinator-   │  │  Round-Robin  │  │  Weighted Round-Robin │  │
  │   │  Only            │  │              │  │  (LPT algorithm)      │  │
  │   │                 │  │  Even split   │  │  Largest splits first,│  │
  │   │  Single split   │  │  cycling      │  │  assigned to least-   │  │
  │   │  or LIMIT-only  │  │              │  │  loaded node           │  │
  │   └─────────────────┘  └──────────────┘  └───────────────────────┘  │
  │                                                                      │
  │                     ▲ Adaptive (default): picks based on query ▲     │
  └──────────────────────────────────────────────────────────────────────┘
```

**Split discovery** runs on the coordinator. The `SplitProvider` converts the resolved `FileSet` into parallelizable units — one per file for small files, or multiple per file aligned to Parquet row-groups / ORC stripes for large files. Ancestor filter predicates are evaluated against partition values for **L1 partition pruning** (skip entire files before reading).

**Distribution uses the standard Exchange.** Once splits are assigned, the coordinator sends each data node a serialized plan with its split subset. Data nodes execute locally, write pages to `ExchangeSinkExec`, and the coordinator collects via `ExchangeSourceExec` — the **same exchange infrastructure** used for ES index queries. No special data path.

**Trade-offs.**
- *Adaptive strategy* avoids distribution overhead for trivial queries (single file, LIMIT 10) while parallelizing large scans — good default, overridable via query pragma.
- *Weighted assignment* requires all splits to report size; falls back to round-robin otherwise. Split sizes come from file metadata (cheap) but may not reflect actual read cost (compression ratio, filter selectivity).
- *Partial results*: if a data node fails, the query can optionally return partial results (configurable) rather than failing entirely — important for large multi-file scans.

---

## 5. Pushdown Optimizations

Three pushdown optimizations reduce I/O and computation, applied during `LocalPhysicalPlanOptimizer` on each data node:

| Optimization | What Gets Pushed | Where It Runs | Format Support |
|---|---|---|---|
| **Filter pushdown** | WHERE predicates → row-group/stripe skip | FormatReader via FilterPredicate | Parquet (stats, bloom, dict), ORC |
| **Limit pushdown** | LIMIT N → early termination | FormatReader stops after N rows | All formats |
| **Aggregate pushdown** | COUNT/MIN/MAX → file statistics | Replaces AggregateExec with constants | Parquet, ORC (stats required) |

```
  Before pushdown:                    After pushdown:

  LimitExec(100)                      LimitExec(100)
    FilterExec(ts > X)                  FilterExec(ts > X)          ← safety net
      AggregateExec(count)                ExternalSourceExec
        ExternalSourceExec                  pushedFilter: ts > X    ← row-group skip
                                            pushedLimit: 100        ← early stop

  Aggregate-only case:
  AggregateExec(count(*))             LocalSourceExec
    ExternalSourceExec         ──▶      [single page: count=1_000_000]
      statistics.rowCount=1M            (no file reads at all)
```

**Filter pushdown uses RECHECK semantics**: filters are pushed to the source for row-group skipping but **remain** in the `FilterExec` node for correctness — the source may return false positives (e.g., a row within a matching row-group that doesn't actually satisfy the predicate). This is a deliberate correctness-over-performance trade-off: the source does coarse pruning, the engine does exact filtering.

**Aggregate pushdown from statistics** is the most aggressive optimization: if the query is an ungrouped `COUNT(*)`, `MIN(col)`, or `MAX(col)` and file-level statistics are available, the entire scan is eliminated — replaced with a constant-value `LocalSourceExec`. For distributed queries, this produces intermediate aggregation values (value + seen-boolean) that the coordinator merges.

---

## 6. Plugin SPI

The system is composed through four SPI interfaces, each responsible for one concern:

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  DataSourcePlugin (entry point, registered via ES plugin system) │
  │                                                                  │
  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
  │  │StorageProvider│  │ FormatReader  │  │ExternalSourceFactory   │ │
  │  │              │  │              │  │  (unified: connectors, │ │
  │  │ s3://, gs://, │  │ .parquet,    │  │   catalogs, files)    │ │
  │  │ http://, file │  │ .csv, .orc,  │  │                       │ │
  │  │              │  │ .ndjson      │  │  resolveMetadata()     │ │
  │  │ newObject()  │  │ read()       │  │  splitProvider()       │ │
  │  │ listObjects()│  │ metadata()   │  │  operatorFactory()     │ │
  │  └──────────────┘  └──────────────┘  └────────────────────────┘ │
  └──────────────────────────────────────────────────────────────────┘

  Composition: StorageProvider + FormatReader = FileSourceFactory
               (handles any scheme × format combination)
```

**Lazy loading.** Plugins declare capabilities (supported schemes, formats) without loading heavy dependencies. Actual class loading (Parquet, Arrow, AWS SDK) happens only when a query first uses that format — via `LazyPluginState` wrappers in `DataSourceModule`.

**Generic configuration.** All source-specific config flows as `Map<String, Object>` — the core engine never imports S3Configuration, IcebergConfig, etc. Only the plugin interprets its own keys. This keeps the module dependency graph clean and enables new sources without modifying core.
