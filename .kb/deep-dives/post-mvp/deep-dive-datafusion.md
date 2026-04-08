# DataFusion Deep Dive: Architecture, Pushdowns & Capabilities

**Date:** 2026-03-04
**Sources:** Official docs (datafusion.apache.org), SIGMOD 2024 paper, GitHub, blog posts

---

## 1. Architecture Overview

### 1.1 What Is DataFusion
Apache DataFusion is a fast, extensible query engine written in Rust, using Apache Arrow as its in-memory format. It originated within the Arrow project and provides both SQL and DataFrame APIs.

**Source:** https://datafusion.apache.org/user-guide/introduction.html

### 1.2 Design Philosophy
- **Embeddable**: Use as a library, not a standalone database
- **Extensible**: 10+ major extension APIs for customization
- **Modular**: Replace any component independently
- **Arrow-native**: RecordBatch is the universal data unit

### 1.3 Component Architecture
```
SQL / DataFrame API
        │
        ▼
   SQL Parser (sqlparser-rs)
        │
        ▼
   Logical Planner
        │
        ▼
   Logical Optimizer (rule-based, multiple passes)
        │
        ▼
   Physical Planner
        │
        ▼
   Physical Optimizer (rule-based)
        │
        ▼
   Execution Engine (streaming, vectorized, multi-threaded)
        │
        ▼
   Arrow RecordBatches (results)
```

### 1.4 SIGMOD 2024 Paper
"Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine"
**Source:** https://dl.acm.org/doi/10.1145/3626246.3653368

---

## 2. Data Source Support

### 2.1 Native Formats
- **Parquet**: Full support with rich pushdown
- **CSV**: Basic reading, column selection
- **JSON**: NDJSON reading
- **Avro**: Reading support
- **Arrow IPC**: Native format reading

### 2.2 Cloud Storage via ObjectStore
- AWS S3
- Azure Blob Storage
- Google Cloud Storage
- Local filesystem
- In-memory (for testing)

The `object_store` crate (in arrow-rs repo) provides the abstraction:
```rust
trait ObjectStore: Send + Sync {
    async fn get(&self, location: &Path) -> Result<GetResult>;
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes>;
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult>;
    async fn list(&self, prefix: Option<&Path>) -> BoxStream<Result<ObjectMeta>>;
}
```

### 2.3 Custom Sources via TableProvider
```rust
trait TableProvider: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn table_type(&self) -> TableType;
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>;
}
```

**Key**: `supports_filters_pushdown()` tells the optimizer which predicates can be handled at the source level.

**Source:** https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html

---

## 3. Pushdown Capabilities (KEY FOCUS)

### 3.1 Projection Pushdown
- **Layer**: Logical optimizer rule
- **What**: Eliminates unneeded columns as early as possible
- **For Parquet**: Only reads column chunks that are needed
- **For CSV/JSON**: Can select columns during parsing
- **Universal**: Works for all TableProvider implementations

### 3.2 Filter/Predicate Pushdown
- **Layer**: Logical optimizer rule + Physical execution
- **What**: Moves filter predicates down toward the source

#### For Parquet (Multi-Stage Pipeline):

**Stage 1 — Row Group Pruning (metadata)**:
- Uses min/max statistics per row group per column
- Example: `WHERE a > 10` skips row groups where max(a) <= 10
- Also uses Bloom filters when available (equality predicates)

**Stage 2 — Page-Level Pruning (metadata)**:
- Uses page-level column statistics (optional Parquet feature)
- Finer granularity than row groups

**Stage 3 — Filter Pushdown / Late Materialization (row-level)**:
- Evaluates filters during Parquet decoding
- Only decodes matching rows from non-filter columns
- Two-phase approach: build boolean mask from filter columns, then selectively decode others
- Configuration: `datafusion.execution.parquet.pushdown_filters`
- **Status**: Implemented but NOT enabled by default due to some performance regressions
- Optimized single-pass pipeline: interleaves filter and decode, caches at most 2 pages per column
- 15% total time reduction in ClickBench, up to 2.24x speedup on selective queries

**Source:** https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
**Source:** https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/

#### For CSV/JSON:
- **No metadata-based pruning** (no statistics)
- **Projection pushdown only** (column selection)
- Filter applied after reading (in-engine)

### 3.3 Limit Pushdown
- **Layer**: Logical optimizer rule
- **What**: Pushes max row counts downward
- Enables specialized limited implementations (e.g., TopK instead of Sort+Limit)

### 3.4 Aggregate Pushdown
- Not pushed to source level by default
- Engine-level aggregation on Arrow RecordBatches
- Statistics-based optimization can answer COUNT(*) from table stats without data access

### 3.5 Dynamic Filter Pushdown (NEW, 2024-2025)
- Runtime filters from hash joins pushed down to source reads
- Example: In a join, if one side has few matching values, those values become a filter on the other side's scan
- "Extended to support inner hash joins, dramatically improving performance"

**Source:** https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/

### 3.6 Which Layer Handles What (CRITICAL COMPARISON)

| Optimization | DataFusion Layer | ES|QL Equivalent |
|-------------|------------------|------------------|
| Projection pushdown | Logical optimizer | Physical optimizer (InsertFieldExtraction) |
| Filter pushdown (logical) | Logical optimizer | Logical optimizer (PushDownAndCombineFilters) |
| Filter pushdown (to Lucene) | N/A | Physical optimizer (PushFiltersToSource) |
| Filter pushdown (to Parquet) | Physical execution (RowFilter) | Not applicable (reads Parquet natively) |
| Row group pruning | ParquetExec (metadata) | N/A (Lucene handles this) |
| Limit pushdown | Logical optimizer | Physical optimizer (PushLimitToSource) |
| Sort+Limit → TopN | Physical optimizer | Logical optimizer (CombineLimitTopN) |
| Dynamic filter pushdown | Physical execution | Not implemented |

---

## 4. Optimization Layers

### 4.1 Logical Optimizer Rules
"Always optimizations" — present in virtually all query engines:

1. **Filter Pushdown** — move filters early to minimize row processing
2. **Projection Pushdown** — eliminate unneeded columns ASAP
3. **Limit Pushdown** — push max row counts down
4. **Expression Simplification & Constant Folding** — pre-evaluate constants
5. **OUTER JOIN → INNER JOIN Elimination** — when NULLs would be filtered anyway
6. **Common Subexpression Elimination** — cache repeated expressions
7. **Subquery Rewriting** — transform subqueries into joins (SEMI JOIN, ANTI JOIN)

**Source:** https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-two/

### 4.2 Physical Optimizer Rules

1. **Algorithm Selection** — TopK instead of Sort+Limit, sorted vs hash grouping
2. **Sort and Distribution Optimization** — minimize data shuffling
3. **Statistics-Based Optimization** — answer queries from stats without data access
4. **Join Order Optimization** — basic reordering (syntactic, not cost-based)

### 4.3 Cost-Based vs Rule-Based

DataFusion deliberately avoids sophisticated cost-based optimization:
- "Syntactic optimizer" respecting query-specified join order
- Basic join reordering to prevent disasters
- Framework for ColumnStatistics and Table Statistics
- Extension APIs for custom optimizers

Rationale: "Any one particular set of heuristics and cost model is unlikely to work well for the wide variety of DataFusion users."

**Source:** https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-two/

### 4.4 Comparison with ES|QL Optimizer

| Aspect | DataFusion | ES|QL |
|--------|-----------|-------|
| Logical rules | ~20+ rules | ~66 rules |
| Physical rules | ~10 rules | ~15 rules (mostly Lucene-specific) |
| Approach | Rule-based | Rule-based |
| Fixed-point iteration | Yes (max 16 passes) | Yes (ONCE or FIXED_POINT) |
| Cost-based | Minimal (statistics framework) | None |
| Source-specific rules | Via TableProvider | LucenePushdownPredicates |
| Extensibility | Plugin any rule | Harder to extend |

---

## 5. Execution Model

### 5.1 Streaming Vectorized Execution
- **Pull-based**: Downstream operators pull RecordBatches from upstream
- **Streaming**: Data flows through pipeline without full materialization
- **Vectorized**: Operations on Arrow RecordBatches (column-at-a-time)
- **Multi-threaded**: Partitioned execution across threads

### 5.2 ExecutionPlan Trait
```rust
trait ExecutionPlan: Send + Sync {
    fn execute(&self, partition: usize, context: Arc<TaskContext>) 
        -> Result<SendableRecordBatchStream>;
    fn output_partitioning(&self) -> Partitioning;
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>;
}
```

### 5.3 Partitioning & Parallelism
- Input files/partitions → output partitions
- RepartitionExec for shuffling
- CoalescePartitionsExec for merging
- Hash partitioning for joins

### 5.4 Memory Management
- Arrow RecordBatches with configurable batch sizes
- Memory pool tracking and limits
- Spill-to-disk for large aggregations/sorts (via external sorter)

---

## 6. DataFusion as Embeddable Engine

### 6.1 Embedding Use Cases
- **As SQL engine**: Embed full SQL in a Rust application
- **As query planner**: Use optimizer, replace execution
- **As execution engine**: Use executor, replace planner
- **As building block**: Mix and match components

### 6.2 Known Users (40+ production systems)
| System | Use Case |
|--------|----------|
| **InfluxDB 3.0** | Time-series database |
| **Comet** | Spark accelerator |
| **GlareDB** | Multi-source analytics |
| **GreptimeDB** | Time-series database |
| **Delta Lake (delta-rs)** | Lakehouse table format |
| **Iceberg-rust** | Lakehouse table format |
| **Ballista** | Distributed compute |
| **Arroyo** | Stream processing |
| **Sail** | Spark-compatible engine |
| **VegaFusion** | Visualization analytics |

**Source:** https://datafusion.apache.org/user-guide/introduction.html

### 6.3 DataFusion vs DuckDB vs Velox

| Aspect | DataFusion | DuckDB | Velox |
|--------|-----------|--------|-------|
| **Language** | Rust | C++ | C++ |
| **Primary use** | Embeddable engine / building block | Embedded analytical database | Execution engine toolkit |
| **Target user** | Engine builders | End users / analysts | Engine builders (Presto/Spark) |
| **Format** | Arrow RecordBatch | Custom + Parquet | Arrow-compatible vectors |
| **Extensibility** | Very high (10+ APIs) | Moderate | High (toolkit) |
| **SQL completeness** | Good (growing) | Excellent | No SQL (execution only) |
| **Performance** | Fastest on Parquet ClickBench (Nov 2024) | Fastest overall on many benchmarks | Very fast execution |
| **License** | Apache 2.0 | MIT | Apache 2.0 |

**Source:** https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/

### 6.4 Java/JVM Interop
- DataFusion is Rust-only, no official Java bindings
- Community project `datafusion-java` (JNI) exists but not production-ready
- For JVM: better to use Arrow Java + custom engine, or use DataFusion via Flight SQL

---

## 7. Roadmap & Direction

### 7.1 Recent Developments
- v50.0.0 released Sept 2025 — dynamic filter pushdown, performance improvements
- Fastest single-node Parquet engine on ClickBench (Nov 2024)
- 30%+ performance improvement between v34 and v43
- Substrait support for cross-engine query plan compatibility
- Growing ecosystem of downstream projects

### 7.2 Community Health
- Top-level Apache project
- Very active (multiple releases per month in arrow-rs)
- 40+ known production users
- Strong Rust ecosystem momentum

### 7.3 Where It's Heading
- Better cost-based optimization (statistics framework evolving)
- More pushdown capabilities (dynamic filters, aggregate pushdown)
- External index support for Parquet (bloom filters, user-defined indexes)
- Streaming/incremental execution improvements
- Cross-engine compatibility via Substrait

**Source:** https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/
