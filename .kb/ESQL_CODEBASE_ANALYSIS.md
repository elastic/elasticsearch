# ESQL Codebase Analysis

This document provides a comprehensive analysis of the Elasticsearch ESQL codebase located in `x-pack/plugin/esql`.

## Table of Contents

1. [High-Level Architecture](#high-level-architecture)
2. [Directory Structure](#directory-structure)
3. [Query Processing Pipeline](#query-processing-pipeline)
4. [Planner](#planner)
5. [Compute Engine](#compute-engine)
6. [Commands Implementation](#commands-implementation)
7. [Functions Implementation](#functions-implementation)
8. [Package Reference](#package-reference)

---

## High-Level Architecture

ESQL is a distributed query execution engine following a classic compiler architecture with distinct phases:

```
Query String → Parser → Analyzer → Optimizer → Planner → Compute Engine → Results
```

The codebase is organized into three major subsystems:

1. **Planner**: Transforms queries from text to optimized execution plans (parsing, analysis, logical/physical optimization)
2. **Compute Engine**: Executes physical plans using columnar, page-based processing with SIMD optimizations
3. **Commands & Functions**: Implementations of ESQL language constructs (FROM, WHERE, EVAL, STATS, etc.) and built-in functions

---

## Directory Structure

```
x-pack/plugin/esql/
├── src/main/java/org/elasticsearch/xpack/esql/
│   ├── action/           # REST API and transport actions
│   ├── analysis/         # Semantic analysis and verification
│   ├── enrich/           # Data enrichment functionality
│   ├── execution/        # Query execution coordination
│   ├── expression/       # Expressions and functions
│   ├── inference/        # ML inference support
│   ├── optimizer/        # Logical and physical optimization
│   ├── parser/           # ANTLR-based query parsing
│   ├── plan/             # Logical and physical plan nodes
│   ├── planner/          # Plan conversion and mapping
│   ├── plugin/           # Elasticsearch plugin integration
│   ├── session/          # Session and configuration management
│   └── type/             # ESQL type system
├── compute/              # Compute engine (separate module)
│   └── src/main/java/org/elasticsearch/compute/
│       ├── aggregation/  # Aggregation framework
│       ├── data/         # Block and Page data structures
│       ├── lucene/       # Lucene integration
│       └── operator/     # Execution operators
├── arrow/                # Apache Arrow integration
└── qa/                   # Integration tests
```

---

## Query Processing Pipeline

### End-to-End Data Flow

```
1. RAW QUERY STRING
   ↓
2. PARSER (EsqlParser + LogicalPlanBuilder)
   - Tokenizes and parses ESQL syntax using ANTLR
   - Output: Parsed LogicalPlan (Stage = PARSED)
   ↓
3. ANALYZER (Analyzer.java)
   - Resolves field names against index mappings
   - Validates field types and expressions
   - Output: Analyzed LogicalPlan (Stage = ANALYZED)
   ↓
4. PRE-OPTIMIZER (LogicalPlanPreOptimizer)
   - Async operations (e.g., inference function folding)
   - Output: Pre-Optimized LogicalPlan
   ↓
5. LOGICAL OPTIMIZER (LogicalPlanOptimizer)
   - Phase 1: Substitutions (shorthand → explicit)
   - Phase 2: Operators (folding, combining, pushing down)
   - Phase 3: Cleanup (TopN conversion, redundancy removal)
   - Output: Optimized LogicalPlan
   ↓
6. MAPPER (mapper/Mapper.java)
   - Translates LogicalPlan → PhysicalPlan
   - Determines coordinator vs. data node split
   - Creates FragmentExec for distributed execution
   ↓
7. PHYSICAL OPTIMIZER (PhysicalPlanOptimizer)
   - Pushes operations into Elasticsearch source
   - Output: Lucene-optimized PhysicalPlan
   ↓
8. LOCAL EXECUTION PLANNER (LocalExecutionPlanner)
   - Converts PhysicalPlan → Driver + Operators
   - Output: Executable Drivers
   ↓
9. COMPUTE ENGINE (Driver + Operators)
   - Source → Intermediate → Sink operators
   - Processes data in columnar Pages
   - Output: Result rows
```

---

## Planner

The planner subsystem transforms ESQL queries into executable plans through multiple stages.

### Parser Package (`org.elasticsearch.xpack.esql.parser`)

The parser converts raw ESQL text into logical plans using ANTLR-generated lexer/parser.

| File | Description |
|------|-------------|
| `EsqlParser.java` | Main ANTLR-based parser entry point; validates query length and handles error reporting |
| `EsqlConfig.java` | Configuration settings for parser initialization |
| `LogicalPlanBuilder.java` | Converts ANTLR parse tree into LogicalPlan objects using visitor pattern |
| `ExpressionBuilder.java` | Builds expression nodes from parse tree for operators, functions, and literals |
| `PlanFactory.java` | Factory for creating plan nodes with composable transformations |
| `ParserUtils.java` | Utility methods for source location tracking and name resolution |
| `AbstractBuilder.java` | Base class for builders with common functionality |
| `IdentifierBuilder.java` | Handles identifier resolution, aliases, and pattern matching |
| `LexerConfig.java` | Lexical analyzer configuration |
| `ParsingException.java` | Custom parsing error type with source location |
| `EsqlBaseLexer.java` | Generated ANTLR lexer for tokenization |
| `EsqlBaseParser.java` | Generated ANTLR parser for syntax analysis |

### Analysis Package (`org.elasticsearch.xpack.esql.analysis`)

The analysis layer validates and resolves the AST to typed, resolved logical plans.

| File | Description |
|------|-------------|
| `Analyzer.java` | Main semantic analyzer resolving references, types, and validating expressions |
| `Verifier.java` | Post-analysis verification checking license capabilities and constraints |
| `PreAnalyzer.java` | Pre-analysis phase resolving indices, enrich policies, and views |
| `AnalyzerRules.java` | Analysis transformation rules for resolution |
| `EnrichResolution.java` | Resolves ENRICH statement policies and mappings |
| `UnmappedResolution.java` | Handles unmapped field references |
| `TimeSeriesGroupByAll.java` | Time series aggregation default grouping |
| `InsertDefaultInnerTimeSeriesAggregate.java` | Inserts default time series aggregations |

### Logical Plan Package (`org.elasticsearch.xpack.esql.plan.logical`)

High-level query plan representation with 48+ logical operators representing "what" the user wants.

| File | Description |
|------|-------------|
| `LogicalPlan.java` | Base class with stage tracking (PARSED→ANALYZED→OPTIMIZED) |
| `LeafPlan.java` | Abstract base for data source nodes |
| `UnaryPlan.java` | Base for single-child operations |
| `BinaryPlan.java` | Base for two-child operations (joins) |
| `EsRelation.java` | Represents FROM clause querying Elasticsearch indices |
| `UnresolvedRelation.java` | Unresolved source reference before index resolution |
| `Row.java` | ROW command generating synthetic data from literals |
| `LocalRelation.java` | In-memory data source for local operations |
| `Filter.java` | WHERE clause filtering conditions |
| `Eval.java` | EVAL command computing derived columns |
| `Project.java` | Column selection and aliasing |
| `Aggregate.java` | STATS command with GROUP BY aggregations |
| `TimeSeriesAggregate.java` | Time-series specific aggregations |
| `OrderBy.java` | SORT command ordering rows |
| `Limit.java` | LIMIT command restricting row count |
| `TopN.java` | Combined sort + limit optimization |
| `Sample.java` | Statistical row sampling |
| `Drop.java` | Column removal |
| `Rename.java` | Column renaming |
| `Keep.java` | Column selection (inverse of DROP) |
| `Dissect.java` | Pattern-based text extraction |
| `Grok.java` | Grok pattern text parsing |
| `RegexExtract.java` | Base for regex extraction operations |
| `MvExpand.java` | Multi-value field expansion |
| `Enrich.java` | Data enrichment from external indices |
| `Lookup.java` | Lookup joins with external data |
| `InlineStats.java` | Inline statistics computation |
| `ChangePoint.java` | Change point detection |
| `Fork.java` | Query branching |
| `Subquery.java` | Subquery handling |

**Interfaces:**
| Interface | Description |
|-----------|-------------|
| `Streaming.java` | Marker for row-at-a-time operations |
| `SortAgnostic.java` | Marker for operations indifferent to row order |
| `SortPreserving.java` | Marker for operations that maintain input sort order |
| `PipelineBreaker.java` | Marker for operations forcing materialization |
| `GeneratingPlan.java` | Marker for plans creating new output columns |

### Physical Plan Package (`org.elasticsearch.xpack.esql.plan.physical`)

Executable representation with 40+ physical operators representing "how" execution happens.

| File | Description |
|------|-------------|
| `PhysicalPlan.java` | Base class for physical execution plans |
| `LeafExec.java` | Base for source operators |
| `UnaryExec.java` | Base for single-input operators |
| `BinaryExec.java` | Base for two-input operators |
| `EsSourceExec.java` | Elasticsearch source data reading |
| `EsQueryExec.java` | Parameterized Elasticsearch query execution |
| `EsStatsQueryExec.java` | Aggregation pushed to Elasticsearch |
| `LocalSourceExec.java` | In-memory data source execution |
| `FieldExtractExec.java` | Field extraction from source/doc_values |
| `FilterExec.java` | WHERE clause execution |
| `EvalExec.java` | Expression evaluation execution |
| `ProjectExec.java` | Column projection execution |
| `AggregateExec.java` | Aggregation with INITIAL/INTERMEDIATE/FINAL modes |
| `TimeSeriesAggregateExec.java` | Time-series aggregation execution |
| `LimitExec.java` | Row limit execution |
| `TopNExec.java` | Top-N execution (optimized sort+limit) |
| `OrderExec.java` | Full sorting execution |
| `SampleExec.java` | Sampling execution |
| `MvExpandExec.java` | Multi-value expansion execution |
| `DissectExec.java` | Pattern extraction execution |
| `GrokExec.java` | Grok pattern execution |
| `RegexExtractExec.java` | Regex extraction execution |
| `EnrichExec.java` | Data enrichment execution |
| `HashJoinExec.java` | Hash join execution |
| `LookupJoinExec.java` | Lookup index join execution |
| `ExchangeExec.java` | Inter-node communication placeholder |
| `ExchangeSinkExec.java` | Result collection for inter-node transfers |
| `ExchangeSourceExec.java` | Result retrieval from inter-node transfers |
| `FragmentExec.java` | Logical plan fragment for distributed execution |
| `MergeExec.java` | Multi-source merge operator |
| `SubqueryExec.java` | Subquery execution wrapper |
| `OutputExec.java` | Final output consumer |
| `ShowExec.java` | SHOW command execution |
| `ChangePointExec.java` | Change point detection execution |

### Optimizer Package (`org.elasticsearch.xpack.esql.optimizer`)

Transforms plans into more efficient forms through rule-based transformations.

| File | Description |
|------|-------------|
| `LogicalPlanOptimizer.java` | Global logical optimization with substitutions, operators, and cleanup phases |
| `LogicalPlanPreOptimizer.java` | Pre-optimization for async operations |
| `LocalLogicalPlanOptimizer.java` | Data node-level logical optimization with index statistics |
| `PhysicalPlanOptimizer.java` | Global physical plan optimization |
| `LocalPhysicalPlanOptimizer.java` | Data node physical optimization pushing operations into Lucene |
| `LogicalVerifier.java` | Validates logical plan correctness |
| `PhysicalVerifier.java` | Validates physical plan correctness |
| `LogicalOptimizerContext.java` | Provides index metadata to global optimizer |
| `LocalLogicalOptimizerContext.java` | Index statistics context for data node optimizer |
| `PhysicalOptimizerContext.java` | Global physical optimization context |
| `LocalPhysicalOptimizerContext.java` | Data node physical optimization context |

**Logical Optimization Rules (`rules/logical/`):**
| Rule | Description |
|------|-------------|
| `PushDownAndCombineFilters.java` | Moves WHERE conditions toward sources and merges adjacent filters |
| `PushDownAndCombineLimits.java` | Combines and pushes LIMIT operations down |
| `PushDownAndCombineOrderBy.java` | Combines SORT operations and pushes them down |
| `PushDownEval.java` | Moves computed fields closer to data sources |
| `PushDownEnrich.java` | Optimizes enrichment operation placement |
| `CombineEvals.java` | Merges consecutive Eval nodes |
| `CombineProjections.java` | Fuses multiple projections |
| `CombineLimitTopN.java` | Converts SORT+LIMIT to optimized TopN |
| `ConstantFolding.java` | Evaluates constant expressions at plan time |
| `BooleanSimplification.java` | Simplifies boolean logic |
| `PropagateEquals.java` | Propagates equality constraints through plan |
| `PropagateEmptyRelation.java` | Short-circuits queries with no matching data |
| `PruneFilters.java` | Removes redundant WHERE conditions |
| `PruneColumns.java` | Removes unused columns from result sets |
| `PruneRedundantOrderBy.java` | Eliminates duplicate sorting |
| `FoldNull.java` | Evaluates NULL propagation |
| `SkipQueryOnLimitZero.java` | Eliminates queries with LIMIT 0 |

**Physical Optimization Rules (`rules/physical/local/`):**
| Rule | Description |
|------|-------------|
| `PushTopNToSource.java` | Pushes TopN operation into Elasticsearch source |
| `PushLimitToSource.java` | Pushes LIMIT into Elasticsearch source |
| `PushFiltersToSource.java` | Pushes WHERE conditions into Lucene queries |
| `PushStatsToSource.java` | Pushes aggregation into Elasticsearch |
| `InsertFieldExtraction.java` | Adds field extraction operators where needed |
| `LucenePushdownPredicates.java` | Translates predicates into Lucene queries |

### Planner Package (`org.elasticsearch.xpack.esql.planner`)

Converts optimized logical plans to executable physical operations.

| File | Description |
|------|-------------|
| `LocalExecutionPlanner.java` | Converts physical plans into Driver instances; implements visitor pattern for all plan node types |
| `mapper/Mapper.java` | Translates logical→physical plans at coordinator; determines distributed execution split |
| `mapper/LocalMapper.java` | Maps logical→physical plans at data nodes; creates EsSourceExec |
| `mapper/PreMapper.java` | Pre-mapping step for async operations |
| `Layout.java` | Maps attribute IDs to execution channels (block positions) |
| `PlannerUtils.java` | Utility methods for plan breaking and query translation |
| `PlannerSettings.java` | Configuration for planning behavior |
| `PhysicalOperationProviders.java` | Interface for creating physical operations |
| `AbstractPhysicalOperationProviders.java` | Base implementation with aggregation handling |
| `EsPhysicalOperationProviders.java` | Elasticsearch-specific operators |
| `TypeConverter.java` | Type conversion between ESQL and Elasticsearch types |
| `PlanConcurrencyCalculator.java` | Determines parallel execution opportunities |
| `ToAggregator.java` | Converts aggregate expressions to Aggregator implementations |
| `AggregateMapper.java` | Maps aggregate operations to execution strategy |

---

## Compute Engine

The compute engine is a columnar, page-based execution system located in `x-pack/plugin/esql/compute/`.

### Data Structures (`org.elasticsearch.compute.data`)

The data layer implements columnar storage with Blocks and Pages.

| File | Description |
|------|-------------|
| `Block.java` | Columnar representation of homogeneous data with position counts |
| `Vector.java` | Dense vector of single values per position, optimized fast path |
| `Page.java` | Column-oriented batch containing multiple Blocks |
| `BlockFactory.java` | Factory for creating typed blocks with circuit breaker tracking |
| `ElementType.java` | Enumeration of supported data types (INT, LONG, DOUBLE, BOOLEAN, BYTES_REF, etc.) |
| `LocalCircuitBreaker.java` | Thread-safe circuit breaker for per-driver memory limits |
| `BlockRamUsageEstimator.java` | Estimates memory footprint of Block objects |
| `BlockStreamInput.java` | Deserializes blocks from binary streams |
| `BlockUtils.java` | Helper functions for block manipulation and filtering |
| `AbstractArrayBlock.java` | Base for single-value array-backed blocks |
| `AbstractVectorBlock.java` | Base for blocks representing dense vectors |
| `AbstractBlockBuilder.java` | Base builder for constructing blocks incrementally |
| `CompositeBlock.java` | Combines multiple blocks into a compound structure |
| `ConstantNullBlock.java` | Represents all-null blocks efficiently |
| `DocBlock.java` | Specialized blocks for document IDs |
| `OrdinalBytesRefBlock.java` | Ordinal-encoded bytes reference blocks for deduplication |

### Operators (`org.elasticsearch.compute.operator`)

Operators form the execution pipeline processing Pages.

| File | Description |
|------|-------------|
| `Operator.java` | Core interface defining needsInput/addInput/getOutput/finish/isFinished contract |
| `SourceOperator.java` | Base class for operators that only produce output |
| `SinkOperator.java` | Base class for operators that only consume input |
| `Driver.java` | Single-threaded orchestrator managing operator chain and Page flow |
| `DriverContext.java` | Shared context across operators managing BlockFactory and resources |
| `DriverRunner.java` | Executes drivers in thread pools |
| `DriverScheduler.java` | Schedules driver work on executors with yield management |
| `FilterOperator.java` | Filters rows based on boolean expression evaluation |
| `EvalOperator.java` | Evaluates expression trees on each Page position |
| `ProjectOperator.java` | Selects specific columns from pages |
| `LimitOperator.java` | Limits output to N rows |
| `OutputOperator.java` | Terminal operator that consumes pages for output |
| `HashAggregationOperator.java` | GROUP BY aggregation using BlockHash for grouping |
| `AggregationOperator.java` | Non-grouped aggregation (e.g., COUNT(*)) |
| `TimeSeriesAggregationOperator.java` | Time-series aggregation with bucketing |
| `MvExpandOperator.java` | Expands multi-valued columns into separate rows |
| `ChangePointOperator.java` | Detects change points in time-series data |
| `SampleOperator.java` | Samples rows probabilistically |
| `AsyncOperator.java` | Wraps async operations for synchronous driver integration |
| `FailureCollector.java` | Collects and manages query execution errors |
| `Warnings.java` | Manages warning messages during execution |

**TopN Operations (`operator/topn/`):**
| File | Description |
|------|-------------|
| `TopNOperator.java` | Selects top N rows by sort keys using binary encoding |
| `SortableTopNEncoder.java` | Encodes sortable columns to binary format |
| `KeyExtractor.java` | Extracts sort key values from blocks |
| `ValueExtractor.java` | Extracts result values from blocks |
| `ResultBuilder.java` | Reconstructs blocks from encoded results |

**Exchange Operations (`operator/exchange/`):**
| File | Description |
|------|-------------|
| `ExchangeSourceOperator.java` | Source operator receiving pages from remote nodes |
| `ExchangeSinkOperator.java` | Sink operator sending pages to remote nodes |
| `ExchangeService.java` | Manages inter-node page exchange |
| `ExchangeBuffer.java` | Buffers pages during exchange |
| `ExchangeRequest.java` | Request messages for page transfer |
| `ExchangeResponse.java` | Response messages for page transfer |

**Lookup Operations (`operator/lookup/`):**
| File | Description |
|------|-------------|
| `EnrichQuerySourceOperator.java` | Loads enrichment data from external sources |
| `RightChunkedLeftJoin.java` | Implements chunked left-outer join semantics |
| `RowInTableLookupOperator.java` | Looks up rows in in-memory tables |

### Aggregation Framework (`org.elasticsearch.compute.aggregation`)

Implements the aggregation execution with support for partial/final modes.

| File | Description |
|------|-------------|
| `Aggregator.java` | Base interface for single aggregation implementations |
| `AggregatorFunction.java` | Function interface for aggregations with eval methods |
| `AggregatorFunctionSupplier.java` | Factory for creating aggregator functions |
| `AggregatorMode.java` | Enum: SINGLE, PARTIAL (first phase), FINAL (combining phase) |
| `AggregatorState.java` | Serializable state for intermediate aggregation results |
| `GroupingAggregator.java` | Wraps aggregation function with grouping support |
| `GroupingAggregatorFunction.java` | Interface for per-group aggregation functions |
| `FilteredAggregatorFunction.java` | Wraps aggregations with pre-filtering |
| `CountAggregatorFunction.java` | Count aggregation implementation |
| `SumDoubleAggregator.java` | Sum for double values |
| `MinMaxAggregator.java` | Min/Max implementations for all types |
| `PercentileAggregator.java` | Percentile calculations |
| `MedianAbsoluteDeviationAggregator.java` | MAD calculations |
| `CountDistinctAggregator.java` | Count distinct using HyperLogLog |
| `QuantileStates.java` | State for quantile calculations |
| `HllStates.java` | State for HyperLogLog cardinality estimation |

**BlockHash (`aggregation/blockhash/`):**
| File | Description |
|------|-------------|
| `BlockHash.java` | Abstract base implementing hash-based grouping |
| `AdaptiveBlockHash.java` | Dynamically selects optimal hash implementation |
| `BooleanBlockHash.java` | Hash for boolean columns (3 buckets) |
| `BytesRefBlockHash.java` | Hash for string columns |
| `LongLongBlockHash.java` | Hash for numeric key combinations |
| `PackedValuesBlockHash.java` | Compact binary encoding of hash keys |
| `TimeSeriesBlockHash.java` | Hash for time-series bucketing |

### Lucene Integration (`org.elasticsearch.compute.lucene`)

Integrates with Lucene for index reading.

| File | Description |
|------|-------------|
| `LuceneSourceOperator.java` | Incremental Lucene searcher producing pages of matching documents |
| `LuceneTopNSourceOperator.java` | Optimized source for TOP N queries |
| `TimeSeriesSourceOperator.java` | Specialized source for time-series indices |
| `LuceneOperator.java` | Base for Lucene-based operators |
| `LuceneCountOperator.java` | Specialized operator for COUNT queries |
| `LuceneMinMaxOperator.java` | MIN/MAX push-down optimizations |
| `LuceneSlice.java` | Represents a slice of index data |
| `LuceneSliceQueue.java` | Manages queue of slices with partitioning strategies |
| `ShardContext.java` | Context for single shard data access |
| `DataPartitioning.java` | Strategies for partitioning data across slices |

### Compute Engine Data Flow

```
1. Query Entry: PlanExecutor receives EsqlQueryRequest
   ↓
2. Plan to Operators: Query planner converts plans to operator chains
   ↓
3. Driver Creation: Each operator chain runs in a Driver instance
   ↓
4. Data Source: SourceOperators produce initial Pages
   - LuceneSourceOperator reads from indices
   - LocalSourceOperator provides in-memory data
   ↓
5. Page Processing:
   - Pages flow through operator pipeline in batches
   - Each operator: needsInput() → addInput() → getOutput()
   - Pages contain columnar Blocks matching input schema
   ↓
6. Intermediate Transformations:
   - Filter: Evaluates boolean expressions, returns position arrays
   - Eval: Appends computed columns via ExpressionEvaluator
   - Project: Selects columns
   ↓
7. Aggregation Pipeline:
   - HashAggregationOperator:
     - BlockHash hashes GROUP BY keys into bucket IDs
     - GroupingAggregator maintains state arrays by bucket
     - Partial results emitted when buffer fills
   - AggregatorMode: SINGLE, PARTIAL, FINAL
   ↓
8. Sorting: TopNOperator encodes keys, sorts, decodes top results
   ↓
9. Result Consolidation:
   - Local: Operators sink to memory or export
   - Distributed: ExchangeSource/ExchangeSink coordinate across nodes
   ↓
10. Driver Output: Pages returned until isFinished() = true
```

---

## Commands Implementation

ESQL commands are implemented as logical plan nodes that get transformed into physical execution.

### Source Commands

| Command | Logical Plan | Physical Plan | Description |
|---------|--------------|---------------|-------------|
| `FROM` | `EsRelation` | `EsSourceExec`/`EsQueryExec` | Query Elasticsearch indices |
| `ROW` | `Row` | `LocalSourceExec` | Generate synthetic rows from literals |
| `TS` | `EsRelation` (time-series mode) | `TimeSeriesAggregateExec` | Time-series index queries |

### Transformation Commands

| Command | Logical Plan | Physical Plan | Pipeline Type |
|---------|--------------|---------------|---------------|
| `WHERE` | `Filter` | `FilterExec` | Streaming |
| `EVAL` | `Eval` | `EvalExec` | Streaming |
| `KEEP` | `Keep`/`Project` | `ProjectExec` | Streaming |
| `DROP` | `Drop` | `ProjectExec` | Streaming |
| `RENAME` | `Rename` | `ProjectExec` | Streaming |
| `SORT` | `OrderBy` | `OrderExec`/`TopNExec` | Pipeline-breaking |
| `LIMIT` | `Limit` | `LimitExec`/`TopNExec` | Pipeline-breaking |
| `STATS` | `Aggregate` | `AggregateExec` | Pipeline-breaking |
| `MV_EXPAND` | `MvExpand` | `MvExpandExec` | Pipeline-breaking |
| `DISSECT` | `Dissect` | `DissectExec` | Streaming |
| `GROK` | `Grok` | `GrokExec` | Streaming |
| `ENRICH` | `Enrich` | `EnrichExec` | Streaming |
| `SAMPLE` | `Sample` | `SampleExec` | Pipeline-breaking |

### Command Execution Properties

Commands are marked with interfaces describing execution characteristics:

- **Streaming**: Single-pass, no materialization needed (Filter, Eval, Rename, Drop, Keep, Dissect, Grok)
- **SortAgnostic**: Order of input doesn't matter (Filter, Eval, Aggregate, Sample, MvExpand)
- **SortPreserving**: Input order preserved (Filter, Limit, Dissect, Grok, Rename, Drop)
- **PipelineBreaker**: Forces materialization (Aggregate, OrderBy, Limit, Sample, InlineStats, MvExpand, Enrich)
- **GeneratingPlan**: Creates new output columns (Eval, Enrich, Dissect, Grok)

---

## Functions Implementation

Functions are located in `org.elasticsearch.xpack.esql.expression.function/`.

### Function Registry

`EsqlFunctionRegistry.java` is the central hub for function management with 200+ registered functions:
- Registration via `FunctionDefinition` entries organized by category
- Resolution maps function names (normalized to lowercase) to definitions
- Handles aliases (e.g., "to_int" → "to_integer")
- Defines type casting priority for overload resolution

### Function Categories

**Aggregate Functions (`function/aggregate/`):**
| Function | Description |
|----------|-------------|
| `Avg.java` | Calculates average; decomposes into SUM/COUNT |
| `Count.java` | Counts rows or non-null values |
| `Sum.java` | Sums numeric values |
| `Min.java`, `Max.java` | Find minimum/maximum values |
| `Median.java` | Statistical median |
| `Percentile.java` | Compute percentiles |
| `CountDistinct.java` | Count unique values using HyperLogLog |
| `StdDev.java`, `Variance.java` | Statistical measures |
| `Top.java`, `Values.java` | Select top values or collect all |
| `Rate.java`, `Deriv.java` | Time-series rate calculations |
| `SpatialCentroid.java` | Spatial aggregations |

**Scalar Math Functions (`function/scalar/math/`):**
| Function | Description |
|----------|-------------|
| `Abs.java` | Absolute value |
| `Ceil.java`, `Floor.java`, `Round.java` | Rounding operations |
| `Sqrt.java`, `Pow.java`, `Exp.java`, `Log.java` | Mathematical operations |
| `Sin.java`, `Cos.java`, `Tan.java` | Trigonometric functions |
| `Greatest.java`, `Least.java` | Min/max comparisons |

**Scalar String Functions (`function/scalar/string/`):**
| Function | Description |
|----------|-------------|
| `Length.java` | Character length |
| `Substring.java`, `Left.java`, `Right.java` | String extraction |
| `Concat.java` | String concatenation |
| `ToUpper.java`, `ToLower.java` | Case conversion |
| `Trim.java`, `LTrim.java`, `RTrim.java` | Whitespace trimming |
| `Replace.java` | String replacement |
| `Split.java` | String splitting |
| `Contains.java`, `StartsWith.java`, `EndsWith.java` | String matching |

**Scalar Date Functions (`function/scalar/date/`):**
| Function | Description |
|----------|-------------|
| `DateParse.java` | Parse dates using format strings |
| `DateFormat.java` | Format dates with patterns |
| `DateDiff.java` | Calculate date differences |
| `DateExtract.java` | Extract date components |
| `DateTrunc.java` | Truncate dates to time unit |
| `Now.java` | Current timestamp |

**Conversion Functions (`function/scalar/convert/`):**
| Function | Description |
|----------|-------------|
| `ToDouble.java`, `ToInteger.java`, `ToLong.java` | Numeric conversions |
| `ToBoolean.java`, `ToString.java` | Type conversions |
| `ToDatetime.java` | Date conversions |
| `ToIp.java` | IP address conversion |
| `ToGeoPoint.java`, `ToGeoShape.java` | Geographic conversions |

**Multivalue Functions (`function/scalar/multivalue/`):**
| Function | Description |
|----------|-------------|
| `MvCount.java` | Count values in multivalue field |
| `MvFirst.java`, `MvLast.java` | Get first/last value |
| `MvMin.java`, `MvMax.java`, `MvAvg.java`, `MvSum.java` | Statistical operations on multivalue |
| `MvSort.java` | Sort multivalue arrays |
| `MvDedupe.java` | Remove duplicates |
| `MvAppend.java`, `MvConcat.java` | Combine multivalue fields |

**Full-Text Functions (`function/fulltext/`):**
| Function | Description |
|----------|-------------|
| `Match.java` | Full-text match queries |
| `MatchPhrase.java` | Phrase matching |
| `MultiMatch.java` | Match across multiple fields |
| `QueryString.java` | Query string syntax |
| `Kql.java` | Kibana Query Language support |

**Vector Functions (`function/vector/`):**
| Function | Description |
|----------|-------------|
| `CosineSimilarity.java` | Cosine similarity |
| `DotProduct.java` | Dot product |
| `L1Norm.java`, `L2Norm.java` | Distance calculations |
| `Knn.java` | K-nearest neighbors search |

**Grouping Functions (`function/grouping/`):**
| Function | Description |
|----------|-------------|
| `Bucket.java` | Bucketing for numeric/date ranges |
| `Categorize.java` | Text categorization |

### Function Resolution Flow

```
1. Parsing: "avg(salary)"
   → UnresolvedFunction("avg", [salary_expression])

2. Analysis:
   → Registry.resolveFunction("avg")
   → FunctionDefinition for Avg class found
   → Avg constructor called with type validation
   → Returns resolved Avg instance

3. Type Resolution:
   → Avg.resolveType() validates:
     - Input is numeric
     - Returns DOUBLE type

4. Optimization (Surrogate Expression):
   → Avg.surrogate() → Div(Sum(...), Count(...))
   → Enables efficient distributed aggregation

5. Evaluation:
   → @Evaluator annotation generates efficient code
   → Block-level and Vector-level evaluators created
```

---

## Package Reference

### Main Plugin Package (`org.elasticsearch.xpack.esql`)

| Package | Description |
|---------|-------------|
| `action/` | REST API handlers and transport actions for ESQL queries, including async query support and capabilities reporting |
| `analysis/` | Semantic analysis resolving field references, validating types, and verifying query correctness against index mappings |
| `enrich/` | Data enrichment functionality for joining external data sources based on match fields |
| `execution/` | Query execution coordination including PlanExecutor entry point and result handling |
| `expression/` | Expression system including all function implementations, predicates, and attribute handling |
| `expression/function/` | Function implementations organized by category (aggregate, scalar/math, scalar/string, scalar/date, etc.) |
| `inference/` | Machine learning inference support for embeddings, completion, and reranking |
| `optimizer/` | Logical and physical plan optimization with 50+ transformation rules |
| `optimizer/rules/logical/` | Logical optimization rules (push-down, folding, pruning, combining) |
| `optimizer/rules/physical/` | Physical optimization rules (Lucene push-down, field extraction) |
| `parser/` | ANTLR-based query parsing converting text to logical plans |
| `plan/logical/` | 48+ logical plan node types representing query intent |
| `plan/physical/` | 40+ physical plan node types representing execution strategy |
| `planner/` | Plan conversion from logical to physical with distributed execution planning |
| `plugin/` | Elasticsearch plugin integration, transport actions, and cluster coordination |
| `session/` | Session management, configuration, and index resolution |
| `type/` | ESQL type system definitions |

### Compute Engine Package (`org.elasticsearch.compute`)

| Package | Description |
|---------|-------------|
| `aggregation/` | Aggregation framework with Aggregator interfaces, state management, and mode support (SINGLE/PARTIAL/FINAL) |
| `aggregation/blockhash/` | Hash table implementations for GROUP BY operations with type-specific variants |
| `aggregation/spatial/` | Spatial aggregation implementations |
| `data/` | Columnar data structures (Block, Vector, Page) with memory management via circuit breakers |
| `data/sort/` | Sorting infrastructure for TopN operations |
| `lucene/` | Lucene integration for reading from Elasticsearch indices with slice-based partitioning |
| `operator/` | Execution operators (Filter, Eval, Project, Limit, etc.) and Driver orchestration |
| `operator/exchange/` | Inter-node communication for distributed query execution |
| `operator/lookup/` | Lookup and enrichment query operators |
| `operator/topn/` | TopN sorting with binary encoding for efficient comparison |
| `operator/mvdedupe/` | Multi-valued field deduplication operators |
| `querydsl/` | Lucene query generation support |

---

## External Data Sources

> **Note (Feb 2026):** ES|QL on main now includes a complete external data sources framework. This section summarizes the infrastructure; see [EXTERNAL_SOURCES_OVERVIEW.md](EXTERNAL_SOURCES_OVERVIEW.md) for full details and [CONNECTOR_SPI_PROPOSAL.md](CONNECTOR_SPI_PROPOSAL.md) for the evolution proposal.

The external data sources infrastructure adds querying of non-Elasticsearch data (S3 Parquet, GCS NDJSON, Iceberg tables, Arrow Flight endpoints, etc.) via the `EXTERNAL` command (dev-gated). Key components:

- **Dual SPI**: File-based (`StorageProvider` + `FormatReader`) and query-based (`Connector` + `ResultCursor`), unified under `ExternalSourceFactory`
- **Distributed execution**: Split-based parallelism across data nodes using `SplitProvider`, `ExternalSliceQueue`, `AdaptiveStrategy`, and `DataNodeComputeHandler` — fully implemented
- **Partition detection**: Hive-style and template-based auto-detection with virtual columns via `HivePartitionDetector`, `TemplatePartitionDetector`
- **Three-level filter model**: L1 partition pruning → L2 per-node pushdown → L3 engine remainder
- **Plugin architecture**: 11+ lazy-loaded modules (S3, GCS, HTTP, Parquet, ORC, NDJSON, CSV, Iceberg, gRPC/Flight, ZSTD, BZIP2)

The infrastructure is in `x-pack/plugin/esql-datasource/` (plugins) with core framework code in `x-pack/plugin/esql/src/.../datasource/`.

---

## Key Design Patterns

1. **Visitor Pattern**: Parse tree → logical plans via visit methods
2. **Factory Pattern**: PlanFactory and OperatorFactory for composable creation
3. **Tree Pattern**: Plans form ASTs with parent-child relationships
4. **Marker Interfaces**: SortAgnostic, Streaming, PipelineBreaker identify plan properties
5. **Stage Tracking**: LogicalPlan.Stage enables validation at each phase
6. **Rule-Based Optimization**: Batched rules with fixed-point iteration
7. **Coordinator/Data Node Split**: FragmentExec for distributed execution
8. **Column-Oriented Processing**: Blocks/Vectors for cache efficiency
9. **Reference Counting**: Blocks tracked for memory control
10. **Type-Specialized Code Generation**: @Evaluator generates type-specific implementations
