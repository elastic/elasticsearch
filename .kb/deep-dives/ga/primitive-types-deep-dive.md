# Native Primitive Types in ESQL: Deep Dive

## Executive Summary

Supporting native `float` in ESQL is **medium effort** (~8-10d AI-assisted). The compute engine already has FloatBlock/FloatVector with full TopN, serialization, aggregation, and evaluator-generation support. The primary work is in the **ESQL layer** (type system, planner, functions, response serialization). Supporting `short`/`byte` natively is **significantly more work** (~16-20d additional each AI-assisted) because ShortBlock/ByteBlock do not exist at all.

---

## 0. What It Takes to Support a New Primitive Type in ESQL

Adding a new primitive type (e.g., `float`, `short`, `byte`) touches every layer of the ESQL stack. Here is the full checklist:

### Compute Engine (Block/Vector layer)

| Component | Purpose | What's Needed | Generated? | Effort |
|-----------|---------|--------------|-----------|--------|
| `{Type}Block` + `{Type}ArrayBlock` + `{Type}BigArrayBlock` + `{Type}VectorBlock` | Primary columnar storage — holds the actual typed values for a column within a Page. Every operator reads/writes through Blocks. | Block storage for the type | Yes (template) | 0.5d |
| `{Type}Vector` + `{Type}ArrayVector` + `{Type}BigArrayVector` + `Constant{Type}Vector` | Dense, no-nulls fast path — when a Block has no nulls or multi-values, operators access the underlying Vector directly for zero-overhead iteration. | Read-only vector variants | Yes (template) | 0.5d |
| `{Type}BlockBuilder` + `{Type}VectorBuilder` + `{Type}VectorFixedBuilder` | Mutable construction — evaluators and readers build result Blocks value-by-value through Builders, which handle growth, null tracking, and multi-value positions. | Mutable builders | Yes (template) | 0.5d |
| `{Type}Lookup` | Hash-based value lookup — used by ENRICH and JOIN operators to match values against a lookup table. | Lookup table support | Yes (template) | included above |
| `ElementType` enum | Type registry — maps each primitive type to its Block/Vector/Builder classes. Every type-dispatching switch in the compute engine uses this. | Add new variant (e.g., `SHORT`) | No | 0.5d |
| `BlockFactory` | Circuit-breaker-aware allocation — all Block/Vector creation goes through BlockFactory, which tracks memory against the ES circuit breaker. | Builder/constant factory methods | No | 0.5d |
| `BlockUtils` | Convenience utilities — used by tests, result builders, and operators for common operations like appending a value to a block or creating a constant block. | `appendValue`, `constantBlock`, `valueAtOffset` | No | 0.5d |
| `ConstantNullBlock` | Null representation — when an entire column is null, a single ConstantNullBlock is used regardless of type. Must know how to convert to a typed Block if needed. | Handle new type | No | included above |

**~12 generated files per type** from templates in `compute/build.gradle`. **~2d if blocks don't exist; ~0 if they do (e.g., FloatBlock).**

### Compute Engine (Operators)

| Component | Purpose | What's Needed | Generated? | Effort |
|-----------|---------|--------------|-----------|--------|
| `BlockHash` | GROUP BY implementation — hashes column values to assign rows to aggregation groups. Each primitive type needs its own hash implementation because hashing int vs float vs bytes is fundamentally different. | `{Type}BlockHash` for GROUP BY | No (new class) | 1-2d |
| `MultivalueDedupe` | Multi-value deduplication — when a multi-value field appears in GROUP BY or is deduplicated (MV_DEDUPE), this removes duplicate values within a single position. Type-specific for correct equality semantics. | `MultivalueDedupe{Type}` for MV dedup | No (new class) | 1d |
| `BatchEncoder` | Composite key encoding — encodes multiple GROUP BY columns into a single byte sequence for hashing. Needs a decoder per type to reconstruct values from the encoded form. | `{Type}sDecoder` for encoding | No (new class) | 0.5d |
| `GroupKeyEncoder` | Single-column key encoding — fast path for GROUP BY on a single column. Encodes typed values into a sortable byte representation. | `encode{Type}` case | No | 0.5d |
| `TopNEncoder` | SORT implementation — encodes/decodes values for TopN (ORDER BY + LIMIT) comparisons. Must preserve sort order for the type. | `encode{Type}`/`decode{Type}` | No | 0.5d |
| TopN extractors | TopN row extraction — reads source Blocks, extracts key/value pairs for sorting, and builds result Blocks from sorted rows. Three classes per type. | `KeyExtractorFor{Type}`, `ValueExtractorFor{Type}`, `ResultBuilderFor{Type}` | Yes (template) | 0.5d |
| `EnrichResultBuilder` | ENRICH result assembly — after ENRICH lookup matches, builds the result Block for the enriched column in the correct type. | `EnrichResultBuilderFor{Type}` | Yes (template) | included above |
| `QueryList` | Lucene query dispatch — reads typed values from Blocks to construct Lucene queries (e.g., for ENRICH or LOOKUP JOIN against an ES index). | `createBlockValueReaderForType` case | No | 0.5d |

**~3-4d total for operator support.**

### Compute Engine (Aggregations)

| Component | Purpose | What's Needed | Generated? | Effort |
|-----------|---------|--------------|-----------|--------|
| Sum, Min, Max, StdDev, Top, Values, Any | Core aggregation functions — each has a type-specific implementation that reads from the corresponding Block type. SumFloat uses Kahan compensation for float precision; MinFloat compares floats natively. | `{Agg}{Type}AggregatorFunction` + supplier | Yes (template) | 0.5d |
| Irate, Delta | Time-series rate aggregations — compute per-second rates and deltas over time-ordered numeric data. Type-specific to preserve precision semantics. | Time-series aggregators | Yes (template) | included above |
| State classes | Aggregation intermediate state — holds partial aggregation results during distributed execution (INITIAL phase on data nodes, FINAL phase on coordinator). Serialized between nodes. | `{Type}State`, `{Type}ArrayState`, `{Type}FallibleState`, `{Type}FallibleArrayState` | Yes (template) | included above |
| Cross-type Top/Sort | Multi-column TopN — when sorting by one type and carrying another (e.g., `SORT float_col | KEEP int_col`), needs cross-type extractor/builder combinations. | `Top{Type1}{Type2}` combinations | Yes (template) | included above |
| Aggregation function dispatches | ESQL-layer routing — each aggregation function (Avg, Sum, etc.) has a `toEvaluator()` method that switches on DataType to select the correct aggregator supplier. Needs a new `case FLOAT/SHORT`. | Add `case FLOAT/SHORT` to each agg function's supplier selection | No | 1d |

**~20-30 generated aggregation files per type. ~1.5d total (mostly template instantiation + dispatch wiring).**

### ESQL Type System

| Component | Purpose | What's Needed | File | Effort |
|-----------|---------|--------------|------|--------|
| `DataType` | Logical type registry — defines every type ESQL understands (INTEGER, DOUBLE, FLOAT, KEYWORD, etc.) with properties like size, name, and widening rules. The `widenSmallNumeric()` method is what currently converts FLOAT→DOUBLE before planning. | Remove `widenSmallNumeric()` for this type, or add new DataType | `DataType.java` | 0.5d |
| `isRepresentable()` | Type gate — determines which DataTypes can flow through the ESQL plan. Non-representable types are rejected during analysis. FLOAT is currently excluded. | Add type to representable set | `DataType.java` | included above |
| `PlannerUtils.toElementType()` | Logical-to-physical bridge — maps DataType (logical) to ElementType (physical Block type). This is THE dispatch point between the ESQL type system and the compute engine. Currently throws for FLOAT. | Map DataType → ElementType (currently throws for unsupported) | `PlannerUtils.java` | 0.5d |
| Analyzer | Field resolution — resolves field references against index mappings or external source schemas. Calls `widenSmallNumeric()` to convert narrow types. The change here is conditional: stop widening for external sources, keep widening for ES indices. | Stop widening during field resolution (conditional: ES indices vs external sources) | `Analyzer.java` | 0.5d |

**~1d total.**

### ESQL Functions and Evaluators

| Approach | Purpose | What's Needed | Scope | Effort |
|----------|---------|--------------|-------|--------|
| **Native evaluators** | Full type fidelity — each function gets a float-specific evaluator that operates on FloatBlock natively. Preserves float precision throughout. Maximum correctness, maximum effort. | Add `@Evaluator` method with new primitive type to every function | ~36+ files, ~100+ functions | 3-5d |
| **Implicit promotion** (recommended) | Pragmatic approach — insert a FloatBlock→DoubleBlock conversion before the existing DOUBLE evaluator. Schema stays `float`, but expressions produce `double`. This is how PostgreSQL handles float4→float8 promotion. Single interception point. | Add single type promotion interception (e.g., float→double) so existing evaluators handle the type | ~1-2 files | 1-2d |

The `@Evaluator` code generator (`Types.java`, `EvaluatorImplementer.java`) fully supports all Java primitives — writing `@Evaluator static float process(float val)` generates correct FloatBlock-based code. But implicit promotion avoids needing this for every function.

### Response Serialization

| Component | Purpose | What's Needed | File | Effort |
|-----------|---------|--------------|------|--------|
| `PositionToXContent` | REST response rendering — converts Block values to JSON/CBOR for the `_query` REST API response. Switches on ElementType to extract and format values. Currently throws for FLOAT. | Add type writer (currently throws for unsupported) | `PositionToXContent.java` | 0.5d |
| `ResponseValueUtils` | Response value extraction — extracts typed values from Blocks for response building (used by both REST and transport serialization). Currently throws for FLOAT. | Add value extractor | `ResponseValueUtils.java` | included above |

**~0.5d total.**

### Data Source Readers (for external sources)

| Component | Purpose | What's Needed | Effort |
|-----------|---------|--------------|--------|
| Format readers (Parquet, ORC, CSV, NDJSON) | Data ingestion — read external file formats and produce typed Blocks. Currently widen float→double in schema resolution (e.g., `ParquetFormatReader.java:357: case FLOAT, DOUBLE -> DataType.DOUBLE`). Need to report native type and produce FloatBlock. | Stop widening, report native type in schema, produce native block | 1d |
| Lucene BlockLoader (for ES indices) | ES index reading — loads column values from Lucene doc values into Blocks. Float fields use `SortedNumericDoubleValues` which widens at the Lucene layer. Native float would require a different doc values access path. | Change doc values path (e.g., `SortedNumericDoubleValues` widens float) | 2d |

### Tests

| Category | Purpose | What's Needed | Effort |
|----------|---------|--------------|--------|
| Block/Vector unit tests | Verify Block/Vector correctness — null handling, multi-value positions, serialization round-trips, builder edge cases for the new type. | ~10-15 generated test files per type | 1d |
| Aggregation tests | Verify aggregation correctness — SUM precision, MIN/MAX ordering, GROUP BY hashing, distributed state serialization for the new type. | ~10-15 generated test files per type | 1d |
| CSV spec tests | ESQL end-to-end correctness — the CSV spec test framework drives queries against inline data and asserts results. Need to add the new type to the framework's type system. | Add type to ESQL CSV test framework | 0.5d |
| Integration tests | Real-world validation — run queries against actual Parquet/ORC files (external sources) or ES indices with the new type. Verifies the full pipeline from reader to response. | End-to-end with real data (index or external source) | 1d |
| Backward compat tests | Upgrade safety — verify that a mixed-version cluster (old nodes + new nodes) handles the new type correctly during rolling upgrades. Only needed for ES indices (schema change is breaking). | For ES indices only (schema change is breaking) | 1d |

**~3-4d total.**

### Total per Type

| Scenario | Generated Files | Non-Generated Changes | Tests | Total |
|----------|----------------|----------------------|-------|-------|
| Block type **already exists** (e.g., float) | 0 (done) | ~15-20 files | ~20+ | **~8-10d** |
| Block type **does not exist** (e.g., short, byte) | ~50-60 new | ~20-25 files | ~30-40 | **~16-20d** |

---

## 1. FloatBlock Operator Coverage

### What Already Handles FLOAT

FloatBlock was added in PR #109746 (May 2024) as part of dense_vector support. The compute engine has comprehensive float support:

**Data layer** (11 generated files):
- FloatBlock, FloatArrayBlock, FloatBigArrayBlock, FloatVectorBlock
- FloatVector, FloatArrayVector, FloatBigArrayVector
- FloatBlockBuilder, FloatVectorBuilder, FloatVectorFixedBuilder
- FloatLookup

**TopN** (all 3 extractors): KeyExtractorForFloat, ValueExtractorForFloat, ResultBuilderForFloat

**Aggregations** (all template-generated):
- SumFloat, MinFloat, MaxFloat, StdDevFloat
- TopFloat (and all cross-type combinations: TopIntFloat, TopLongFloat, TopFloatDouble, etc.)
- ValuesFloat, AnyFloat
- IrateFloat, DeltaFloat
- FirstFloatByTimestamp, LastFloatByTimestamp
- State classes: FloatState, FloatArrayState, FloatFallibleState, FloatFallibleArrayState

**Operators that handle FLOAT**:
- `GroupKeyEncoder` — has `case FLOAT` (encodes via `encodeFloat`)
- `BlockUtils` — both `appendValue` and `constantBlock` handle FLOAT
- `QueryList.createBlockValueReaderForType` — has `case FLOAT`
- `EnrichResultBuilder.enrichResultBuilder` — has `case FLOAT` (EnrichResultBuilderForFloat)
- All TopN dispatchers (ResultBuilder, KeyExtractor, ValueExtractor)
- `ConstantNullBlock` — handles FLOAT
- `BlockFactory` — full float builder and constant support

**Evaluator code generator** fully supports float:
- `Types.java` has FLOAT_BLOCK, FLOAT_VECTOR, FLOAT_BLOCK_BUILDER, FLOAT_VECTOR_BUILDER, FLOAT_VECTOR_FIXED_BUILDER
- `TYPES` map includes `TypeDef.of(TypeName.FLOAT, "FLOAT", "FloatBlock", "FloatVector", null)`
- `builderType()`, `vectorFixedBuilderType()`, `elementType()` all handle float
- Any `@Evaluator` that takes `float` parameters will automatically generate FloatBlock-based code

### GAPS: What Does NOT Handle FLOAT

These are the specific subsystems that handle DOUBLE but NOT FLOAT:

#### Critical Infrastructure (3 files — would cause runtime crashes)

1. **`BlockHash.newForElementType()`** — No `case FLOAT`. Would throw `IllegalArgumentException("unsupported grouping element type")` if a float column appeared in GROUP BY.
   - File: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/blockhash/BlockHash.java:256`
   - Fix: Create `FloatBlockHash` (similar to DoubleBlockHash) OR cast float→double for hashing

2. **`MultivalueDedupe`** (3 methods) — No float case. `dedupeToBlockAdaptive`, `dedupeToBlockUsingCopyMissing`, `dedupeToBlockUsingCopyAndSort`, `evaluator` all switch on elementType and throw for non-handled types.
   - File: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/mvdedupe/MultivalueDedupe.java`
   - Fix: Create `MultivalueDedupeFloat` class (similar to MultivalueDedupeDouble)

3. **`BatchEncoder.decoder()`** — No float case. Throws `IllegalArgumentException("can't decode FLOAT")`.
   - File: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/mvdedupe/BatchEncoder.java:51`
   - Fix: Add `FloatsDecoder` class

#### ESQL Type System (2 files — the widening barrier)

4. **`PlannerUtils.toElementType()`** — FLOAT is in the `throw` clause. This is THE key barrier. The switch maps DataType→ElementType, and FLOAT explicitly throws an exception.
   - File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java:492`
   - Fix: Add `case FLOAT -> ElementType.FLOAT` to the switch

5. **`DataType.widenSmallNumeric()`** — FLOAT is declared with `.widenSmallNumeric(DOUBLE)`. This causes the Analyzer to widen float→double during field resolution, before FLOAT ever reaches the planner.
   - File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/DataType.java:254`
   - Fix: Remove `widenSmallNumeric(DOUBLE)` from the FLOAT definition
   - **Cascading impact**: `Analyzer.java:430` calls `widenSmallNumeric()` during field resolution. Removing widening means FLOAT DataType propagates through the entire plan.

#### Response Serialization (2 files)

6. **`PositionToXContent`** — FLOAT is in the `throw` clause.
   - File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/PositionToXContent.java:261`
   - Fix: Add float→xcontent writer (similar to double)

7. **`ResponseValueUtils`** — FLOAT is in the `throw` clause.
   - File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/ResponseValueUtils.java:206`
   - Fix: Add float value extractor

#### Functions (36 files — all in the ESQL expression layer)

Every function that currently supports DOUBLE would need explicit FLOAT handling in its `toEvaluator()` method. The key pattern: functions switch on `dataType()` (which is a DataType, not ElementType), so adding FLOAT support means adding `case FLOAT` or `dataType() == DataType.FLOAT` branches.

Major categories of affected functions:
- **Multivalue functions (18)**: MvSort, MvSum, MvAvg, MvMedian, MvMedianAbsoluteDeviation, MvMin, MvMax, MvFirst, MvLast, MvPercentile, MvPSeriesWeightedSum, MvSlice, MvContains, MvIntersects, MvIntersection, MvUnion, MvAppend
- **Aggregation functions (4)**: Delta, Idelta, Deriv, Sample
- **Scalar functions (4)**: ClampMin, ClampMax, Coalesce, RoundTo
- **Math/Score (1)**: Decay
- Other optimizer/planner refs: ApproximationPlan, ReplaceRoundToWithQueryAndTags

**However**: Many of these functions would "just work" if we map `DataType.FLOAT → DataType.DOUBLE` in their `toEvaluator()` (accepting precision loss for the evaluator while keeping the schema as FLOAT). This is a design choice — native float evaluators vs. double-evaluator-with-float-schema.

---

## 2. DataType Propagation

### How `widenSmallNumeric` Works

The widening system operates at the **type resolution** stage in the Analyzer, before any physical planning:

1. **`Analyzer.java:430`**: When resolving field references against index mappings, calls `t.getDataType().widenSmallNumeric()`. A Lucene `float` field gets DataType.FLOAT, which immediately becomes DataType.DOUBLE.

2. **`AbstractConvertFunction.java:62`**: Convert functions also widen before dispatching.

3. **`EsqlDataTypeRegistry.java:27`**: Counter types widen before looking up counter variants.

4. **`Grok.java:45`**: Grok output types are widened.

The widening is purely at the **logical type level**. It doesn't directly control the physical block type — that's controlled by `PlannerUtils.toElementType()`. But since FLOAT DataType never survives past the Analyzer, `toElementType()` never sees it (except from `DENSE_VECTOR`).

### Does ESQL Distinguish FLOAT from DOUBLE at the Logical Level?

**Yes, in theory.** DataType.FLOAT and DataType.DOUBLE are separate enum values with different properties (name, estimatedSize, etc.). FLOAT is flagged as `rationalNumber`, just like DOUBLE. The type system CAN carry FLOAT through the plan.

**No, in practice.** The `widenSmallNumeric(DOUBLE)` call in the Analyzer converts every FLOAT to DOUBLE before it enters the plan tree. The `isRepresentable()` check explicitly excludes FLOAT (line 732), which means FLOAT columns would be flagged as unsupported if they somehow survived. The `toElementType()` switch explicitly throws for FLOAT.

### What "Stopping Widening" Requires

Removing widening for FLOAT means:
1. Remove `.widenSmallNumeric(DOUBLE)` from FLOAT definition
2. Add FLOAT to `isRepresentable()` (remove from exclusion list)
3. Add `case FLOAT -> ElementType.FLOAT` to `toElementType()`
4. Handle FLOAT in every function's `toEvaluator()` method (see Section 1 gaps)
5. Handle FLOAT in response serialization
6. Handle FLOAT in BlockHash, MultivalueDedupe, BatchEncoder

---

## 3. Evaluator Generation

### How Evaluators Are Generated

Evaluators are generated **per @Evaluator annotation**, not per ElementType. Each annotated method specifies its Java primitive types explicitly:

```java
// In Abs.java:
@Evaluator(extraName = "Double")
static double process(double fieldVal) { ... }

@Evaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
static int process(int fieldVal) { ... }
```

This generates `AbsDoubleEvaluator` and `AbsIntEvaluator`, each hardcoded to work with the corresponding block type (DoubleBlock/IntBlock).

### Does a DOUBLE Evaluator Automatically Support FLOAT?

**No.** An evaluator generated from `double process(double val)` reads from `DoubleBlock` and writes to `DoubleBlock`. It cannot read from `FloatBlock`.

### What Would Be Needed for Float Evaluators?

**Option A: Native float evaluators** — Add `@Evaluator(extraName = "Float") static float process(float val)` to every function. The annotation processor already supports `float` parameters and will generate `FloatBlock`-based code. This is ~100+ functions.

**Option B: Widen-on-demand in evaluators** — Keep using DOUBLE evaluators but add a FloatBlock→DoubleBlock conversion step in the evaluator chain. This is a single interception point but adds a copy per block.

**Option C: Mixed approach** — Keep most functions using DOUBLE evaluators (via Option B), but add native float evaluators only for aggregations (SUM, AVG, MIN, MAX) where precision semantics differ (float SUM should use Kahan compensation differently).

The annotation processor and code generator (`Types.java`, `EvaluatorImplementer.java`) already fully support float as a first-class type. Writing `@Evaluator` methods with `float` parameters will "just work" in the code generation phase.

---

## 4. End-to-End Float Column Trace

### Step A: Reader produces FloatBlock

**Parquet**: The Parquet reader currently reads FLOAT columns and widens to DoubleBlock because the schema says DOUBLE. To produce FloatBlock, the Parquet reader would need to check if the Parquet physical type is FLOAT and produce FloatBlock directly.

**Lucene**: Lucene float fields use `SortedNumericDoubleValues` which already does float→double conversion at the doc values layer. Native float would require using `FloatPointValues` or changing the BlockLoader path.

**External sources**: Format readers already produce blocks based on the declared ElementType. If the schema says FLOAT, they'd produce FloatBlock.

**Status: Needs changes in each reader.**

### Step B: Schema reports FLOAT

**Currently**: `Analyzer.java:430` widens FLOAT→DOUBLE before the schema is constructed.

**Fix**: Remove widening, add FLOAT to representable types, update `toElementType()`.

**Status: Needs changes in DataType + PlannerUtils + Analyzer.**

### Step C: WHERE clause comparison on float column

Comparison evaluators (`Equals`, `GreaterThan`, etc.) switch on DataType:
```java
if (lt == DataType.DOUBLE || rt == DataType.DOUBLE) { ... }
```

If a FLOAT column is compared with a literal, type coercion would need to handle FLOAT. Either:
- Add explicit FLOAT cases to all comparison functions
- OR define FLOAT as promotable to DOUBLE in comparison context (commonType logic in `EsqlDataTypeConverter`)

**Status: Needs changes in comparison functions and/or type coercion.**

### Step D: STATS AVG(float_col)

Aggregation functions dispatch to aggregator suppliers based on DataType:
```java
// In Avg.java:
if (dt == DataType.DOUBLE) { return new SumDoubleAggregatorFunctionSupplier(...); }
```

The compute engine already has `SumFloatAggregatorFunctionSupplier`, `MinFloatAggregatorFunctionSupplier`, etc. The fix is adding `case FLOAT` dispatches in each aggregation function's supplier selection.

**HOWEVER**, BlockHash is the blocker — `STATS ... BY float_col` would crash because BlockHash.newForElementType() doesn't handle FLOAT.

**Status: Aggregation implementations exist. Needs dispatch wiring + BlockHash fix.**

### Step E: Result serialization

`PositionToXContent` and `ResponseValueUtils` both throw for FLOAT.

Fix: Add float extraction — `((FloatBlock) block).getFloat(offset)` in response writers.

**Status: ~0.5d in 2 files.**

### Summary Table

| Step | Works Today? | Effort to Fix |
|------|-------------|---------------|
| A. Reader produces FloatBlock | No (widens to Double) | ~1d (per reader) |
| B. Schema carries FLOAT | No (Analyzer widens) | ~1d (3 files) |
| C. WHERE on float | No (no evaluators) | ~1d (type coercion + comparisons) |
| D. STATS AVG(float) | Partial (aggregators exist, dispatch missing) | ~2d (BlockHash + MvDedupe + dispatches) |
| E. Result serialization | No (throws) | ~0.5d (2 files) |

---

## 5. Short/Byte: How Much MORE Work?

### FloatBlock Exists, ShortBlock and ByteBlock Do NOT

The difference is massive:

**FloatBlock (exists)**: 11 generated data files + aggregation states + TopN extractors + enrich builders — all already generated from templates. PR #109746 added them.

**ShortBlock (does not exist)**: Would need ALL of the following:

#### Template Instantiations Needed

The compute engine's `build.gradle` uses string templates to generate block types. Current counts:
- `intProperties` is used in **40** template instantiations
- `floatProperties` is used in **34** template instantiations
- `doubleProperties` is used in **38** template instantiations

For ShortBlock you would need:
1. Define `shortProperties = prop("Short", "Short", "short", "Short", "SHORT", "Short.BYTES", "ShortArray", "LongHash")`
2. Add ~34-40 template instantiation blocks in build.gradle
3. Generate: ShortBlock, ShortArrayBlock, ShortBigArrayBlock, ShortVectorBlock, ShortVector, ShortArrayVector, ShortBigArrayVector, ConstantShortVector, ShortBlockBuilder, ShortVectorBuilder, ShortVectorFixedBuilder, ShortLookup
4. Generate: ShortState, ShortArrayState, ShortFallibleState, ShortFallibleArrayState
5. Generate: SumShort, MinShort, MaxShort, StdDevShort, TopShort (and cross-type combos), ValuesShort, AnyShort aggregators
6. Generate: KeyExtractorForShort, ValueExtractorForShort, ResultBuilderForShort (TopN)
7. Generate: MultivalueDedupeShort, BatchEncoder shorts decoder

That's approximately **50+ generated files** plus:

#### Non-Generated Changes Needed

1. `ElementType.java` — add `SHORT(N, "Short", ...)` enum value
2. `BlockFactory.java` — add short builder/constant methods
3. `BlockUtils.java` — add SHORT case in appendValue, constantBlock, valueAtOffset
4. `GroupKeyEncoder.java` — add SHORT case with `encodeShort`
5. `TopNEncoder` interface — add `encodeShort`/`decodeShort` methods
6. `BlockHash.java` — create ShortBlockHash
7. `MultivalueDedupe.java` — add SHORT dispatches
8. `BatchEncoder.java` — add SHORT decoder
9. `QueryList.java` — add SHORT value reader
10. `EnrichResultBuilder.java` — create EnrichResultBuilderForShort
11. `ConstantNullBlock.java` — add SHORT handling
12. `Types.java` (code generator) — add SHORT_BLOCK, SHORT_VECTOR, etc.
13. `EvaluatorImplementer.java` — verify short support
14. `PlannerUtils.toElementType()` — add SHORT case
15. `DataType.java` — remove `widenSmallNumeric(INTEGER)` from SHORT
16. All the same ESQL-layer changes as FLOAT (functions, serialization, etc.)

Plus block serialization transport version handling.

#### Estimated Count

- **Generated files**: ~50-60 new files
- **Non-generated file changes**: ~20-25 files
- **Test files**: ~30-40 new test files
- **Total new/modified files**: ~100-125

### Precedent: FloatBlock PR #109746

The PR that added FloatBlock touched **37 files** and added **~2,400 lines**. But it had the advantage of:
- Templates already existed for Int/Long/Double/Boolean/BytesRef
- Adding one more instantiation to each template was mechanical
- The PR was focused only on the compute engine, not the ESQL type system

ShortBlock would be similar in the compute engine (~2,000-2,500 LOC), but the ESQL layer changes are additive.

ByteBlock would be essentially identical scope to ShortBlock.

---

## 6. Estimated Work Breakdown

### Option 1: Native Float Only (FLOAT column stays float end-to-end)

| Category | Files | Effort |
|----------|-------|--------|
| Remove float widening (DataType, Analyzer, PlannerUtils) | 3 | 1d |
| BlockHash + MultivalueDedupe + BatchEncoder for float | 3-4 new, 3 modified | 2d |
| Response serialization (PositionToXContent, ResponseValueUtils) | 2 | 0.5d |
| Function toEvaluator() updates (36 files, mostly mechanical) | 36 | 3d |
| Type coercion (commonType, comparison operators) | 5-10 | 1d |
| Reader changes (Lucene, Parquet, external sources) | 3-5 | 1d |
| Tests (CSV spec, unit, integration) | 20+ | 3d |
| **Total** | **~80 files** | **~8-10d** |

### Option 2: Native Float + Short + Byte

| Category | Additional Effort (Short) | Additional Effort (Byte) |
|----------|--------------------------|-------------------------|
| New ElementType + block classes | 50-60 generated | 50-60 generated |
| Non-generated infra | 20-25 files | 20-25 files |
| ESQL layer functions | Same 36+ files as float | Same 36+ files as float |
| Tests | 30-40 new | 30-40 new |
| **Per-type total** | **~16-20d** | **~16-20d** |
| **Grand total (float + short + byte)** | | **~40-50d** |

### Option 3: Float Natively, Short/Byte Remain Widened (Recommended)

Float is the highest-value target:
- 50% memory savings vs double for external source columns
- Dense vector already uses FloatBlock (precedent exists)
- Compute engine is 90% ready
- Most common "small numeric" type in practice

Short/byte offer diminishing returns:
- Rare in practice (mostly legacy ES mappings)
- ShortBlock/ByteBlock don't exist, requiring massive template work
- 50-75% memory savings vs int, but int columns are already small

**Recommended: Support native FLOAT (~8-10d AI-assisted), keep SHORT→INTEGER and BYTE→INTEGER widening.**

---

## 7. Key Design Decisions

### Should FLOAT evaluators use float arithmetic or double arithmetic?

**Recommendation**: Use float arithmetic for aggregations (SUM, AVG) where precision semantics differ, and delegate to double evaluators for scalar functions where the precision difference is negligible. This avoids writing 100+ float evaluators while preserving correct aggregation semantics.

### Should FLOAT appear in the user-visible schema?

**Recommendation**: Yes. If the source declares float, the ESQL response should say `float`, not `double`. This is important for external sources where schema fidelity matters. For Elasticsearch indices, FLOAT fields have always been widened — this would be a breaking change if we stop widening ES-indexed float fields.

### Should this be gated behind a capability/feature flag?

**Yes, absolutely.** Use `DataType.Builder.underConstruction(TransportVersion)` as documented in DataType.java. This prevents the new type from appearing in non-SNAPSHOT builds until all nodes support it.

### What about HALF_FLOAT and SCALED_FLOAT?

These should remain widened to DOUBLE. They have no native block type and don't benefit from native representation. HALF_FLOAT is 16-bit (IEEE 754 binary16) — Java has no native half-float, so it would need custom encoding. SCALED_FLOAT is a long internally and has fundamentally different semantics.

---

## 8. Opinion: Defer to Post-MVP

There are three possible paths for native primitive type support in ESQL. **Path A** — float-only for external sources (~8-10d) — delivers schema fidelity and 50% memory savings for float columns, but creates an asymmetry: external sources would report `float` while ES indices still report `double` for the same underlying data type. When we eventually stop widening for ES indices, that becomes a breaking change to user-visible schemas — anything built on `"type": "double"` today (Kibana dashboards, client deserializers, saved queries) would break. We would be shipping a known future breaking change into the first GA release of external sources. **Path B** — all four types (float, short, byte, char) for external sources (~40-50d) — is thorough but represents 8-10 calendar weeks of AI-assisted engineering for types that are uncommon in external formats. Parquet INT16 is annotated as int32, not a first-class type, and byte/char columns are nearly nonexistent in practice. This is a poor return on investment for GA, especially when the alternative (widening) produces correct results, just slightly less memory-efficient. **Path C** — all types for all of ESQL including ES indices — is the "right" answer but is a breaking change by definition and belongs at a major version boundary, not a feature GA.

The proposed direction is to **defer all native primitive type work to post-MVP**. The current widening behavior (`float→double`, `short→integer`, `byte→integer`) is correct — it produces accurate query results, and every function, operator, and aggregation already handles the widened types. The cost is ~2x memory overhead on float columns and loss of schema fidelity, neither of which are GA blockers for external data sources. When the time comes to address this properly, it should be done holistically — all types, all sources (external and ES indices), gated behind a major version — rather than piecemeal in a way that creates inconsistency between source types or ships a known future breaking change. This keeps the GA scope focused on correctness and coverage (the 25 open MVP items), not optimization that benefits a narrow subset of columns.
