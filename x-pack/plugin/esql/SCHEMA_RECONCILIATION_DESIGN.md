# Schema Reconciliation for Multi-File External Sources — Design Proposal

**Author**: costin
**Date**: 2026-03-26
**Status**: Draft — for team review
**Branch**: costin/aggregate-pushdown-v3
**Related**: aggregate pushdown Phase 2 depends on union-by-name (see `AGGREGATE_PUSHDOWN_PLAN.md`)

---

## Problem Statement

ESQL datasources currently support only `FIRST_FILE_WINS` when reading multiple files from
blob storage via glob patterns. The first file's schema becomes the planning-phase schema for
all subsequent files. Schema differences in subsequent files are silently ignored, which can
cause data corruption (wrong column mapping) or runtime errors.

Two additional strategies are needed:

1. **STRICT** — validate that all files share the exact same schema; fail fast if they don't.
2. **UNION_BY_NAME** — merge schemas from all files into a superset, filling missing columns
   with nulls and resolving compatible type differences.

These are defined as placeholders in `FormatReader.SchemaResolution` but unimplemented.

---

## Current Architecture

### Resolution Flow (Planning Phase)

```
Query: FROM "s3://bucket/data/*.parquet"
  → GlobExpander.expandGlob()           # list all matching files
  → ExternalSourceResolver              # take first file's schema
    .resolveMultiFileSource()
  → resolveSingleSource(firstFile)       # read metadata from first file only
  → enrichSchemaWithPartitionColumns()   # add Hive partition columns
  → ResolvedSource(metadata, fileSet)    # planning-phase schema + file list
```

### Key Observations

- **Metadata per file is cheap**: `FormatReader.metadata(StorageObject)` reads only the file
  footer (Parquet: 4–32 KB, ORC: similar). It does NOT read data.
- **Column matching is by name**: both Parquet and ORC use named columns, not positional.
- **Schema → Attribute list**: each file's schema becomes `List<Attribute>` where each
  attribute is a `ReferenceAttribute(name, DataType, Nullability)`.
- **Statistics come with metadata**: `SourceMetadata.statistics()` returns per-file column
  statistics (min/max/null count/row count). This is relevant for aggregate pushdown Phase 2.
- **File count**: typical use cases range from tens to thousands of files per glob.

---

## Industry Survey

### DuckDB

| Aspect | Behavior |
|--------|----------|
| **Default** | Union by position. Schema from first file. Fails on type/column count mismatch. |
| **`union_by_name=true`** | Reads ALL file metadata upfront. Union of all column names. Missing columns → NULL. |
| **Type conflicts** | Safe numeric widening (int→bigint, float→double). Cross-category (int→string) → error. |
| **Schema scanning** | Eager: all file metadata read during bind phase. Parallelized since v1.1. |
| **Performance** | 1000 small CSV files: 0.6s with parallel scanning. ~10% overhead vs positional. |
| **Strict mode** | Default behavior IS strict (by position). Named matching is the "lenient" option. |
| **`schema` param** | Explicit schema declaration using Parquet field IDs. Cannot combine with `union_by_name`. |

**Key insight**: DuckDB treats strict-by-position as the default and union-by-name as the opt-in.
Type widening is limited to safe numeric promotions. The `filename` virtual column helps users
debug which file contributed which rows.

### Apache Spark

| Aspect | Behavior |
|--------|----------|
| **Default** | `mergeSchema=false`. Schema from `_common_metadata` summary file, `_metadata`, or a random file. |
| **`mergeSchema=true`** | Reads ALL file footers in parallel across executors. `StructType.merge()` unions fields by name. |
| **Type conflicts** | `StructType.merge()` is STRICT: int vs long → **error** (no auto-widening at merge time). |
| **Read-time widening** | Separate from merge: vectorized reader supports int32→int64, float→double, etc. |
| **Nested schemas** | Recursive merge of structs, arrays, maps. |
| **ORC vs Parquet** | Same merge logic. ORC native reader has broader type conversion support. |
| **Performance** | Parallel footer reading on executors. Driver collects partial results. Can be slow for large footer files on S3. |

**Key insight**: Spark separates schema merge (strict equality except nullability) from read-time
type promotion (vectorized reader does widening). This two-layer design is worth considering.
Spark's `mergeSchema` is off by default because it's expensive.

### ClickHouse

| Aspect | Behavior |
|--------|----------|
| **Default** | `schema_inference_mode='default'`. First file only. Missing columns → fill with defaults/NULL. |
| **Union mode** | `schema_inference_mode='union'`. Reads ALL file metadata. Union by name. `getLeastSupertype()` for type conflicts. |
| **Type promotion** | Numeric widening within signed/unsigned families. int+float→float64. String+numeric → **error** (no common type). |
| **Missing columns** | `input_format_parquet_allow_missing_columns=true` (default). Fills with type defaults. |
| **Extra columns** | Silently ignored (column projection). |
| **Variant fallback** | `input_format_try_infer_variants=true` enables Variant type for truly incompatible types. |

**Key insight**: ClickHouse's `allow_missing_columns` setting (on by default for Parquet, off for ORC)
is a pragmatic middle ground — it allows FIRST_FILE_WINS to handle added columns without erroring.
Their `getLeastSupertype` is well-defined with clear failure modes.

### Cribl

| Aspect | Behavior |
|--------|----------|
| **Model** | Event-centric (per-event fields), not table-centric. |
| **Schema merge** | None. Each file is read independently. Events simply lack missing fields. |
| **Type handling** | Strict on writes (type violation → drop row). Undocumented on reads. |
| **Philosophy** | "Schema-on-need" — normalize upstream (Stream) not at query time (Search). |

**Key insight**: Cribl's event-centric model avoids schema reconciliation entirely. Not applicable
to our table-oriented query engine, but their "normalize early" philosophy is worth noting —
users who need guaranteed consistency should use a table format (Iceberg/Delta) over raw files.

---

## Comparative Summary

| Feature | DuckDB | Spark | ClickHouse | **Our Proposal** |
|---------|--------|-------|------------|-------------------|
| Default mode | Strict (positional) | Random file | First file | FIRST_FILE_WINS (current) |
| Union by name | Opt-in param | Opt-in param | Opt-in setting | Opt-in WITH clause |
| Strict validation | Default behavior | Not available | Not available | Opt-in WITH clause |
| Type widening | Safe numeric | None at merge | Safe numeric | Safe numeric |
| Metadata scanning | All files eagerly | All files in parallel | All files eagerly | All files in parallel |
| Missing columns | NULL | NULL | Type default / NULL | NULL |
| Cross-category types | Error | Error | Error | Error |

---

## Proposed Design

### Strategy Selection

Users select the strategy via the WITH clause:

```sql
-- Current default (no change)
FROM "s3://bucket/data/*.parquet"

-- Explicit first-file-wins
FROM "s3://bucket/data/*.parquet" WITH schema_resolution = "first_file_wins"

-- Strict: all files must match
FROM "s3://bucket/data/*.parquet" WITH schema_resolution = "strict"

-- Union by name: merge all schemas
FROM "s3://bucket/data/*.parquet" WITH schema_resolution = "union_by_name"
```

The format reader's `defaultSchemaResolution()` determines the default when no explicit value
is provided. Initially all formats default to `FIRST_FILE_WINS` (preserving current behavior).

> **Open question 1**: Should the default eventually change to STRICT for Parquet/ORC once the
> feature is proven stable? STRICT is the safest default and matches DuckDB's philosophy. Users
> with heterogeneous files would opt into `union_by_name`.

### Strategy 1: STRICT

#### Behavior

All files in the glob must have the **exact same schema** (column names, types, and order).
If any file differs, the query fails at planning time with a descriptive error.

#### What "exact match" means

- Same column count
- Same column names (case-sensitive; case-insensitive as a future option)
- Same data types (no implicit widening — int32 ≠ int64)
- Nullability differences are tolerated (a non-nullable column is compatible with nullable)

> **Open question 2**: Should we allow safe numeric widening in STRICT mode? DuckDB's strict
> mode does NOT allow it (int32 ≠ int64 is an error). Spark's `StructType.merge()` also rejects
> it. Both require explicit action to handle type differences. Our recommendation is to NOT
> allow widening in STRICT — if you need widening, use UNION_BY_NAME.

#### Error messages

Errors must identify:
- Which file has the mismatch (absolute path)
- What the mismatch is (missing column, extra column, type difference)
- What the reference schema is (from the first file)

Example:
```
Schema mismatch in file [s3://bucket/data/2024-02.parquet]:
  Column [salary] has type [LONG] but first file [s3://bucket/data/2024-01.parquet] has type [INTEGER].
  Use schema_resolution = "union_by_name" to merge schemas automatically.
```

#### Implementation: scanning strategy

**Option A: Scan all files eagerly (recommended)**

Read metadata from ALL files during planning. Compare each file's schema against the first.
Fail on the first mismatch.

- **Pro**: Immediate, deterministic errors. No surprises at execution time.
- **Pro**: Enables collecting per-file statistics for aggregate pushdown (Phase 2).
- **Con**: Planning time grows linearly with file count. ~100ms per 100 files on S3 (footer reads).
- **Mitigation**: Parallelize footer reads. Use thread pool already available in `ExternalSourceResolver`.

**Option B: Validate lazily at execution time**

Read only the first file at planning. Validate each subsequent file when its split is read
by the data node.

- **Pro**: Zero overhead at planning time.
- **Con**: Errors surface mid-query, potentially after significant work. Non-deterministic
  error timing depending on which data node hits a mismatched file first.
- **Con**: Cannot collect per-file statistics for aggregate pushdown.

**Recommendation**: Option A. The cost of reading file footers (not data) is trivial compared
to reading the actual data. Eager validation provides a much better user experience. This also
enables aggregate pushdown Phase 2.

#### Implementation: validation depth

**Level 1 — Schema only**: Read the file footer schema. Compare column names and types.
Do not read column statistics. This is sufficient for STRICT validation.

**Level 2 — Schema + statistics**: Read the file footer schema AND statistics. Compare
schemas for STRICT, and store statistics for aggregate pushdown. This is a natural extension
once we're reading all footers anyway.

**Recommendation**: Implement Level 2 from the start. The incremental cost is zero (statistics
are in the same footer). This unblocks aggregate pushdown Phase 2 for free.

### Strategy 2: UNION_BY_NAME

#### Behavior

The unified schema is the **union of all column names** across all files. Columns missing from
a file are filled with NULL when reading that file. Type differences on the same column name
are resolved by type widening where possible, or produce an error.

#### Schema merge algorithm

```
unified_schema = {}

for each file in file_set:
    file_schema = read_metadata(file).schema()
    for each column in file_schema:
        if column.name not in unified_schema:
            unified_schema[column.name] = column (made nullable)
        else:
            existing = unified_schema[column.name]
            unified_schema[column.name] = merge(existing, column)

function merge(a, b):
    if a.type == b.type:
        return a.withNullability(a.nullable || b.nullable)
    widened = tryWiden(a.type, b.type)
    if widened != null:
        return a.withType(widened).withNullability(true)
    else:
        error("Cannot merge column [name]: type [a.type] and [b.type] are incompatible")
```

#### Type widening rules

Follow the same rules as DuckDB and ClickHouse — safe numeric promotions only:

| From | To | Allowed |
|------|----|---------|
| BYTE → SHORT | yes | Lossless |
| SHORT → INTEGER | yes | Lossless |
| INTEGER → LONG | yes | Lossless |
| FLOAT → DOUBLE | yes | Lossless |
| UNSIGNED_LONG → ??? | no | No safe supertype in ESQL |
| INTEGER → DOUBLE | yes | Lossless (int32 fits in float64) |
| LONG → DOUBLE | **no** | Lossy (int64 > 2^53 loses precision) |
| Any numeric → KEYWORD/TEXT | **no** | Cross-category |
| KEYWORD → TEXT | **no** | Different semantics |
| DATE → DATETIME | **TBD** | Semantically reasonable |

> **Open question 3**: Should we support LONG→DOUBLE widening? DuckDB does not (it's lossy).
> Spark does not. ClickHouse does (int+float→float64). Our recommendation is NO — follow
> DuckDB/Spark and reject lossy promotions.

> **Open question 4**: Should DATE→DATETIME widening be supported? Spark doesn't (they're
> incompatible). DuckDB does. It's semantically reasonable (date is a datetime at midnight).
> Our recommendation is to defer this and start strict — only the numeric widening ladder.

#### Nullability handling

- A column that exists in ALL files: nullable if ANY file declares it nullable.
- A column that is missing from at least one file: **always nullable** (it will be NULL-filled
  for files that lack it).

This matches DuckDB and Spark behavior.

#### Column ordering in the unified schema

Columns appear in **first-seen order**: the first file's columns come first (in their original
order), then new columns from subsequent files are appended in the order they're first
encountered.

This is the same approach as DuckDB's `union_by_name`.

> **Open question 5**: Should column ordering be deterministic? Currently files are sorted by
> path in `GlobExpander`, so the ordering IS deterministic. We should preserve this.

#### Per-file column mapping

For each file, the system maintains a mapping from the unified schema to the file's local schema:

```java
record FileColumnMapping(
    int[] localToGlobalIndex,    // file column i → unified column index
    Set<Integer> missingColumns  // unified column indices absent from this file
)
```

This mapping is computed once during schema merge and stored alongside the file entry.
At read time:
- Present columns → read from file, cast to unified type if needed
- Missing columns → emit NULL block of the appropriate type

#### Implementation: metadata scanning

Same approach as STRICT Option A: read metadata from ALL files during planning, in parallel.

**Performance characteristics** (based on DuckDB/Spark experience):

| File count | Estimated planning overhead (S3) | Notes |
|------------|----------------------------------|-------|
| 10 files | ~50ms | Negligible |
| 100 files | ~200ms | Acceptable |
| 1,000 files | ~2s | Noticeable, parallelism critical |
| 10,000 files | ~15s | Heavy. Consider sampling/caching. |

**Parallelization**: Use the existing executor in `ExternalSourceResolver`. Submit metadata
reads as tasks to a thread pool. Merge schemas as results arrive.

> **Open question 6**: For very large file sets (10,000+), should we support a sampling mode
> that reads N random files and uses their merged schema? This is risky (may miss columns) but
> could be a future opt-in. DuckDB doesn't offer this — they always read everything.
> Our recommendation: don't implement sampling. If the user has 10,000 files with different
> schemas, they should use a table format (Iceberg/Delta) that maintains a catalog.

#### Read-time behavior: NULL filling for missing columns

When reading a file that is missing columns from the unified schema, the reader must produce
NULL values for those columns. There are two approaches:

**Option A: Reader-level NULL injection**

The `FormatReader.read()` method is aware of the unified schema and injects NULL blocks for
missing columns. Each format reader handles this internally.

- **Pro**: No changes to the execution engine.
- **Con**: Every format reader must implement NULL injection logic. Duplicated code.

**Option B: Wrapper/adapter layer**

A `SchemaAdaptingIterator` wraps the format reader's output and:
1. Reads the file with only the columns it has (column projection)
2. Creates NULL blocks for missing columns
3. Reorders columns to match the unified schema

- **Pro**: Format readers don't need changes. Single implementation in the framework.
- **Con**: Extra object creation per page. Minor.

**Recommendation**: Option B. A `SchemaAdaptingIterator` in the datasource framework keeps
format readers simple and avoids duplicating NULL-injection logic across Parquet, ORC, CSV, etc.

```java
class SchemaAdaptingIterator implements CloseableIterator<Page> {
    private final CloseableIterator<Page> delegate;  // reads only present columns
    private final List<Attribute> unifiedSchema;
    private final FileColumnMapping mapping;

    @Override
    public Page next() {
        Page filePage = delegate.next();
        int positions = filePage.getPositionCount();

        Block[] unifiedBlocks = new Block[unifiedSchema.size()];
        for (int i = 0; i < unifiedSchema.size(); i++) {
            if (mapping.missingColumns().contains(i)) {
                unifiedBlocks[i] = createNullBlock(unifiedSchema.get(i).dataType(), positions);
            } else {
                int localIndex = mapping.globalToLocalIndex(i);
                unifiedBlocks[i] = maybeCast(filePage.getBlock(localIndex), unifiedSchema.get(i));
            }
        }
        return new Page(positions, unifiedBlocks);
    }
}
```

#### Read-time behavior: type casting for widened columns

When a column exists in a file but with a narrower type than the unified schema (e.g., file
has INT32, unified schema has INT64), the reader must cast the values.

Options:
- **In the SchemaAdaptingIterator**: cast blocks inline during page adaptation.
- **In the format reader**: read with explicit type casting (Parquet/ORC readers support this natively).

**Recommendation**: Use format-native casting where possible (Parquet and ORC readers support
reading INT32 columns as INT64 via their projection schemas). Fall back to block-level casting
in the adapter for formats that don't support native widening.

### Integration with Existing Features

#### Hive Partition Columns

Partition columns are currently appended after schema resolution (in `enrichSchemaWithPartitionColumns`).
This works with all three strategies:

- **FIRST_FILE_WINS**: Partition columns appended after first file's schema (current behavior).
- **STRICT**: Partition columns appended after validation. Partition columns are NOT subject to
  strict validation — they come from paths, not file schemas.
- **UNION_BY_NAME**: Partition columns appended after merge. If a partition column name conflicts
  with a data column, the partition column wins (current behavior).

No changes needed.

#### Filter Pushdown

Filter pushdown currently produces per-query filter predicates (e.g., Parquet `FilterPredicate`).
These predicates reference column names from the planning-phase schema.

- **STRICT**: No change — all files have the same schema, same column names.
- **UNION_BY_NAME**: The filter may reference columns that don't exist in some files. For files
  missing a filtered column, the filter should evaluate to "unknown" (include the row group).
  Parquet's `FilterPredicate` already handles this gracefully — a predicate on a missing column
  returns "unknown" which means "don't skip". ORC behaves similarly.

No changes needed for basic filter pushdown. However, if we push type-widened filters (e.g.,
filter says `salary > 50000` with LONG type but the file has INT32), the Parquet/ORC reader
needs to handle the type mismatch in the predicate. This needs investigation.

#### Aggregate Pushdown (Phase 2)

UNION_BY_NAME is the **enabler** for aggregate pushdown Phase 2. When reading all file metadata
for schema merging, per-file statistics are available for free:

1. During schema merge, collect `SourceStatistics` for each file
2. Store statistics in `FileSplit.statistics` (the nullable field from Phase 1)
3. The optimizer rule `PushAggregatesToExternalSource` finds pre-populated statistics
   and takes the fast path (no file I/O on data nodes)

This is already designed in `AGGREGATE_PUSHDOWN_PLAN.md` Phase 2. UNION_BY_NAME makes it
free because we're reading footers anyway.

For STRICT mode, the same optimization applies — all files are scanned, statistics are available.

#### Column Projection

Currently, the optimizer pushes column projection to the format reader (read only needed columns).
With UNION_BY_NAME:

- The projected columns are a subset of the UNIFIED schema.
- For each file, the projected columns are intersected with the file's actual columns.
- Missing projected columns → NULL blocks (handled by SchemaAdaptingIterator).
- This is a natural composition — no special handling needed.

### Error Policy Interaction

`FormatReader` already has an `ErrorPolicy` (STRICT vs LENIENT). This is orthogonal to
`SchemaResolution`:

- `SchemaResolution` governs planning-time schema decisions.
- `ErrorPolicy` governs execution-time data reading errors (malformed rows, parse failures).

They are independent axes. A query can use `schema_resolution = "union_by_name"` (lenient
about schema differences) with `error_policy = "strict"` (fail on malformed data), or vice versa.

### Data Structures

#### SchemaReconciliationResult

The output of schema reconciliation during planning:

```java
record SchemaReconciliationResult(
    List<Attribute> unifiedSchema,
    Map<StoragePath, FileSchemaInfo> perFileInfo
)

record FileSchemaInfo(
    List<Attribute> fileSchema,         // original file schema
    @Nullable FileColumnMapping mapping, // null for FIRST_FILE_WINS
    @Nullable SourceStatistics statistics // null if not collected
)

record FileColumnMapping(
    int[] globalToLocalIndex,   // unified column i → file column index (-1 if missing)
    DataType[] casts            // null entries = no cast needed; non-null = cast to this type
)
```

For `FIRST_FILE_WINS`, `perFileInfo` contains only the first file (backward compatible).
For `STRICT`, `perFileInfo` contains all files (for statistics), mappings are identity.
For `UNION_BY_NAME`, `perFileInfo` contains all files with mappings and casts.

#### Extended FileSet

The existing `FileSet` carries the list of files. It should be extended to carry per-file
schema info:

```java
// FileSet already has:
List<StorageEntry> files();
PartitionMetadata partitionMetadata();

// Add:
@Nullable Map<StoragePath, FileSchemaInfo> fileSchemaInfo();
// null for FIRST_FILE_WINS (backward compatible)
// populated for STRICT and UNION_BY_NAME
```

This allows per-file metadata to flow from planning through to execution without changing
the existing plumbing significantly.

### Serialization Considerations

Per-file schema info must be serialized when `FileSet` is sent from coordinator to data nodes.
The `FileSchemaInfo` per file includes:
- Column mapping (int array + DataType array) — compact
- Statistics (existing `SourceStatistics` serialization) — already handled

For 1,000 files × 20 columns: ~80 KB for mappings + ~200 KB for statistics = ~280 KB total.
Negligible compared to the data being transferred.

---

## Implementation Phases

### Phase A: STRICT Mode

**Scope**: Validate all file schemas match. Collect statistics opportunistically.

1. Add `schema_resolution` WITH clause parameter parsing
2. Implement parallel metadata scanning in `ExternalSourceResolver`
3. Implement schema comparison logic
4. Implement `SchemaReconciliationResult` data structures
5. Wire statistics into `FileSplit` (aggregate pushdown Phase 2)
6. Add tests for: matching schemas, column count mismatch, type mismatch, name mismatch,
   nullability tolerance, partition column interaction, error messages
7. Add CSV-spec tests demonstrating the feature

**Estimated complexity**: Medium. Mostly new code in `ExternalSourceResolver` + data structures.

### Phase B: UNION_BY_NAME

**Scope**: Merge schemas, adapt reads.

1. Implement schema merge algorithm with type widening
2. Implement `FileColumnMapping` computation
3. Implement `SchemaAdaptingIterator` for NULL filling and type casting
4. Wire into the execution path (format reader wrapping or operator adaptation)
5. Handle filter pushdown with missing/widened columns
6. Add tests for: added columns, removed columns, type widening, mixed schemas,
   null filling, column ordering, filter pushdown interaction, aggregate pushdown interaction
7. Add CSV-spec tests demonstrating the feature

**Estimated complexity**: High. Requires changes across planning, optimization, and execution.

### Phase C: Polish & Defaults (Future)

1. Consider changing default to STRICT for Parquet/ORC
2. Add `filename` virtual column (DuckDB-inspired) to help users debug which file contributed rows
3. Case-insensitive column matching option
4. Metadata caching for repeated queries on the same glob

---

## Trade-off Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Metadata scanning | Eager (all files) | Better UX, enables statistics collection |
| STRICT type widening | No | Matches DuckDB/Spark. Use UNION_BY_NAME for widening. |
| UNION_BY_NAME type widening | Safe numeric only | Matches industry. Lossy/cross-category = error. |
| NULL filling implementation | Adapter layer | Single implementation, format readers stay simple. |
| Column matching | By name (case-sensitive) | Parquet/ORC use named columns. Case-insensitive as future option. |
| Column ordering | First-seen order | Deterministic, matches DuckDB. |
| LONG→DOUBLE widening | No | Lossy. Matches DuckDB/Spark consensus. |
| Sampling for large file sets | No | Risky. Use table formats for 10K+ heterogeneous files. |
| Default strategy | FIRST_FILE_WINS (now), STRICT (future) | Backward compatible now. Revisit after proving stability. |

---

## Open Questions for Team Discussion

1. **Default strategy**: Should we plan to change the default to STRICT once the feature is
   stable? This is the safest default but would be a breaking change for users with
   heterogeneous files who rely on silent schema ignorance.

2. **Type widening in STRICT mode**: Should STRICT allow safe numeric widening (int→long)?
   Or should it be truly strict (exact type match)?

3. **LONG→DOUBLE widening**: Allow it (ClickHouse does) or reject it (DuckDB/Spark don't)?

4. **DATE→DATETIME widening**: Allow in UNION_BY_NAME?

5. **`filename` column**: Should we add a virtual column showing the source file path?
   Useful for debugging UNION_BY_NAME queries. DuckDB has this.

6. **Sampling mode**: For 10K+ file globs, should we offer a `schema_sample_size = N` option
   that reads only N file schemas? Or just recommend table formats?

7. **Case sensitivity**: Should column name matching be case-sensitive by default?
   Parquet column names are case-sensitive. ORC column names are case-insensitive by convention.
   Should we follow the format's convention or be consistently case-sensitive?

8. **WITH clause vs format reader default**: Should individual format readers be able to
   set their own default strategy? E.g., Parquet defaults to STRICT, CSV defaults to
   FIRST_FILE_WINS? Or one global default for all formats?

---

## Appendix: Detailed Type Widening Matrix

The full proposed type widening hierarchy for UNION_BY_NAME:

```
BYTE → SHORT → INTEGER → LONG
                            ↑ (no further widening to DOUBLE — lossy)
FLOAT → DOUBLE
          ↑ (INTEGER → DOUBLE allowed — lossless for int32)

KEYWORD  (no widening to/from TEXT or any numeric)
TEXT     (no widening to/from KEYWORD or any numeric)
BOOLEAN  (no widening)
DATE     (no widening — TBD)
DATETIME (no widening — TBD)
IP       (no widening)
VERSION  (no widening)
GEO_POINT (no widening)
GEO_SHAPE (no widening)
```

Widening is transitive: if file A has BYTE and file B has LONG, the unified type is LONG
(BYTE→SHORT→INTEGER→LONG).

Widening is commutative: the order files are scanned doesn't affect the result.

---

## Appendix: Comparison of Error Messages

### DuckDB (strict mode)
```
Schema mismatch in Parquet glob: column "id" in parquet file "file2.parquet"
is of type BIGINT, but in the original file "file1.parquet" this column is of type INTEGER
```

### Spark (mergeSchema=true, incompatible types)
```
Failed to merge incompatible data types LongType and StringType
```

### ClickHouse (no common type)
```
NO_COMMON_TYPE: There is no supertype for types UInt64 and String
```

### Proposed (STRICT mode)
```
Schema mismatch in [s3://bucket/data/2024-02.parquet]:
  Column [salary] has type [LONG] but reference file [s3://bucket/data/2024-01.parquet] has type [INTEGER].
  Hint: use schema_resolution = "union_by_name" to automatically merge different schemas.
```

### Proposed (UNION_BY_NAME, incompatible types)
```
Cannot merge schemas for column [status]:
  File [s3://bucket/data/2024-01.parquet] has type [INTEGER]
  File [s3://bucket/data/2024-02.parquet] has type [KEYWORD]
  No compatible supertype exists. Consider using an explicit CAST in your query.
```
