# Aggregate Pushdown for External Sources — Implementation Plan

## Design Principle

Replace `AggregateExec → ExternalSourceExec` with `LocalSourceExec` on each data node.
The coordinator's FINAL AggregateExec is never touched — it merges intermediate values
from all data nodes regardless of whether each data node pushed down or scanned.

Each data node decides independently based on statistics availability:
- All splits have statistics → precompute from stats, no scanning
- Any split lacks statistics → normal scan for ALL splits (all-or-nothing)
  Parquet and ORC always write statistics, so this is the common case.
  Mixed-mode (precompute some, scan others) deferred to future work.

## Phase 1: Data Node Footer Reading

### Overview

On each data node, the local physical optimizer replaces the aggregate subtree with
a `LocalSourceExec` whose `LocalSupplier` reads file footers at execution time.
Files WITH statistics get their values from the footer. Files WITHOUT statistics
fall back to the normal scan path (the original AggregateExec → ExternalSourceExec
is kept for those splits).

### Hook for Phase 2

`FileSplit` gets a `@Nullable SourceStatistics statistics` field.
In Phase 1, this is always null — the supplier reads footers on the data node.
In Phase 2 (union-by-name), the coordinator populates this from schema merging.
The optimizer rule checks split statistics first and skips footer reads when present.

### Step 1: Add statistics field to FileSplit

Add `@Nullable SourceStatistics statistics` to `FileSplit`.
Phase 1: always null. Phase 2: populated by coordinator.
Update serialization to handle nullable statistics (write a boolean flag + data).

Files:
- `FileSplit.java` — add field, constructor, accessor, serialization
- `FileSplitProvider.java` — pass null for statistics in Phase 1

### Step 2: Rewrite PushAggregatesToExternalSource optimizer rule

Replace current approach (eliminate AggregateExec, set hint on ExternalSourceExec)
with the LocalSourceExec approach.

The rule:
1. Matches `AggregateExec(any mode) → ExternalSourceExec`
2. Checks: ungrouped only, format supports pushdown
3. Checks: all splits have statistics in FileSplit? (Phase 2 fast path)
   - Yes: build intermediate page from split stats, return LocalSourceExec
4. No pre-populated stats: check if format reader supports aggregate pushdown
   - Yes: return LocalSourceExec with FileStatsSupplier (Phase 1 lazy path)
   - No: return original plan (normal scan)

Output attributes:
- SINGLE mode: final aggregate attributes from AggregateExec.output()
- INITIAL mode: intermediate attributes from AggregateExec.intermediateAttributes()

Files:
- `PushAggregatesToExternalSource.java` — rewrite rule logic

### Step 3: Implement FileStatsSupplier

A `LocalSupplier` that reads file footers at execution time.

For each assigned split:
1. If split.statistics() != null → use pre-populated stats (Phase 2 path)
2. If split.statistics() == null → open file footer, extract statistics
3. If a file lacks statistics → that split cannot be pushed down

Merge strategy across splits:
- COUNT_STAR: sum of per-file row counts
- MIN: minimum of per-file minimums
- MAX: maximum of per-file maximums

If ALL splits yield statistics, produce a single intermediate format page:
- SINGLE mode: [count, min_salary, max_age]
- INITIAL mode: [count, seen=true, min_salary, seen=true, max_age, seen=true]

All-or-nothing: if ANY split lacks statistics, the supplier signals failure
and the optimizer keeps the original plan (normal scan for all splits).
Parquet and ORC always write statistics so this fallback is rare.

The supplier needs:
- List of splits (captured from ExternalSourceExec)
- Source config (for creating StorageProvider)
- Source type (for resolving FormatReader)
- AggregateSpec list (what to compute)
- AggregatorMode (SINGLE vs INITIAL determines output format)
- FormatReaderRegistry (for opening files)

Files:
- `FileStatsSupplier.java` (new) — implements LocalSupplier

### Step 4: Remove execution-path aggregate pushdown

Remove all hint-through-execution-chain infrastructure since the optimizer
handles everything via LocalSourceExec.

Remove:
- `ExternalSourceExec.PushedAggregate` record and related fields
- `FormatReader.withPushedAggregate()` default method
- `SourceOperatorContext.pushedAggregate` field and builder method
- `FileSourceFactory` aggregate hint plumbing
- `AsyncExternalSourceOperatorFactory` aggregate merge logic
- `AggregatePageMerger` class
- `ParquetFormatReader.createAggregateResultIterator()` and `createStatBlock()`
- `OrcFormatReader.createAggregateResultIterator()` and `createStatBlock()`
- `LocalExecutionPlanner` projectedColumns aggregate check

Keep:
- `AggregateSpec` record in SPI (used by optimizer rule and supplier)
- `AggregatePushdownSupport` interface (format capability declaration)
- `FormatReader.aggregatePushdownSupport()` (optimizer probes capability)
- `FormatReaderRegistry.findByName()` (nullable lookup for optimizer)
- `ParquetAggregatePushdownSupport` and `OrcAggregatePushdownSupport`

### Step 5: Footer statistics extraction

Move statistics extraction logic from format readers into reusable utilities
that FileStatsSupplier can call.

For Parquet: read footer → iterate row groups → extract min/max/count
For ORC: open reader → get file-level statistics → extract min/max/count

These are the same operations as the current createAggregateResultIterator
methods but returning SourceStatistics instead of Page.

Files:
- `ParquetFileStatistics.java` (new) — extracts SourceStatistics from Parquet footer
- `OrcFileStatistics.java` (new) — extracts SourceStatistics from ORC file stats

### Step 6: Intermediate format production

Build blocks in the correct format based on AggregatorMode.

SINGLE mode output (final values):
  Block[0] = count (LONG)
  Block[1] = min_salary (LONG)
  Block[2] = max_age (INT)

INITIAL mode output (intermediate channels):
  Block[0] = count (LONG)
  Block[1] = seen (BOOLEAN = true)
  Block[2] = min_salary (LONG)
  Block[3] = seen (BOOLEAN = true)
  Block[4] = max_age (INT)
  Block[5] = seen (BOOLEAN = true)

The intermediate attributes come from AggregateFunction.intermediateAttributes()
for each aggregate in the plan.

### Step 7: Update tests

- Rewrite PushAggregatesToExternalSourceTests for LocalSourceExec output
- Rewrite AggregatePageMergerTests → remove (merger removed)
- Adapt ParquetAggregatePushdownTests to test via FileStatsSupplier
- Adapt OrcAggregatePushdownTests to test via FileStatsSupplier
- Add tests for INITIAL mode intermediate format production
- Add tests for mixed statistics (some splits have stats, some don't → fallback)
- Add tests for multi-split merge (3 files, merge counts/mins/maxes)

## Phase 2: Coordinator Pre-Populates Statistics (Future — Union-by-Name)

### When this activates

When union-by-name / schema merging lands, the coordinator reads ALL file
metadata to merge schemas. At that point, collecting per-file statistics
is free — the footer is already in memory.

### What changes

1. **FileSplitProvider**: When creating splits, populate `statistics` field
   from the metadata that was already read during schema merging.

2. **ExternalSourceResolver**: During multi-file resolution, collect
   SourceStatistics for each file alongside the schema.

3. **Optimizer rule (no changes needed)**: The existing rule checks
   `split.statistics() != null` first. Phase 2 pre-populates this,
   so the rule takes the fast path (no file I/O on data node).

4. **Coordinator-level pushdown (optional optimization)**: For SINGLE mode
   queries where the coordinator has all statistics, the coordinator's
   local optimizer can replace the entire plan with LocalSourceExec
   without sending anything to data nodes. This is an optimization
   over Phase 1 where data nodes do the work.

### Cost analysis

Phase 1 (data node footer reads):
- Per file: one range read of footer (4-32KB, one HTTP round-trip)
- 100 files on one data node: 100 footer reads (~100ms on S3)
- Memory: ~100 bytes per column stat × columns × files (temporary)
- Compare to full scan: orders of magnitude less I/O

Phase 2 (pre-populated from schema merge):
- Zero incremental I/O (metadata already read for schema)
- Per split serialization overhead: ~100 bytes per column per file
- 1000 files × 20 columns: ~2MB in split serialization (negligible)
- Data node: zero file I/O for aggregate pushdown

### Hints for Phase 2 implementer

When implementing union-by-name schema merging:

1. The `FormatReader.metadata(StorageObject)` method already returns
   `SourceMetadata` which contains `Optional<SourceStatistics>`.
   Store this per-file alongside the schema.

2. When creating FileSplits in `FileSplitProvider`, pass the
   `SourceStatistics` from the metadata into the split constructor.
   The nullable field is already there from Phase 1.

3. The optimizer rule `PushAggregatesToExternalSource` already checks
   `split.statistics() != null` as its first code path. When statistics
   are pre-populated, this path fires and the FileStatsSupplier is
   never created — no file I/O on the data node.

4. For coordinator-level pushdown in SINGLE mode: add a check in the
   rule for when ALL splits have statistics AND mode is SINGLE.
   In that case, build the final-values page directly in the rule
   (no LocalSupplier needed) and return LocalSourceExec immediately.
   This avoids sending splits to data nodes entirely.
