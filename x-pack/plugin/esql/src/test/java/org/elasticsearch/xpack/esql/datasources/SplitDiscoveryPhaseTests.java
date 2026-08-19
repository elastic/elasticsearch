/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SplitDiscoveryPhaseTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testExternalSourceExecGetsSplitsAttached() {
        FileList fileList = createFileList(3);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertTrue(result instanceof ExternalSourceExec);
        ExternalSourceExec resolved = (ExternalSourceExec) result;
        assertEquals(3, resolved.splits().size());
        for (ExternalSplit split : resolved.splits()) {
            assertTrue(split instanceof FileSplit);
        }
    }

    public void testNoExternalSourceUnchanged() {
        PhysicalPlan leaf = createExternalSourceExec(FileList.UNRESOLVED, "parquet");
        LimitExec limit = new LimitExec(SRC, leaf, new Literal(SRC, 10, DataType.INTEGER), null);

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(limit, factories);

        assertTrue(result instanceof LimitExec);
        ExternalSourceExec child = (ExternalSourceExec) ((LimitExec) result).child();
        assertTrue(child.splits().isEmpty());
    }

    public void testScanStatsReportedForExternalSource() {
        // createFileList sizes file i as 100*(i+1); three files => 100 + 200 + 300 bytes.
        FileList fileList = createFileList(3);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        SplitDiscoveryPhase.Result result = SplitDiscoveryPhase.resolveExternalSplitsWithStats(
            exec,
            factories,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );

        // Whole-file splits (no format registry), so one split per file and bytes == total file size.
        assertEquals(3, result.filesScanned());
        assertEquals(3, result.splitsScanned());
        assertEquals(600L, result.bytesScanned());
    }

    /**
     * The fragment path's seed derivation. A {@code Filter} above an {@code ExternalRelation} inside a fragment must be
     * recovered as that relation's pruning seed — without this step there is no pruning at all, because lowering the
     * relation to an {@code ExternalSourceExec} discards the
     * {@code Filter} that stood above it.
     */
    public void testGuardedRelationsCollectsFilterAncestorsInFragment() {
        ExternalRelation relation = externalRelation();
        Expression year = new Equals(SRC, relation.output().get(0), new Literal(SRC, 2025, DataType.INTEGER));
        Expression month = new Equals(SRC, relation.output().get(1), new Literal(SRC, 6, DataType.INTEGER));
        // Two stacked Filters, as PushDownAndCombineFilters may leave them: both must reach the relation.
        LogicalPlan fragment = new Filter(SRC, new Filter(SRC, relation, year), month);

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(fragment);

        assertEquals(1, guarded.size());
        assertSame(relation, guarded.get(0).relation());
        assertEquals("both Filter ancestors must seed the relation", 2, guarded.get(0).filters().size());
        assertTrue(guarded.get(0).filters().containsAll(List.of(year, month)));
    }

    /** An AND condition is split into its conjuncts, so each can be matched against a partition column on its own. */
    public void testGuardedRelationsSplitsConjunction() {
        ExternalRelation relation = externalRelation();
        Expression year = new Equals(SRC, relation.output().get(0), new Literal(SRC, 2025, DataType.INTEGER));
        Expression month = new Equals(SRC, relation.output().get(1), new Literal(SRC, 6, DataType.INTEGER));
        LogicalPlan fragment = new Filter(SRC, relation, new And(SRC, year, month));

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(fragment);

        assertEquals(1, guarded.size());
        assertEquals("`a AND b` must arrive as two prunable conjuncts, not one opaque expression", 2, guarded.get(0).filters().size());
    }

    /**
     * A filter above a {@code LIMIT} must not become a pruning seed. {@code SORT id | LIMIT 4 | WHERE year == 2025}
     * takes four rows and filters them afterwards; pruning the non-2025 files first would refill the limit window from
     * the survivors and return rows the query never asked for. The seed must be dropped at the cardinality-sensitive
     * node, leaving the relation unguarded (full scan, correct answer).
     */
    public void testGuardedRelationsDoesNotSeedPastLimit() {
        ExternalRelation relation = externalRelation();
        Expression year = new Equals(SRC, relation.output().get(0), new Literal(SRC, 2025, DataType.INTEGER));
        LogicalPlan fragment = new Filter(SRC, new Limit(SRC, new Literal(SRC, 4, DataType.INTEGER), relation), year);

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(fragment);

        assertEquals(1, guarded.size());
        assertTrue(
            "a filter above a LIMIT must not prune the source — LIMIT does not commute with WHERE",
            guarded.get(0).filters().isEmpty()
        );
    }

    /** The complement: with the filter <em>below</em> the limit, WHERE genuinely runs first and pruning is sound. */
    public void testGuardedRelationsSeedsFilterBelowLimit() {
        ExternalRelation relation = externalRelation();
        Expression year = new Equals(SRC, relation.output().get(0), new Literal(SRC, 2025, DataType.INTEGER));
        LogicalPlan fragment = new Limit(SRC, new Literal(SRC, 4, DataType.INTEGER), new Filter(SRC, relation, year));

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(fragment);

        assertEquals(1, guarded.size());
        assertEquals("a filter below the LIMIT still prunes", List.of(year), guarded.get(0).filters());
    }

    /**
     * The physical twin of {@link #testGuardedRelationsDoesNotSeedPastLimit}: a {@code FilterExec} above a
     * {@code LimitExec} must not reach the source either. The two walks encode the same rule and have to agree — the
     * whole reason this bug existed is that a second path was added and the rule was not carried over to it.
     */
    public void testFilterExecAboveLimitExecDoesNotReachSource() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        LimitExec limit = new LimitExec(SRC, exec, new Literal(SRC, 4, DataType.INTEGER), null);
        Expression year = new Equals(SRC, outputAttr(exec, "year"), new Literal(SRC, 2025, DataType.INTEGER));
        FilterExec filter = new FilterExec(SRC, limit, year);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(filter, factories);

        assertTrue(
            "a filter above a LIMIT must not prune the source on the physical path either",
            recorder.lastContext.filterHints().isEmpty()
        );
    }

    /** An unfiltered relation is guarded by nothing: every file must be read, and no conjunct may be invented. */
    public void testGuardedRelationsWithoutFilterHasEmptySeed() {
        ExternalRelation relation = externalRelation();

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(relation);

        assertEquals(1, guarded.size());
        assertTrue("no Filter above the relation means no pruning seed", guarded.get(0).filters().isEmpty());
    }

    /** An ExternalRelation with `year`/`month` output attributes, standing in for a Hive-partitioned dataset. */
    private static ExternalRelation externalRelation() {
        List<Attribute> output = List.of(fieldAttr("year", DataType.INTEGER), fieldAttr("month", DataType.INTEGER));
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(
            output,
            "parquet",
            "s3://bucket/data/*.parquet",
            null,
            null,
            Map.of(),
            Map.of()
        );
        return new ExternalRelation(SRC, "s3://bucket/data/*.parquet", metadata, output, FileList.UNRESOLVED, Map.of());
    }

    public void testScanStatsZeroWhenNoSplitsDiscovered() {
        ExternalSourceExec exec = createExternalSourceExec(FileList.UNRESOLVED, "parquet");
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        SplitDiscoveryPhase.Result result = SplitDiscoveryPhase.resolveExternalSplitsWithStats(
            exec,
            factories,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );

        assertEquals(0, result.filesScanned());
        assertEquals(0, result.splitsScanned());
        assertEquals(0L, result.bytesScanned());
    }

    /**
     * Exhaustive prune: a resolved, non-empty fileList whose split discovery yields nothing (every file pruned) must
     * have its {@link ExternalSourceExec} swapped to read {@link FileList#EMPTY}, so the read path scans nothing
     * instead of reading the whole dataset only to drop every row in a downstream filter. Stats stay honestly zero.
     */
    public void testExhaustivelyPrunedResolvedFileListSwappedToEmpty() {
        FileList fileList = createFileList(3); // resolved, non-empty
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        // A provider that exhaustively prunes: zero splits out, reported as a row-count-safe prune.
        Map<String, ExternalSourceFactory> factories = Map.of(
            "parquet",
            testFactory(new FixedSplitProvider(new SplitDiscoveryResult(List.of(), 0, true)))
        );

        SplitDiscoveryPhase.Result result = SplitDiscoveryPhase.resolveExternalSplitsWithStats(
            exec,
            factories,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );

        assertTrue(result.plan() instanceof ExternalSourceExec);
        ExternalSourceExec resolved = (ExternalSourceExec) result.plan();
        assertSame("an exhaustively-pruned resolved fileList must be swapped to FileList.EMPTY", FileList.EMPTY, resolved.fileList());
        assertTrue(resolved.splits().isEmpty());
        assertEquals(0, result.filesScanned());
        assertEquals(0, result.splitsScanned());
        assertEquals(0L, result.bytesScanned());
    }

    /**
     * Empty splits are NOT enough to swap: when the provider reports the empty result is not an exhaustive prune
     * (e.g. files were dropped by the row-count-unsafe no-column-overlap heuristic, whose rows a {@code COUNT(*)}
     * still needs), the source must fall through unchanged to the whole read so the row filter runs.
     */
    public void testEmptySplitsNotExhaustivelyPrunedNotSwapped() {
        FileList fileList = createFileList(3); // resolved, non-empty
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        // Zero splits, but explicitly NOT an exhaustive prune.
        Map<String, ExternalSourceFactory> factories = Map.of(
            "parquet",
            testFactory(new FixedSplitProvider(new SplitDiscoveryResult(List.of(), 0, false)))
        );

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertSame("a non-exhaustive empty result must fall through unchanged, not be swapped to EMPTY", exec, result);
        assertSame("the resolved fileList must be preserved for the whole read", fileList, ((ExternalSourceExec) result).fileList());
    }

    /**
     * An unresolved fileList (glob not yet expanded — resolved and read at runtime) must NOT be swapped when discovery
     * returns no splits: there is nothing to prune yet, and swapping to EMPTY would make the source read nothing at
     * all. It falls through unchanged to the runtime whole-fileList read.
     */
    public void testUnresolvedFileListNotSwappedOnEmptySplits() {
        ExternalSourceExec exec = createExternalSourceExec(FileList.UNRESOLVED, "parquet");
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new RecordingSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertSame("an unresolved fileList must fall through unchanged, not be swapped to EMPTY", exec, result);
        assertSame(FileList.UNRESOLVED, ((ExternalSourceExec) result).fileList());
    }

    /**
     * A non-splitting SINGLE/connector source (no resolved fileList, carries {@link FileList#UNRESOLVED} in
     * production) returns empty splits by design and must be left untouched — the whole-read at runtime is the
     * intended behavior, not an exhaustive prune.
     */
    public void testSingleProviderLeavesUnresolvedFileListUnchanged() {
        ExternalSourceExec exec = createExternalSourceExec(FileList.UNRESOLVED, "unknown_type");

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of());

        assertSame("SINGLE provider on an unresolved fileList must not be swapped to EMPTY", exec, result);
    }

    /**
     * An already-empty glob ({@link FileList#EMPTY}, {@code fileCount() == 0}) reads nothing regardless, so the
     * {@code fileCount() > 0} guard leaves it untouched — no redundant swap.
     */
    public void testAlreadyEmptyFileListNotReswapped() {
        ExternalSourceExec exec = createExternalSourceExec(FileList.EMPTY, "parquet");
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new RecordingSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertSame("an already-empty glob needs no swap", exec, result);
    }

    /** {@link ExternalRelation#withFileList} swaps only the fileList, preserving the rest of the relation. */
    public void testExternalRelationWithFileListSwapsOnlyFileList() {
        ExternalRelation relation = externalRelation();
        assertSame(FileList.UNRESOLVED, relation.fileList());

        ExternalRelation swapped = relation.withFileList(FileList.EMPTY);

        assertSame(FileList.EMPTY, swapped.fileList());
        assertEquals(relation.sourcePath(), swapped.sourcePath());
        assertEquals(relation.output(), swapped.output());
        assertEquals(relation.metadata(), swapped.metadata());
    }

    public void testFilterExecAboveExternalSourceCollectsFilters() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        Expression condition = new Equals(SRC, outputAttr(exec, "year"), new Literal(SRC, 2024, DataType.INTEGER));
        FilterExec filter = new FilterExec(SRC, exec, condition);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(filter, factories);

        assertEquals(1, recorder.lastContext.filterHints().size());
    }

    /**
     * Fragment-path seeding: when the {@code ExternalSourceExec} is lowered from an isolated {@code ExternalRelation}
     * — no surviving {@code FilterExec} ancestor — the coordinator must seed the recursive walk with the fragment's
     * partition-column filter conjuncts, or L1 partition pruning never sees them.
     * Asserts the seed overload plumbs {@code seedFilters} through to {@link SplitDiscoveryContext#filterHints()}.
     */
    public void testSeedFiltersReachContextWithoutFilterExec() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        // The seed filter references the relation's own `year` output attribute (matching NameId), so the
        // output-binding guard keeps it.
        Expression seed = new Equals(SRC, outputAttr(exec, "year"), new Literal(SRC, 2025, DataType.INTEGER));

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplitsWithStats(
            exec,
            factories,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            List.of(seed)
        );

        assertEquals(
            "seed filter must reach the split-discovery context on the fragment path",
            1,
            recorder.lastContext.filterHints().size()
        );
        assertSame(seed, recorder.lastContext.filterHints().get(0));
    }

    /**
     * Shadowing guard: a seed conjunct that references an attribute NOT in the relation's output — e.g. a downstream
     * {@code EVAL year = ...} that shadows the partition column with a fresh {@code NameId} — must be dropped before
     * pruning, so the split provider never prunes files by the path partition value for a
     * row-derived column. Binding is by id, not name: a same-named attribute with a different id is not kept.
     */
    public void testSeedFilterOverShadowingAttributeIsDropped() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        // A fresh `year` FieldAttribute — same NAME as the relation's `year` output column, but a distinct NameId,
        // standing in for a downstream-generated (EVAL/DISSECT/GROK) column. The guard must drop the filter over it.
        Expression shadowing = new Equals(SRC, fieldAttr("year", DataType.INTEGER), new Literal(SRC, 2025, DataType.INTEGER));

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplitsWithStats(
            exec,
            factories,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            List.of(shadowing)
        );

        assertTrue(
            "a filter over a shadowing (non-output) attribute must be dropped, not used to prune",
            recorder.lastContext.filterHints().isEmpty()
        );
    }

    public void testMultipleExternalSourcesEachGetOwnSplits() {
        FileList fileList1 = createFileList(2);
        FileList fileList2 = createFileList(3);
        ExternalSourceExec exec1 = createExternalSourceExec(fileList1, "parquet");
        ExternalSourceExec exec2 = createExternalSourceExec(fileList2, "csv");

        LimitExec limit = new LimitExec(SRC, exec1, new Literal(SRC, 10, DataType.INTEGER), null);

        Map<String, ExternalSourceFactory> factories = Map.of(
            "parquet",
            testFactory(new FileSplitProvider()),
            "csv",
            testFactory(new FileSplitProvider())
        );

        PhysicalPlan result1 = SplitDiscoveryPhase.resolveExternalSplits(limit, factories);
        PhysicalPlan result2 = SplitDiscoveryPhase.resolveExternalSplits(exec2, factories);

        ExternalSourceExec resolved1 = (ExternalSourceExec) ((LimitExec) result1).child();
        ExternalSourceExec resolved2 = (ExternalSourceExec) result2;

        assertEquals(2, resolved1.splits().size());
        assertEquals(3, resolved2.splits().size());
    }

    public void testUnknownSourceTypeDefaultsToSingleProvider() {
        FileList fileList = createFileList(3);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "unknown_type");

        Map<String, ExternalSourceFactory> factories = Map.of();

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertTrue(result instanceof ExternalSourceExec);
        ExternalSourceExec resolved = (ExternalSourceExec) result;
        assertTrue(resolved.splits().isEmpty());
    }

    public void testSplitCountMatchesFileCount() {
        int fileCount = randomIntBetween(1, 10);
        FileList fileList = createFileList(fileCount);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        ExternalSourceExec resolved = (ExternalSourceExec) result;
        assertEquals(fileCount, resolved.splits().size());
    }

    public void testFiltersFromDifferentBranchesAreNotMixed() {
        FileList fileList1 = createFileList(2);
        FileList fileList2 = createFileList(2);
        ExternalSourceExec exec1 = createExternalSourceExec(fileList1, "parquet");
        ExternalSourceExec exec2 = createExternalSourceExec(fileList2, "csv");

        Expression cond1 = new Equals(SRC, outputAttr(exec1, "year"), new Literal(SRC, 2024, DataType.INTEGER));
        FilterExec filter1 = new FilterExec(SRC, exec1, cond1);

        Expression cond2 = new Equals(SRC, outputAttr(exec2, "month"), new Literal(SRC, 6, DataType.INTEGER));
        FilterExec filter2 = new FilterExec(SRC, exec2, cond2);

        RecordingSplitProvider recorder1 = new RecordingSplitProvider();
        RecordingSplitProvider recorder2 = new RecordingSplitProvider();

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder1), "csv", testFactory(recorder2));

        SplitDiscoveryPhase.resolveExternalSplits(filter1, factories);
        SplitDiscoveryPhase.resolveExternalSplits(filter2, factories);

        assertEquals(1, recorder1.lastContext.filterHints().size());
        assertEquals(1, recorder2.lastContext.filterHints().size());
    }

    public void testNestedFiltersAccumulateForDescendantSource() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        Expression cond1 = new Equals(SRC, outputAttr(exec, "year"), new Literal(SRC, 2024, DataType.INTEGER));
        Expression cond2 = new Equals(SRC, outputAttr(exec, "month"), new Literal(SRC, 6, DataType.INTEGER));
        FilterExec inner = new FilterExec(SRC, exec, cond1);
        FilterExec outer = new FilterExec(SRC, inner, cond2);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(outer, factories);

        assertEquals(2, recorder.lastContext.filterHints().size());
    }

    public void testCancellationSignalThreadedIntoContext() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplitsWithStats(
            exec,
            factories,
            org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> true
        );

        assertNotNull(recorder.lastContext);
        assertTrue(
            "cancellation signal must be threaded into the split discovery context",
            recorder.lastContext.isCancelled().getAsBoolean()
        );
    }

    public void testDefaultContextIsNotCancelled() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertNotNull(recorder.lastContext);
        assertFalse(recorder.lastContext.isCancelled().getAsBoolean());
    }

    public void testNoFiltersWhenNoFilterExecInPlan() {
        FileList fileList = createFileList(2);
        ExternalSourceExec exec = createExternalSourceExec(fileList, "parquet");
        LimitExec limit = new LimitExec(SRC, exec, new Literal(SRC, 10, DataType.INTEGER), null);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(limit, factories);

        assertTrue(recorder.lastContext.filterHints().isEmpty());
    }

    // -- helpers --

    private static FileList createFileList(int fileCount) {
        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/file" + i + ".parquet"), 100 * (i + 1), Instant.EPOCH));
        }
        return GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");
    }

    private static ExternalSourceExec createExternalSourceExec(FileList fileList, String sourceType) {
        // Output carries a partition-like `year` and a `month` so tests can build a filter over the SAME attribute
        // instances (matching NameId), which is what the analyzer produces in a real plan and what
        // SplitDiscoveryPhase's NameId-binding guard requires.
        List<Attribute> attrs = List.of(
            fieldAttr("id", DataType.LONG),
            fieldAttr("name", DataType.KEYWORD),
            fieldAttr("year", DataType.INTEGER),
            fieldAttr("month", DataType.INTEGER)
        );
        return new ExternalSourceExec(SRC, "s3://bucket/data/*.parquet", sourceType, attrs, Map.of(), Map.of(), null, null).withFileList(
            fileList
        );
    }

    /** The output attribute named {@code name} on {@code exec} — used to build filters whose reference id matches the relation output. */
    private static Attribute outputAttr(ExternalSourceExec exec, String name) {
        return exec.output().stream().filter(a -> a.name().equals(name)).findFirst().orElseThrow();
    }

    private static Attribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalSourceFactory testFactory(SplitProvider provider) {
        return new ExternalSourceFactory() {

            @Override
            public void validateConfig(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException("test stub does not implement validation");
            }

            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                return null;
            }

            @Override
            public SplitProvider splitProvider() {
                return provider;
            }
        };
    }

    private static class RecordingSplitProvider implements SplitProvider {
        SplitDiscoveryContext lastContext;

        @Override
        public SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context) {
            this.lastContext = context;
            return SplitDiscoveryResult.EMPTY;
        }
    }

    /** Returns a fixed {@link SplitDiscoveryResult}, so a test can pin the phase's reaction to a specific result. */
    private static class FixedSplitProvider implements SplitProvider {
        private final SplitDiscoveryResult result;

        FixedSplitProvider(SplitDiscoveryResult result) {
            this.result = result;
        }

        @Override
        public SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context) {
            return result;
        }
    }
}
