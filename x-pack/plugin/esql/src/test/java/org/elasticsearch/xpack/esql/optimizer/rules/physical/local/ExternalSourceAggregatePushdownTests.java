/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Fold-resolution tests for {@link ExternalSourceAggregatePushdown} — the shared COUNT/MIN/MAX-from-stats
 * resolution ({@code resolveFromStats}, {@code servableExtremum}, {@code buildBlock}, {@code canServeAllFromStats},
 * {@code partitionColumnNames}) plus the helper-level matrices. Exercised through the {@link PushStatsToExternalSource}
 * rule (the sole ES stats-serving rule; the former {@code PushAggregatesToExternalSource} was collapsed into it, and
 * these are its migrated fold-behavior tests). {@link PushStatsToExternalSourceTests} is the sibling that covers the
 * rule's own wiring (alias/Eval/Project/Filter unwrapping, mode/grouping bails, the format pushability gate); a few
 * shared-shape scenarios appear in both by design — the split is fold-resolution (here) vs rule-plumbing (there).
 */
public class ExternalSourceAggregatePushdownTests extends ESTestCase {

    private static final ReferenceAttribute AGE = referenceAttribute("age", DataType.INTEGER);
    private static final ReferenceAttribute SCORE = referenceAttribute("score", DataType.DOUBLE);

    public void testServableExtremumRejectsUnrepresentableButAllowsLosslessNarrowing() {
        // Same-type and lossless cases are served.
        assertEquals(5L, ExternalSourceAggregatePushdown.servableExtremum(5L, DataType.LONG));
        assertEquals(5L, ExternalSourceAggregatePushdown.servableExtremum(5L, DataType.INTEGER)); // in-range long-for-int (ORC int32)
        assertEquals(5.0, ExternalSourceAggregatePushdown.servableExtremum(5.0, DataType.DOUBLE));
        assertEquals(5.0, ExternalSourceAggregatePushdown.servableExtremum(5.0, DataType.LONG)); // exact integral double, buildBlock -> 5
        assertEquals("a", ExternalSourceAggregatePushdown.servableExtremum("a", DataType.KEYWORD));
        assertEquals(Long.MAX_VALUE, ExternalSourceAggregatePushdown.servableExtremum(Long.MAX_VALUE, DataType.LONG)); // long boundary
        // Unrepresentable in the resolved integral type -> safe-miss (null), never coerce-overflow.
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(1.0e20, DataType.LONG)); // double beyond long range
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(5.5, DataType.LONG)); // fractional
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(3.0e10, DataType.INTEGER)); // double beyond int range
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(5_000_000_000L, DataType.INTEGER)); // LONG beyond int range
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(null, DataType.LONG));

        // F1 invariant (elastic/elasticsearch#150920): servableExtremum's servable set MUST equal buildBlock's arm
        // set — a type with no arm safe-misses (re-scans) rather than throwing or serving a wrong coercion.
        // UNSIGNED_LONG became servable when elastic/elasticsearch#152858 made Parquet sign-flip-encode its stat
        // into ESQL's wire form (the LONG arm then serves the encoded value byte-identically to the scan), so it
        // now serves; VERSION still has no arm and safe-misses, keeping the invariant.
        assertEquals(
            "UNSIGNED_LONG serves via the sign-flip-encoded LONG arm",
            5L,
            ExternalSourceAggregatePushdown.servableExtremum(5L, DataType.UNSIGNED_LONG)
        );
        assertNull(
            "VERSION has no buildBlock arm -> safe-miss",
            ExternalSourceAggregatePushdown.servableExtremum(new BytesRef("1.0.0"), DataType.VERSION)
        );
        // The buildable non-integral arms still serve as-is.
        assertEquals(true, ExternalSourceAggregatePushdown.servableExtremum(true, DataType.BOOLEAN));
    }

    /**
     * Cross-declaration type-mismatch guard: a shared file+config cache entry can be pollinated by a sibling dataset
     * declaring a column a DIFFERENT type (the reconcile matches path+mtime+config, never the declared type), so each
     * non-integral {@code servableExtremum} arm must safe-miss (null) a value that is not its harvested Java type —
     * otherwise {@code buildBlock} crashes ({@code (Number)} cast / {@code parseBoolean(BytesRef.toString()=hex)}) or
     * serves a bogus keyword ({@code toBytesRef(Number.toString())}). The IP arm additionally requires the fixed
     * 16-byte {@code InetAddressPoint} encoding a variable-length keyword {@code BytesRef} would violate.
     */
    public void testServableExtremumSafeMissesCrossDeclarationForeignTypes() {
        // DOUBLE arm serves only a Number; a foreign BytesRef/Boolean safe-misses.
        assertEquals(5.0, ExternalSourceAggregatePushdown.servableExtremum(5.0, DataType.DOUBLE));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(new BytesRef("5.0"), DataType.DOUBLE));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(true, DataType.DOUBLE));
        // BOOLEAN arm serves only a Boolean; a foreign BytesRef/Number safe-misses.
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(new BytesRef("true"), DataType.BOOLEAN));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(1L, DataType.BOOLEAN));
        // KEYWORD (BYTES_REF) arm serves BytesRef/String; a foreign Number/Boolean safe-misses.
        assertEquals(new BytesRef("a"), ExternalSourceAggregatePushdown.servableExtremum(new BytesRef("a"), DataType.KEYWORD));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(9, DataType.KEYWORD));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(9L, DataType.KEYWORD));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(true, DataType.KEYWORD));
        // IP (BYTES_REF) arm serves ONLY the fixed 16-byte encoding; variable-length BytesRef / String / Number safe-miss.
        BytesRef ip16 = new BytesRef(new byte[16]);
        assertEquals(ip16, ExternalSourceAggregatePushdown.servableExtremum(ip16, DataType.IP));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(new BytesRef("1.2.3.4"), DataType.IP));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum("1.2.3.4", DataType.IP));
        assertNull(ExternalSourceAggregatePushdown.servableExtremum(9L, DataType.IP));
    }

    public void testBuildBlockServesIpAsEncodedBytesRefNotNull() {
        // IP is in MIN_MAX_TYPES and harvested as its 16-byte InetAddressPoint encoding. buildBlock must
        // reconstruct a real IP block from it -- before the IP arm existed it fell to the default and, since
        // the value is a BytesRef (not a Number), served a null block: a warm MIN/MAX(ip) that answered NULL.
        BytesRef encoded = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0.1")));
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (Block block = ExternalSourceAggregatePushdown.buildBlock(blockFactory, encoded, DataType.IP)) {
            assertTrue("IP must build a BytesRefBlock, got " + block.getClass().getSimpleName(), block instanceof BytesRefBlock);
            assertFalse("MIN/MAX(ip) must serve the encoded address, not a null block", block.isNull(0));
            assertEquals(encoded, ((BytesRefBlock) block).getBytesRef(0, new BytesRef()));
        }
    }

    // --- SINGLE mode tests ---

    public void testCountStarPushedInSingleMode() {
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(statsMetadata(1000L, null, null)), countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(1, local.output().size());
        Page page = local.supplier().get();
        assertNotNull(page);
        assertEquals(1, page.getPositionCount());
        assertEquals(1000L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    public void testCountFieldPushedInSingleMode() {
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(statsMetadata(1000L, "age", 50L)), countFieldAlias(AGE));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(950L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testMinPushedInSingleMode() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L);
        metadata.put("_stats.columns.age.min", 18);
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("m", new Min(Source.EMPTY, AGE)));

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMaxPushedInSingleMode() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L);
        metadata.put("_stats.columns.age.max", 99);
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("m", new Max(Source.EMPTY, AGE)));

        as(applyRule(agg), LocalSourceExec.class);
    }

    // --- INITIAL mode tests (intermediate blocks: value + seen) ---

    public void testCountStarPushedInInitialMode() {
        var agg = aggregateExec(AggregatorMode.INITIAL, externalSource(statsMetadata(500L, null, null)), countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(agg.intermediateAttributes(), local.output());
        Page page = local.supplier().get();
        assertNotNull(page);
        assertEquals(2, page.getBlockCount());
        assertEquals(500L, as(page.getBlock(0), LongBlock.class).getLong(0));
        assertTrue(as(page.getBlock(1), BooleanBlock.class).getBoolean(0));
    }

    public void testMinPushedInInitialMode() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L);
        metadata.put("_stats.columns.age.min", 18);
        var agg = aggregateExec(AggregatorMode.INITIAL, externalSource(metadata), alias("m", new Min(Source.EMPTY, AGE)));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(2, page.getBlockCount());
        assertTrue(as(page.getBlock(1), BooleanBlock.class).getBoolean(0));
    }

    public void testMultipleAggsPushedInInitialMode() {
        Map<String, Object> metadata = statsMetadata(500L, "score", 10L);
        metadata.put("_stats.columns.score.min", 1.0);
        metadata.put("_stats.columns.score.max", 100.0);

        var agg = aggregateExec(
            AggregatorMode.INITIAL,
            externalSource(metadata),
            countStarAlias(),
            alias("mn", new Min(Source.EMPTY, SCORE)),
            alias("mx", new Max(Source.EMPTY, SCORE))
        );

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(agg.intermediateAttributes(), local.output());
        Page page = local.supplier().get();
        assertEquals(6, page.getBlockCount());
        assertEquals(500L, as(page.getBlock(0), LongBlock.class).getLong(0));
        assertTrue(as(page.getBlock(1), BooleanBlock.class).getBoolean(0));
        assertTrue(as(page.getBlock(3), BooleanBlock.class).getBoolean(0));
        assertTrue(as(page.getBlock(5), BooleanBlock.class).getBoolean(0));
    }

    public void testNotPushedInFinalMode() {
        var agg = aggregateExec(AggregatorMode.FINAL, externalSource(statsMetadata(500L, null, null)), countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    // --- Output schema consistency (regression for VerificationException) ---

    public void testInitialModeOutputMatchesAggregateExecOutput() {
        var agg = aggregateExec(AggregatorMode.INITIAL, externalSource(statsMetadata(1000L, null, null)), countStarAlias());
        List<Attribute> expectedOutput = agg.output();

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals("LocalSourceExec output must match AggregateExec.output() for INITIAL mode", expectedOutput, local.output());
        assertEquals("Block count must match output attribute count", local.output().size(), local.supplier().get().getBlockCount());
    }

    public void testInitialModeMultiAggOutputMatchesAggregateExecOutput() {
        Map<String, Object> metadata = statsMetadata(500L, "score", 10L);
        metadata.put("_stats.columns.score.min", 1.0);
        metadata.put("_stats.columns.score.max", 100.0);

        var agg = aggregateExec(
            AggregatorMode.INITIAL,
            externalSource(metadata),
            countStarAlias(),
            alias("mn", new Min(Source.EMPTY, SCORE)),
            alias("mx", new Max(Source.EMPTY, SCORE))
        );
        List<Attribute> expectedOutput = agg.output();

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(
            "LocalSourceExec output must match AggregateExec.output() for INITIAL mode with multiple aggs",
            expectedOutput,
            local.output()
        );
        assertEquals(
            "Block count must match output attribute count for multiple aggs",
            local.output().size(),
            local.supplier().get().getBlockCount()
        );
    }

    public void testSingleModeOutputMatchesAggregateExecOutput() {
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(statsMetadata(1000L, null, null)), countStarAlias());
        List<Attribute> expectedOutput = agg.output();

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals("LocalSourceExec output must match AggregateExec.output() for SINGLE mode", expectedOutput, local.output());
        assertEquals("Block count must match output attribute count", local.output().size(), local.supplier().get().getBlockCount());
    }

    // --- Not-pushed cases ---

    public void testNotPushedWithGroupings() {
        ExternalSourceExec ext = externalSource(statsMetadata(1000L, null, null));
        ReferenceAttribute groupField = referenceAttribute("dept", DataType.KEYWORD);
        var agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(groupField),
            List.of(countStarAlias()),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithoutStats() {
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(Map.of()), countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithUnsupportedFormat() {
        ExternalSourceExec ext = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.csv",
            "csv",
            defaultAttrs(),
            Map.of(),
            statsMetadata(1000L, null, null),
            null
        );
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithUnsupportedAggregation() {
        var agg = aggregateExec(
            AggregatorMode.SINGLE,
            externalSource(statsMetadata(1000L, "age", 0L)),
            alias("s", new Sum(Source.EMPTY, AGE))
        );

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWhenChildIsNotExternalSourceExec() {
        ExternalSourceExec ext = externalSource(statsMetadata(1000L, null, null));
        Expression filterCondition = greaterThanOf(AGE, of(20L));
        FilterExec filter = new FilterExec(Source.EMPTY, ext, filterCondition);
        var agg = aggregateExec(AggregatorMode.SINGLE, filter, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithNullFormatReaderRegistry() {
        ExternalSourceExec ext = externalSource(statsMetadata(1000L, null, null));
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        PhysicalPlan result = new PushStatsToExternalSource().apply(agg, nullRegistryContext());
        as(result, AggregateExec.class);
    }

    public void testFormatGateRespected() {
        // ITEM 2 gate: the rule consults the format's declared aggregate pushability BEFORE touching stats, so it
        // cannot fold where the format declares the aggregate unpushable (symmetric with ComputeService's
        // split-discovery skip-gate). A support that declares NO must make the rule bail even though the stats
        // could serve COUNT(*). Fails without the canPushAggregates gate in PushStatsToExternalSource.rule.
        AggregatePushdownSupport rejectAll = (aggregates, groupings) -> AggregatePushdownSupport.Pushability.NO;
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        registry.registerLazy("parquet", (settings, blockFactory) -> new StubFormatReader(rejectAll), null, null);

        // Stats that WOULD serve COUNT(*) if the gate let the rule proceed.
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(statsMetadata(1000L, null, null)), countStarAlias());
        as(new PushStatsToExternalSource().apply(agg, buildContext(registry)), AggregateExec.class);
    }

    public void testCountFieldWithoutColumnEntriesPushedAsImplicitNullCount() {
        // The merged metadata map carries a row count but no _stats.columns.age.* entries.
        // Under the SplitStats SPI's "implicit nulls" contract, the column is treated as
        // absent from this scope, so columnNullCount("age") == rowCount and COUNT(age) == 0.
        // The rule pushes down a LocalSourceExec with value 0 rather than bailing out.
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), countFieldAlias(AGE));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(0L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testMinWithoutColumnStatsNotPushed() {
        var agg = aggregateExec(
            AggregatorMode.SINGLE,
            externalSource(statsMetadata(100L, null, null)),
            alias("m", new Min(Source.EMPTY, AGE))
        );

        as(applyRule(agg), AggregateExec.class);
    }

    /**
     * Partial-stats safe-miss: a summary that harvested only one column ("age" has min/max) must still
     * fall back to a scan for a MIN/MAX of an UN-harvested column ("score"), never serve null/wrong — while
     * COUNT(*) over the same summary still serves from the row count. This is the cross-column interaction
     * the harvest-scope work must preserve (e.g. PROJECTED-harvest of a different column, or COUNT-scope).
     */
    public void testMinOfUnharvestedColumnSafeMissesWhileCountStarServes() {
        // age is harvested (present with min/max); score has NO stats entries at all.
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L);
        metadata.put("_stats.columns.age.min", 18);
        metadata.put("_stats.columns.age.max", 99);

        // MIN(score): score was never harvested -> must NOT push down; falls back to a scan.
        var minAgg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("m", new Min(Source.EMPTY, SCORE)));
        as(applyRule(minAgg), AggregateExec.class);

        // MAX(score): same.
        var maxAgg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("m", new Max(Source.EMPTY, SCORE)));
        as(applyRule(maxAgg), AggregateExec.class);

        // COUNT(*): independent of column stats -> still serves from the row count.
        var countStar = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), countStarAlias());
        LocalSourceExec local = as(applyRule(countStar), LocalSourceExec.class);
        assertEquals(100L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));

        // MIN(age) over the SAME summary DOES serve (the harvested column), proving the safe-miss is
        // column-specific, not a blanket refusal.
        var minAge = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("m", new Min(Source.EMPTY, AGE)));
        as(applyRule(minAge), LocalSourceExec.class);
    }

    /**
     * gate/fold servability ALIGNMENT. The skip-discovery gate ({@code ComputeService.canSkipSplitDiscovery}) and the
     * fold rule previously diverged: the gate's type-only
     * {@code AggregatePushdownSupport.canPushAggregates} said {@code MIN(score)} was pushable while the fold
     * safe-missed on {@code score}'s unservable stats, leaving a zero-split scan that crashed under
     * union_by_name. The gate now consults the SAME servability decision the fold uses —
     * {@code ExternalSourceAggregatePushdown.canServeAllFromStats} (the boolean twin of the fold's resolution).
     * On the identical (aggregate, stats) the type check still says YES but the shared servability probe
     * DECLINES, so the gate and fold cannot disagree again. The end-to-end recurrence proof lives in
     * {@code ExternalSourceProfileIT.testCsvUnionByNameWarmMinUnservableColumnServesInsteadOfCrashing}.
     */
    public void testGateServabilityProbeAgreesWithFoldSafeMiss() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L);
        metadata.put("_stats.columns.age.min", 18);
        metadata.put("_stats.columns.age.max", 99);
        // score: complete file-level row count present, but NO servable per-column min.

        AggregatePushdownSupport support = new TextAggregatePushdownSupport();
        // The type-only criterion is unchanged (and still insufficient on its own): MIN(score) is pushable.
        assertEquals(AggregatePushdownSupport.Pushability.YES, support.canPushAggregates(List.of(new Min(Source.EMPTY, SCORE)), List.of()));
        // The shared servability probe the gate NOW additionally consults declines the unservable column —
        // the same decision the fold makes, so "gate skips" can no longer outrun "fold serves".
        assertFalse(
            "gate's servability probe must decline the unservable column, matching the fold safe-miss",
            ExternalSourceAggregatePushdown.canServeAllFromStats(
                List.of(alias("m", new Min(Source.EMPTY, SCORE))),
                SplitStats.of(metadata),
                support.appliesImplicitNullsForAbsentColumn(),
                Set.of()
            )
        );

        // FOLD rule: still safe-misses (aggregate not served) — the two now agree.
        var minAgg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("m", new Min(Source.EMPTY, SCORE)));
        as(applyRule(minAgg), AggregateExec.class);
    }

    public void testFilteredAggregateDeclinesStatServing() {
        // G1 (elastic/elasticsearch#150920): a PER-AGGREGATE filter (e.g. COUNT(*) WHERE p) cannot be served from
        // whole-file/split stats — resolveCount/Min/Max bail on hasFilter(). TextAggregatePushdownSupport's type
        // gate does NOT inspect filters, so a regression dropping this bail would serve the UNFILTERED count. The
        // servability probe must decline it (safe-miss -> re-scan).
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L);
        metadata.put("_stats.columns.age.min", 18);
        metadata.put("_stats.columns.age.max", 99);
        SplitStats stats = SplitStats.of(metadata);
        boolean implicitNulls = new TextAggregatePushdownSupport().appliesImplicitNullsForAbsentColumn();

        // Unfiltered COUNT(*) IS servable.
        assertTrue(ExternalSourceAggregatePushdown.canServeAllFromStats(List.of(countStarAlias()), stats, implicitNulls, Set.of()));
        // The SAME COUNT with a per-aggregate filter must decline.
        var filteredCount = new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")).withFilter(greaterThanOf(AGE, of(18L)));
        assertFalse(
            "COUNT(*) WHERE p must decline stat-serving -> safe-miss",
            ExternalSourceAggregatePushdown.canServeAllFromStats(List.of(alias("c", filteredCount)), stats, implicitNulls, Set.of())
        );
        // Same for a filtered MIN.
        var filteredMin = new Min(Source.EMPTY, AGE).withFilter(greaterThanOf(AGE, of(18L)));
        assertFalse(
            "MIN(age) WHERE p must decline stat-serving -> safe-miss",
            ExternalSourceAggregatePushdown.canServeAllFromStats(List.of(alias("m", filteredMin)), stats, implicitNulls, Set.of())
        );
    }

    public void testCountOfPartitionColumnSafeMisses() {
        // ITEM 3 (H4, elastic/elasticsearch#150920): a Hive partition column lives in the directory path, absent
        // from every file's column stats, so columnNullCount returns rowCount -> COUNT(p) would serve
        // rowCount - rowCount = 0. When p is flagged path-derived the resolution safe-misses (the scan's
        // VirtualColumnIterator answers correctly). COUNT(*) still serves on the same partitioned source.
        Map<String, Object> metadata = statsMetadata(100L, null, null); // rowCount 100, no column stats for p
        SplitStats stats = SplitStats.of(metadata);
        // Footer format (parquet/ORC): an absent column reads as all-null (implicitNulls == true) — this is the
        // path where COUNT(p) lies. Text (implicitNulls == false) already safe-misses an unharvested column.
        boolean implicitNulls = true;
        ReferenceAttribute p = referenceAttribute("p", DataType.KEYWORD);
        Count countP = new Count(Source.EMPTY, p);

        // The bug without the guard: COUNT(p) serves rowCount - rowCount = 0.
        assertEquals(0L, ((Number) ExternalSourceAggregatePushdown.resolveFromStats(countP, stats, implicitNulls, Set.of())).longValue());
        // The fix: p flagged path-derived -> safe-miss.
        assertNull(
            "COUNT(partition_col) must safe-miss on a footer format",
            ExternalSourceAggregatePushdown.resolveFromStats(countP, stats, implicitNulls, Set.of("p"))
        );
        // COUNT(*) still serves on the partitioned source.
        Count countStar = new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*"));
        assertEquals(
            100L,
            ((Number) ExternalSourceAggregatePushdown.resolveFromStats(countStar, stats, implicitNulls, Set.of("p"))).longValue()
        );
        // MIN(p) is guarded belt-and-suspenders (already safe via a null columnMin).
        assertNull(ExternalSourceAggregatePushdown.resolveFromStats(new Min(Source.EMPTY, p), stats, implicitNulls, Set.of("p")));
        // The gate twin declines COUNT(p) too (symmetry -> no zero-split crash).
        assertFalse(
            "gate must decline COUNT(partition_col), matching the fold",
            ExternalSourceAggregatePushdown.canServeAllFromStats(List.of(alias("c", countP)), stats, implicitNulls, Set.of("p"))
        );
    }

    public void testPartitionColumnNamesFromSourceMetadata() {
        // ITEM 3 wiring: the partition set the fold (PushStatsToExternalSource) and the split-discovery gate
        // (ComputeService) feed to resolveFromStats is read from the SERIALIZED sourceMetadata, stamped at
        // resolution under PARTITION_COLUMNS_KEY. This is what makes the guard survive to the DATA-NODE fold,
        // where the coordinator-only FileList is UNRESOLVED. A null/absent/empty key yields the empty set (an
        // unpartitioned source is unguarded, so its physical columns serve normally).
        assertEquals(Set.of(), SourceStatisticsSerializer.partitionColumnNames(null));
        assertEquals(Set.of(), SourceStatisticsSerializer.partitionColumnNames(Map.of()));
        assertEquals(
            Set.of("p"),
            SourceStatisticsSerializer.partitionColumnNames(Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of("p")))
        );
        assertEquals(
            Set.of("year", "month"),
            SourceStatisticsSerializer.partitionColumnNames(
                Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of("year", "month"))
            )
        );
    }

    /**
     * COUNT(col) WRONG-DATA safe-miss for a text format under partial harvest. A CSV/NDJSON file can have
     * a complete whole-file row count (count-scope or projected-scope of a different column) but NO
     * per-column key for a column that is physically present yet was never harvested. The implicit-nulls
     * contract ({@code columnNullCount == rowCount} for an absent key) is a FOOTER-format property; for a
     * text format the absent key means "not harvested," so {@code COUNT(col)} must NOT serve
     * {@code rowCount - rowCount = 0} — it must safe-miss and re-scan.
     * <p>
     * The companion {@link #testCountFieldWithoutColumnEntriesPushedAsImplicitNullCount} proves the FOOTER
     * contract is preserved (the same shape over a footer-format source still serves 0).
     */
    public void testCountFieldOfUnharvestedColumnSafeMissesForTextFormat() {
        // Complete whole-file stats: 1000 rows, but no _stats.columns.age.* (age present, not harvested).
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), countFieldAlias(AGE));

        // Text format: implicit nulls do not apply to an absent (unharvested) column -> safe-miss.
        as(applyRuleText(agg), AggregateExec.class);

        // A harvested column with a genuine null count over the same text source STILL serves: the safe-miss
        // is column-specific, not a blanket refusal for text formats.
        Map<String, Object> harvested = statsMetadata(1000L, "age", 50L);
        var aggHarvested = aggregateExec(AggregatorMode.SINGLE, externalSource(harvested), countFieldAlias(AGE));
        LocalSourceExec local = as(applyRuleText(aggHarvested), LocalSourceExec.class);
        assertEquals(950L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));

        // COUNT(*) over the unharvested-column source is independent of column stats -> still serves.
        var countStar = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), countStarAlias());
        LocalSourceExec countLocal = as(applyRuleText(countStar), LocalSourceExec.class);
        assertEquals(1000L, as(countLocal.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    /**
     * The payoff of ALL scope: a cold scan that never projected "score" still committed its min/max under ALL,
     * so a later warm {@code MIN(score)} / {@code MAX(score)} short-circuits to a LocalSourceExec instead of
     * re-scanning. This is the optimizer-layer view of the ALL capability — the harvest scope decides what
     * lands in this metadata map; here we assert that once a non-query-projected column's stats ARE present
     * (which only ALL produces), the pushdown fires. The companion partial-stats safe-miss test
     * ({@link #testMinOfUnharvestedColumnSafeMissesWhileCountStarServes}) proves the converse stays correct.
     */
    public void testMinMaxOfNonProjectedColumnServesWarmUnderAllScopeHarvest() {
        // Metadata as ALL would commit it: row count + min/max for "score" even though the cold query that
        // produced this summary need not have projected score (ALL harvests every file column).
        Map<String, Object> metadata = statsMetadata(500L, "score", 0L);
        metadata.put("_stats.columns.score.min", 1.0);
        metadata.put("_stats.columns.score.max", 100.0);

        var minAgg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("mn", new Min(Source.EMPTY, SCORE)));
        LocalSourceExec minLocal = as(applyRule(minAgg), LocalSourceExec.class);
        assertEquals(1.0, as(minLocal.supplier().get().getBlock(0), DoubleBlock.class).getDouble(0), 0.0);

        var maxAgg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), alias("mx", new Max(Source.EMPTY, SCORE)));
        LocalSourceExec maxLocal = as(applyRule(maxAgg), LocalSourceExec.class);
        assertEquals(100.0, as(maxLocal.supplier().get().getBlock(0), DoubleBlock.class).getDouble(0), 0.0);
    }

    public void testMaxWithoutColumnStatsNotPushed() {
        var agg = aggregateExec(
            AggregatorMode.SINGLE,
            externalSource(statsMetadata(100L, null, null)),
            alias("m", new Max(Source.EMPTY, AGE))
        );

        as(applyRule(agg), AggregateExec.class);
    }

    public void testMultipleAggsPushedDown() {
        Map<String, Object> metadata = statsMetadata(500L, "score", 10L);
        metadata.put("_stats.columns.score.min", 1.0);
        metadata.put("_stats.columns.score.max", 100.0);

        var agg = aggregateExec(
            AggregatorMode.SINGLE,
            externalSource(metadata),
            countStarAlias(),
            alias("mn", new Min(Source.EMPTY, SCORE)),
            alias("mx", new Max(Source.EMPTY, SCORE))
        );

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(3, local.output().size());
    }

    public void testCountStarNotPushedWithPartialStats() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null);
        metadata.put(SourceStatisticsSerializer.STATS_PARTIAL, Boolean.TRUE);
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithMixedSplitStats() {
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), statsMetadata(100L, null, null), null);
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    // --- Multi-split tests ---

    public void testPushedWithMultipleSplitsWithStats() {
        ExternalSourceExec ext = externalSourceWithSplits(
            Map.of(),
            statsMetadata(100L, null, null),
            statsMetadata(200L, null, null),
            statsMetadata(300L, null, null)
        );
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(600L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testPushedWithMultipleSplitsInInitialMode() {
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), statsMetadata(100L, null, null), statsMetadata(200L, null, null));
        var agg = aggregateExec(AggregatorMode.INITIAL, ext, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(2, page.getBlockCount());
        assertEquals(300L, as(page.getBlock(0), LongBlock.class).getLong(0));
        assertTrue(as(page.getBlock(1), BooleanBlock.class).getBoolean(0));
    }

    public void testPushedWithTenSplitsWholeFileStats() {
        @SuppressWarnings("unchecked")
        Map<String, Object>[] perSplitStats = (Map<String, Object>[]) new Map<?, ?>[10];
        for (int i = 0; i < 10; i++) {
            perSplitStats[i] = statsMetadata(1000L, null, null);
        }
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), perSplitStats);
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(10_000L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testMultiSplitMinMaxMergedCorrectly() {
        Map<String, Object> split1Stats = statsMetadata(500L, "age", 10L);
        split1Stats.put("_stats.columns.age.min", 18);
        split1Stats.put("_stats.columns.age.max", 65);

        Map<String, Object> split2Stats = statsMetadata(300L, "age", 5L);
        split2Stats.put("_stats.columns.age.min", 22);
        split2Stats.put("_stats.columns.age.max", 90);

        Map<String, Object> split3Stats = statsMetadata(200L, "age", 0L);
        split3Stats.put("_stats.columns.age.min", 15);
        split3Stats.put("_stats.columns.age.max", 70);

        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), split1Stats, split2Stats, split3Stats);

        var countAgg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());
        LocalSourceExec countLocal = as(applyRule(countAgg), LocalSourceExec.class);
        assertEquals(1000L, as(countLocal.supplier().get().getBlock(0), LongBlock.class).getLong(0));

        ExternalSourceExec ext2 = externalSourceWithSplits(Map.of(), split1Stats, split2Stats, split3Stats);
        var minAgg = aggregateExec(AggregatorMode.SINGLE, ext2, alias("m", new Min(Source.EMPTY, AGE)));
        LocalSourceExec minLocal = as(applyRule(minAgg), LocalSourceExec.class);
        assertEquals(15, as(minLocal.supplier().get().getBlock(0), IntBlock.class).getInt(0));

        ExternalSourceExec ext3 = externalSourceWithSplits(Map.of(), split1Stats, split2Stats, split3Stats);
        var maxAgg = aggregateExec(AggregatorMode.SINGLE, ext3, alias("m", new Max(Source.EMPTY, AGE)));
        LocalSourceExec maxLocal = as(applyRule(maxAgg), LocalSourceExec.class);
        assertEquals(90, as(maxLocal.supplier().get().getBlock(0), IntBlock.class).getInt(0));
    }

    public void testMultiSplitCountFieldSubtractsNullsAcrossFiles() {
        Map<String, Object> split1Stats = statsMetadata(1000L, "score", 50L);
        Map<String, Object> split2Stats = statsMetadata(2000L, "score", 100L);

        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), split1Stats, split2Stats);
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countFieldAlias(SCORE));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(2850L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testMultiSplitMinMaxDoublesMergedCorrectly() {
        Map<String, Object> split1Stats = statsMetadata(100L, "score", 0L);
        split1Stats.put("_stats.columns.score.min", 1.5);
        split1Stats.put("_stats.columns.score.max", 99.0);

        Map<String, Object> split2Stats = statsMetadata(200L, "score", 0L);
        split2Stats.put("_stats.columns.score.min", 0.5);
        split2Stats.put("_stats.columns.score.max", 100.5);

        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), split1Stats, split2Stats);
        var minAgg = aggregateExec(AggregatorMode.SINGLE, ext, alias("m", new Min(Source.EMPTY, SCORE)));
        LocalSourceExec minLocal = as(applyRule(minAgg), LocalSourceExec.class);
        assertEquals(0.5, as(minLocal.supplier().get().getBlock(0), DoubleBlock.class).getDouble(0), 0.0001);

        ExternalSourceExec ext2 = externalSourceWithSplits(Map.of(), split1Stats, split2Stats);
        var maxAgg = aggregateExec(AggregatorMode.SINGLE, ext2, alias("m", new Max(Source.EMPTY, SCORE)));
        LocalSourceExec maxLocal = as(applyRule(maxAgg), LocalSourceExec.class);
        assertEquals(100.5, as(maxLocal.supplier().get().getBlock(0), DoubleBlock.class).getDouble(0), 0.0001);
    }

    public void testCountOverPartiallyPresentColumnUsesImplicitNulls() {
        // Metadata-fast-path scenario (single sourceMetadata map, no per-split stats).
        // The map already encodes the merged null_count under the new contract:
        // 600 implicit nulls (rows from files lacking the column) + 10 explicit nulls = 610.
        // Pushdown computes Count(bonus) = rowCount - columnNullCount = 1000 - 610 = 390.
        // Pre-fix the merger would have produced null_count=10 and the result would be 990.
        ReferenceAttribute bonus = referenceAttribute("bonus", DataType.INTEGER);
        Map<String, Object> metadata = statsMetadata(1000L, "bonus", 610L);
        ExternalSourceExec ext = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            List.of(referenceAttribute("x", DataType.INTEGER), AGE, SCORE, bonus),
            Map.of(),
            metadata,
            null
        );
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countFieldAlias(bonus));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(390L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testPushdownAcrossSplitsWithMissingColumnInOneSplit() {
        // Per-split path: split 1 has full bonus stats (100 rows, 5 nulls, min=1, max=99);
        // split 2 has 200 rows but no bonus column at all.
        // Under the implicit-nulls contract:
        // COUNT(bonus) = (100 + 200) - (5 + 200) = 95 (split 2 contributes 200 implicit nulls).
        // MIN(bonus) = 1 (split 2 has no candidate, gets skipped rather than poisoning).
        // MAX(bonus) = 99 (same logic).
        ReferenceAttribute bonus = referenceAttribute("bonus", DataType.INTEGER);
        Map<String, Object> withBonus = new HashMap<>();
        withBonus.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        withBonus.put(SourceStatisticsSerializer.columnNullCountKey("bonus"), 5L);
        withBonus.put(SourceStatisticsSerializer.columnMinKey("bonus"), 1);
        withBonus.put(SourceStatisticsSerializer.columnMaxKey("bonus"), 99);
        withBonus.put(SourceStatisticsSerializer.columnSizeBytesKey("bonus"), 800L);

        Map<String, Object> withoutBonus = new HashMap<>();
        withoutBonus.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);

        // ExternalSourceExec with custom output that includes bonus in its attributes.
        List<Attribute> attrs = List.of(referenceAttribute("x", DataType.INTEGER), AGE, SCORE, bonus);

        ExternalSourceExec extCount = externalSourceWithSplitsAndAttrs(attrs, Map.of(), withBonus, withoutBonus);
        var countAgg = aggregateExec(AggregatorMode.SINGLE, extCount, countFieldAlias(bonus));
        LocalSourceExec countLocal = as(applyRule(countAgg), LocalSourceExec.class);
        assertEquals(95L, as(countLocal.supplier().get().getBlock(0), LongBlock.class).getLong(0));

        ExternalSourceExec extMin = externalSourceWithSplitsAndAttrs(attrs, Map.of(), withBonus, withoutBonus);
        var minAgg = aggregateExec(AggregatorMode.SINGLE, extMin, alias("mn", new Min(Source.EMPTY, bonus)));
        LocalSourceExec minLocal = as(applyRule(minAgg), LocalSourceExec.class);
        assertEquals(1, as(minLocal.supplier().get().getBlock(0), IntBlock.class).getInt(0));

        ExternalSourceExec extMax = externalSourceWithSplitsAndAttrs(attrs, Map.of(), withBonus, withoutBonus);
        var maxAgg = aggregateExec(AggregatorMode.SINGLE, extMax, alias("mx", new Max(Source.EMPTY, bonus)));
        LocalSourceExec maxLocal = as(applyRule(maxAgg), LocalSourceExec.class);
        assertEquals(99, as(maxLocal.supplier().get().getBlock(0), IntBlock.class).getInt(0));
    }

    /**
     * Cross-file-merge WRONG-DATA safe-miss for MIN/MAX under a text format (Julian's repro). One file
     * (a.csv) was warmed for the "value" column with stats [1000000, 1020000); a sibling (b.csv) was
     * warmed count-only and never harvested "value", so its split stats carry no "value" entry. For a
     * footer format an absent column is genuinely all-null and would be skipped, but for a text format
     * b.csv's "value" is physically present yet unobserved — its true range [0, 20000) is invisible.
     * Serving the merged extremum from a.csv alone would return MIN=1000000 (wrong; true MIN is 0). The
     * rule must safe-miss instead, exactly as it does for COUNT(col) over an unharvested column.
     * <p>
     * The companion footer-format test ({@link #testPushdownAcrossSplitsWithMissingColumnInOneSplit})
     * proves the implicit-nulls skip is preserved where it is sound.
     */
    public void testMultiSplitMinMaxOfUnharvestedColumnSafeMissesForTextFormat() {
        ReferenceAttribute value = referenceAttribute("value", DataType.INTEGER);
        List<Attribute> attrs = List.of(referenceAttribute("x", DataType.INTEGER), AGE, SCORE, value);

        // a.csv: "value" harvested, range [1000000, 1020000).
        Map<String, Object> withValue = new HashMap<>();
        withValue.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        withValue.put(SourceStatisticsSerializer.columnNullCountKey("value"), 0L);
        withValue.put(SourceStatisticsSerializer.columnMinKey("value"), 1000000);
        withValue.put(SourceStatisticsSerializer.columnMaxKey("value"), 1019999);

        // b.csv: count-only warm-up; "value" physically present but never harvested (no per-column key).
        Map<String, Object> countOnly = new HashMap<>();
        countOnly.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);

        // Text format: an unobserved column in a contributing split means "not harvested," so the merged
        // MIN/MAX is unknowable -> safe-miss (fall back to a scan), never serve the a.csv-only extremum.
        ExternalSourceExec extMin = externalSourceWithSplitsAndAttrs(attrs, Map.of(), withValue, countOnly);
        var minAgg = aggregateExec(AggregatorMode.SINGLE, extMin, alias("mn", new Min(Source.EMPTY, value)));
        as(applyRuleText(minAgg), AggregateExec.class);

        ExternalSourceExec extMax = externalSourceWithSplitsAndAttrs(attrs, Map.of(), withValue, countOnly);
        var maxAgg = aggregateExec(AggregatorMode.SINGLE, extMax, alias("mx", new Max(Source.EMPTY, value)));
        as(applyRuleText(maxAgg), AggregateExec.class);

        // Sanity: when BOTH files harvested "value", the merge is sound and the warm MIN/MAX serves. This
        // proves the safe-miss is triggered by the unharvested sibling, not a blanket refusal for text.
        Map<String, Object> withValue2 = new HashMap<>();
        withValue2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        withValue2.put(SourceStatisticsSerializer.columnNullCountKey("value"), 0L);
        withValue2.put(SourceStatisticsSerializer.columnMinKey("value"), 0);
        withValue2.put(SourceStatisticsSerializer.columnMaxKey("value"), 19999);

        ExternalSourceExec extBoth = externalSourceWithSplitsAndAttrs(attrs, Map.of(), withValue, withValue2);
        var minBoth = aggregateExec(AggregatorMode.SINGLE, extBoth, alias("mn", new Min(Source.EMPTY, value)));
        LocalSourceExec minLocal = as(applyRuleText(minBoth), LocalSourceExec.class);
        assertEquals(0, as(minLocal.supplier().get().getBlock(0), IntBlock.class).getInt(0));
    }

    public void testCountFieldNotPushedWhenMergedNullCountPoisoned() {
        // Defensive end-to-end check for the present-but-stats-less poison path:
        // mergeStatistics drops null_count for `bonus` when any present file lacks it, so the
        // sourceMetadata reaching the optimizer has no _stats.columns.bonus.null_count entry,
        // even though _stats.columns.bonus.size_bytes is set (column is physically present
        // somewhere). columnNullCount must return -1 and the rule must bail out.
        ReferenceAttribute bonus = referenceAttribute("bonus", DataType.INTEGER);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        metadata.put(SourceStatisticsSerializer.columnSizeBytesKey("bonus"), 4000L);
        // No _stats.columns.bonus.null_count -> SplitStats.of treats nullCount as -1 (unknown).
        ExternalSourceExec ext = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            List.of(referenceAttribute("x", DataType.INTEGER), AGE, SCORE, bonus),
            Map.of(),
            metadata,
            null
        );
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countFieldAlias(bonus));

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithMultipleSplitsWithoutStats() {
        ExternalSourceExec ext = externalSourceWithSplits(statsMetadata(1000L, null, null), (Map<String, Object>[]) null);
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWhenSourceHasPushedFilter() {
        var ext = externalSourceWithPushedFilter(statsMetadata(1000L, null, null), "some_pushed_filter");
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWhenSourceHasPushedFilterInInitialMode() {
        var ext = externalSourceWithPushedFilter(statsMetadata(1000L, null, null), "some_pushed_filter");
        var agg = aggregateExec(AggregatorMode.INITIAL, ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWhenSourceHasPushedFilterWithMinMax() {
        Map<String, Object> meta = statsMetadata(1000L, "age", 0L);
        meta.put("_stats.columns.age.min_value", 1);
        meta.put("_stats.columns.age.max_value", 99);
        var ext = externalSourceWithPushedFilter(meta, "like_filter");
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, alias("mn", new Min(Source.EMPTY, AGE)));

        as(applyRule(agg), AggregateExec.class);
    }

    public void testStillPushedWhenSourceHasNoPushedFilter() {
        var ext = externalSourceWithPushedFilter(statsMetadata(1000L, null, null), null);
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(1000L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    // --- helpers ---

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static ExternalSourceExec externalSourceWithSplitsAndAttrs(
        List<Attribute> attrs,
        Map<String, Object> sourceMetadata,
        Map<String, Object>... perSplitStats
    ) {
        List<ExternalSplit> splits = new ArrayList<>(perSplitStats.length);
        for (int i = 0; i < perSplitStats.length; i++) {
            splits.add(fileSplit(i, perSplitStats[i]));
        }
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", attrs, Map.of(), sourceMetadata, null, null)
            .withSplits(splits);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static ExternalSourceExec externalSourceWithSplits(Map<String, Object> sourceMetadata, Map<String, Object>... perSplitStats) {
        List<ExternalSplit> splits = new ArrayList<>(perSplitStats == null ? 1 : perSplitStats.length);
        if (perSplitStats == null) {
            splits.add(fileSplit(0, null));
            splits.add(fileSplit(1, null));
        } else {
            for (int i = 0; i < perSplitStats.length; i++) {
                splits.add(fileSplit(i, perSplitStats[i]));
            }
        }
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", defaultAttrs(), Map.of(), sourceMetadata, null, null)
            .withSplits(splits);
    }

    private static ExternalSourceExec externalSource(Map<String, Object> sourceMetadata) {
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", defaultAttrs(), Map.of(), sourceMetadata, null);
    }

    private static ExternalSourceExec externalSourceWithPushedFilter(Map<String, Object> sourceMetadata, Object pushedFilter) {
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            defaultAttrs(),
            Map.of(),
            sourceMetadata,
            pushedFilter,
            null
        );
    }

    private static List<Attribute> defaultAttrs() {
        return List.of(referenceAttribute("x", DataType.INTEGER), AGE, SCORE);
    }

    private static FileSplit fileSplit(int index, Map<String, Object> stats) {
        return new FileSplit(
            "parquet",
            StoragePath.of("file:///split" + (index + 1) + ".parquet"),
            0,
            100,
            "parquet",
            Map.of(),
            Map.of(),
            null,
            stats
        );
    }

    private static AggregateExec aggregateExec(AggregatorMode mode, PhysicalPlan child, NamedExpression... aggregates) {
        List<Attribute> intermediateAttrs = AbstractPhysicalOperationProviders.intermediateAttributes(List.of(aggregates), List.of());
        return new AggregateExec(Source.EMPTY, child, List.of(), List.of(aggregates), mode, intermediateAttrs, null);
    }

    private static Alias countStarAlias() {
        return alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
    }

    private static Alias countFieldAlias(ReferenceAttribute field) {
        return alias("c", new Count(Source.EMPTY, field));
    }

    private static Map<String, Object> statsMetadata(Long rowCount, String colName, Long nullCount) {
        Map<String, Object> metadata = new HashMap<>();
        if (rowCount != null) {
            metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        }
        if (colName != null && nullCount != null) {
            metadata.put("_stats.columns." + colName + ".null_count", nullCount);
        }
        return metadata;
    }

    private static FormatReaderRegistry buildParquetRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        AggregatePushdownSupport parquetSupport = (aggregates, groupings) -> {
            if (groupings.isEmpty() == false) {
                return AggregatePushdownSupport.Pushability.NO;
            }
            for (Expression agg : aggregates) {
                if (agg instanceof Count || agg instanceof Min || agg instanceof Max) {
                    continue;
                }
                return AggregatePushdownSupport.Pushability.NO;
            }
            return AggregatePushdownSupport.Pushability.YES;
        };
        registry.registerLazy("parquet", (settings, blockFactory) -> new StubFormatReader(parquetSupport), null, null);
        return registry;
    }

    /**
     * Registry whose "parquet"-named reader carries the real {@link TextAggregatePushdownSupport}, i.e. it
     * declares {@code appliesImplicitNullsForAbsentColumn() == false}. Used to exercise the text-format
     * partial-harvest safe-miss without standing up a CSV/NDJSON reader; the source-type name stays
     * "parquet" only because {@link #externalSource} hard-codes it — the support is what the rule reads.
     */
    private static FormatReaderRegistry buildTextRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        AggregatePushdownSupport textSupport = new TextAggregatePushdownSupport();
        registry.registerLazy("parquet", (settings, blockFactory) -> new StubFormatReader(textSupport), null, null);
        return registry;
    }

    private static LocalPhysicalOptimizerContext buildContext(FormatReaderRegistry registry) {
        return new LocalPhysicalOptimizerContext(null, null, null, null, null, new ExternalOptimizerContext(registry));
    }

    private static LocalPhysicalOptimizerContext nullRegistryContext() {
        return new LocalPhysicalOptimizerContext(null, null, null, null, null, ExternalOptimizerContext.NONE);
    }

    private static PhysicalPlan applyRule(AggregateExec agg) {
        return new PushStatsToExternalSource().apply(agg, buildContext(buildParquetRegistry()));
    }

    private static PhysicalPlan applyRuleText(AggregateExec agg) {
        return new PushStatsToExternalSource().apply(agg, buildContext(buildTextRegistry()));
    }

    /**
     * Minimal FormatReader stub that only provides aggregate pushdown support.
     */
    private static class StubFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AggregatePushdownSupport support;

        StubFormatReader(AggregatePushdownSupport support) {
            this.support = support;
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata metadata(
            org.elasticsearch.xpack.esql.datasources.spi.StorageObject object
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.compute.operator.CloseableIterator<org.elasticsearch.compute.data.Page> read(
            org.elasticsearch.xpack.esql.datasources.spi.StorageObject object,
            org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public AggregatePushdownSupport aggregatePushdownSupport() {
            return support;
        }

        @Override
        public void close() {}
    }
}
