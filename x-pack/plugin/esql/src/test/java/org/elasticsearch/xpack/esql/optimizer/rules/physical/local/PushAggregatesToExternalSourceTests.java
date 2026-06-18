/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
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
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
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

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

public class PushAggregatesToExternalSourceTests extends ESTestCase {

    private static final ReferenceAttribute AGE = referenceAttribute("age", DataType.INTEGER);
    private static final ReferenceAttribute SCORE = referenceAttribute("score", DataType.DOUBLE);

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

        PhysicalPlan result = new PushAggregatesToExternalSource().apply(agg, nullRegistryContext());
        as(result, AggregateExec.class);
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
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            attrs,
            Map.of(),
            sourceMetadata,
            null,
            -1,
            null,
            null,
            splits
        );
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
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            defaultAttrs(),
            Map.of(),
            sourceMetadata,
            null,
            -1,
            null,
            null,
            splits
        );
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

    private static LocalPhysicalOptimizerContext buildContext(FormatReaderRegistry registry) {
        return new LocalPhysicalOptimizerContext(null, null, null, null, null, new ExternalOptimizerContext(registry));
    }

    private static LocalPhysicalOptimizerContext nullRegistryContext() {
        return new LocalPhysicalOptimizerContext(null, null, null, null, null, ExternalOptimizerContext.NONE);
    }

    private static PhysicalPlan applyRule(AggregateExec agg) {
        return new PushAggregatesToExternalSource().apply(agg, buildContext(buildParquetRegistry()));
    }

    /**
     * Minimal FormatReader stub that only provides aggregate pushdown support.
     */
    private static class StubFormatReader implements NoConfigFormatReader {

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
