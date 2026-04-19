/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BooleanBlock;
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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
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

    public void testCountFieldWithoutNullCountNotPushed() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        var agg = aggregateExec(AggregatorMode.SINGLE, externalSource(metadata), countFieldAlias(AGE));

        as(applyRule(agg), AggregateExec.class);
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

    public void testNotPushedWithMultipleSplitsWithoutStats() {
        ExternalSourceExec ext = externalSourceWithSplits(statsMetadata(1000L, null, null), (Map<String, Object>[]) null);
        var agg = aggregateExec(AggregatorMode.SINGLE, ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    // --- helpers ---

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
        return new LocalPhysicalOptimizerContext(null, null, null, null, null, registry);
    }

    private static LocalPhysicalOptimizerContext nullRegistryContext() {
        return new LocalPhysicalOptimizerContext(null, null, null, null, null, null);
    }

    private static PhysicalPlan applyRule(AggregateExec agg) {
        return new PushAggregatesToExternalSource().apply(agg, buildContext(buildParquetRegistry()));
    }

    /**
     * Minimal FormatReader stub that only provides aggregate pushdown support.
     */
    private static class StubFormatReader implements FormatReader {
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
