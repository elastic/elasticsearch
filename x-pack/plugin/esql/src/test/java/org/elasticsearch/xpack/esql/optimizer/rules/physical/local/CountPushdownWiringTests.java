/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitCoalescer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Wiring tests that verify COUNT(*) pushdown works end-to-end through
 * {@link PlannerUtils#localPlan} — the same code path used on data nodes.
 * <p>
 * Unlike {@link PushAggregatesToExternalSourceTests} which tests the optimizer rule
 * in isolation, these tests exercise the full chain: FragmentExec containing a logical
 * Aggregate → ExternalRelation is mapped, splits are injected, and the physical
 * optimizer runs. This catches regressions where splits lose their statistics during
 * the wiring between split discovery and the optimizer.
 */
public class CountPushdownWiringTests extends ESTestCase {

    private static final ReferenceAttribute AGE = referenceAttribute("age", DataType.INTEGER);

    /**
     * Core scenario: EXTERNAL "file" | STATS c = COUNT(*) with multiple row-group splits.
     * Each split carries per-row-group statistics. The optimizer should replace the
     * AggregateExec → ExternalSourceExec subtree with a LocalSourceExec.
     */
    public void testCountStarPushdownThroughLocalPlan() {
        List<ExternalSplit> splits = createSplitsWithStats(5, 1000L);
        PhysicalPlan result = runLocalPlanWithSplits(countStarAggregate(), splits);

        LocalSourceExec local = as(result, LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(5000L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    /**
     * Single-split scenario: one row group with stats.
     */
    public void testCountStarPushdownSingleSplit() {
        List<ExternalSplit> splits = createSplitsWithStats(1, 42_000L);
        PhysicalPlan result = runLocalPlanWithSplits(countStarAggregate(), splits);

        LocalSourceExec local = as(result, LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(42_000L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    /**
     * Regression guard: splits without stats must NOT push down. The optimizer
     * should preserve the AggregateExec → ExternalSourceExec subtree.
     */
    public void testCountStarNotPushedWithoutSplitStats() {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            splits.add(
                new FileSplit("file", StoragePath.of("file:///split" + i + ".parquet"), 0, 100, ".parquet", Map.of(), Map.of(), null)
            );
        }
        PhysicalPlan result = runLocalPlanWithSplits(countStarAggregate(), splits);

        as(result, AggregateExec.class);
    }

    /**
     * Regression test: CoalescedSplits wrapping FileSplits with stats must still push down.
     * This is the exact scenario that breaks with large S3 Parquet files (>32 row-group splits
     * trigger SplitCoalescer, which wraps FileSplits in CoalescedSplit).
     */
    public void testCountStarPushdownWithCoalescedSplits() {
        List<ExternalSplit> fileSplits = createSplitsWithStats(50, 200L);
        List<ExternalSplit> coalesced = SplitCoalescer.coalesce(fileSplits);
        assertFalse("coalescing should have fired for 50 splits", coalesced.equals(fileSplits));

        PhysicalPlan result = runLocalPlanWithSplits(countStarAggregate(), coalesced);

        LocalSourceExec local = as(result, LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(10_000L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    /**
     * Mixed splits: some with stats, some without. Must NOT push down.
     */
    public void testCountStarNotPushedWithMixedSplitStats() {
        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(fileSplitWithStats(0, 1000L));
        splits.add(new FileSplit("file", StoragePath.of("file:///noStats.parquet"), 0, 100, ".parquet", Map.of(), Map.of(), null));
        splits.add(fileSplitWithStats(2, 2000L));

        PhysicalPlan result = runLocalPlanWithSplits(countStarAggregate(), splits);
        as(result, AggregateExec.class);
    }

    /**
     * Empty splits list: pushdown should use sourceMetadata from the ExternalRelation
     * (the pre-fix behavior where the optimizer sees 0 splits).
     */
    public void testCountStarPushdownWithEmptySplitsUsesSourceMetadata() {
        PhysicalPlan result = runLocalPlanWithSplits(countStarAggregate(), List.of());

        LocalSourceExec local = as(result, LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(99_000L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    /**
     * INITIAL mode (data node): intermediate blocks should have (value, seen) pairs.
     */
    public void testCountStarInitialModeThroughLocalPlan() {
        List<ExternalSplit> splits = createSplitsWithStats(3, 500L);
        Aggregate logicalAgg = countStarAggregate();

        PhysicalPlan mapped = LocalMapper.INSTANCE.map(logicalAgg);
        AggregateExec aggExec = as(mapped, AggregateExec.class);
        assertEquals(AggregatorMode.INITIAL, aggExec.getMode());

        FormatReaderRegistry registry = buildParquetRegistry();
        PhysicalPlan result = runLocalPlanWithSplitsAndRegistry(logicalAgg, splits, registry);

        LocalSourceExec local = as(result, LocalSourceExec.class);
        Page page = local.supplier().get();
        assertEquals(2, page.getBlockCount());
        assertEquals(1500L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    // ---- helpers ----

    private PhysicalPlan runLocalPlanWithSplits(Aggregate logicalAgg, List<ExternalSplit> splits) {
        return runLocalPlanWithSplitsAndRegistry(logicalAgg, splits, buildParquetRegistry());
    }

    private PhysicalPlan runLocalPlanWithSplitsAndRegistry(
        Aggregate logicalAgg,
        List<ExternalSplit> splits,
        FormatReaderRegistry registry
    ) {
        FragmentExec fragment = new FragmentExec(logicalAgg);

        PhysicalPlan result = PlannerUtils.localPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            fragment,
            SearchStats.EMPTY,
            registry,
            splits,
            null
        );

        return result;
    }

    private static Aggregate countStarAggregate() {
        List<Attribute> attrs = List.of(referenceAttribute("x", DataType.INTEGER), AGE);
        Map<String, Object> sourceMetadata = new HashMap<>();
        sourceMetadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 99_000L);

        SourceMetadata metadata = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return attrs;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "file:///test.parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return sourceMetadata;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof SourceMetadata;
            }

            @Override
            public int hashCode() {
                return 1;
            }
        };

        ExternalRelation external = new ExternalRelation(Source.EMPTY, "file:///test.parquet", metadata, attrs);
        Alias countAlias = alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        return new Aggregate(Source.EMPTY, external, List.of(), List.of(countAlias));
    }

    private static List<ExternalSplit> createSplitsWithStats(int count, long rowsPerSplit) {
        List<ExternalSplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            splits.add(fileSplitWithStats(i, rowsPerSplit));
        }
        return splits;
    }

    private static FileSplit fileSplitWithStats(int index, long rowCount) {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        stats.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, rowCount * 100);
        return new FileSplit(
            "file",
            StoragePath.of("file:///split" + index + ".parquet"),
            index * 1024L,
            1024,
            ".parquet",
            Map.of(),
            Map.of(),
            null,
            stats
        );
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

    private static class StubFormatReader implements FormatReader {
        private final AggregatePushdownSupport support;

        StubFormatReader(AggregatePushdownSupport support) {
            this.support = support;
        }

        @Override
        public SourceMetadata metadata(org.elasticsearch.xpack.esql.datasources.spi.StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.compute.operator.CloseableIterator<Page> read(
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
