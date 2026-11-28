/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.planner.TimeSeriesSplitPlanner.ESTIMATED_BYTES_PER_GROUP;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesSplitPlannerTests extends ESTestCase {

    public void testNumSplitsFitsGroupsIntoMemoryPerSplit() {
        long memory = 1000 * ESTIMATED_BYTES_PER_GROUP;
        assertThat(TimeSeriesSplitPlanner.numSplits(0, memory, 256), equalTo(0));
        assertThat(TimeSeriesSplitPlanner.numSplits(1000, memory, 256), equalTo(1));
        assertThat(TimeSeriesSplitPlanner.numSplits(1001, memory, 256), equalTo(2));
        assertThat(TimeSeriesSplitPlanner.numSplits(10_000, memory, 256), equalTo(10));
        assertThat(TimeSeriesSplitPlanner.numSplits(10_000, memory, 4), equalTo(4));
        assertThat(TimeSeriesSplitPlanner.numSplits(Long.MAX_VALUE, memory, 256), equalTo(256));
    }

    public void testEstimatedGroupsFromIndexStats() {
        long span = 1000;
        IndexProperties index = new IndexProperties(IndexMode.TIME_SERIES, 1, 0, new TimeSeriesStats(10 * span, 0, span - 1));
        long[] unbounded = { Long.MIN_VALUE, Long.MAX_VALUE };
        assertThat(TimeSeriesSplitPlanner.estimateGroupFromIndexStats(List.of(index, index), null, unbounded), equalTo(20L));
        assertThat(TimeSeriesSplitPlanner.estimateGroupFromIndexStats(List.of(index), null, new long[] { 0, span / 2 - 1 }), equalTo(10L));
        assertThat(TimeSeriesSplitPlanner.estimateGroupFromIndexStats(List.of(index), null, new long[] { span, 2 * span }), equalTo(0L));
        IndexProperties unknown = new IndexProperties(IndexMode.TIME_SERIES, 1, 0, null);
        assertThat(TimeSeriesSplitPlanner.estimateGroupFromIndexStats(List.of(index, unknown), null, unbounded), equalTo(-1L));
        Bucket bucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.EPOCH),
            Literal.timeDuration(Source.EMPTY, Duration.ofMillis(250)),
            null,
            null,
            null,
            TEST_CFG
        );
        assertThat(TimeSeriesSplitPlanner.estimateGroupFromIndexStats(List.of(index, index), bucket, unbounded), equalTo(100L));
        assertThat(
            TimeSeriesSplitPlanner.estimateGroupFromIndexStats(List.of(index), bucket, new long[] { 0, span / 2 - 1 }),
            equalTo(30L)
        );
    }

    public void testSplitAddsRangeToFragmentFilter() {
        TransportVersion version = TransportVersion.current();
        var generated = EsIndexGenerator.esIndex(
            "k8s",
            EsqlTestUtils.loadMapping("k8s-mappings.json"),
            Map.of("k8s-1", IndexMode.TIME_SERIES, "k8s-2", IndexMode.TIME_SERIES)
        );
        var index = new EsIndex(
            generated.name(),
            generated.mapping(),
            generated.indexProperties(),
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of("k8s")),
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of("k8s-1", "k8s-2"))
        );
        var analyzer = EsqlTestUtils.analyzer().addIndex(index).minimumTransportVersion(version);
        var optimizer = new LogicalPlanOptimizer(EsqlTestUtils.unboundLogicalOptimizerContext(version));
        var logical = optimizer.optimize(
            analyzer.query("TS k8s | STATS rate = SUM(RATE(network.total_bytes_in)) BY cluster, bucket = TBUCKET(1 hour)")
        );
        var physicalOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, version));
        var physical = physicalOptimizer.optimize(new Mapper().map(new Versioned<>(logical, version)));
        var requestFilter = QueryBuilders.termQuery("cluster", "prod");
        physical = PlannerUtils.integrateEsFilterIntoFragment(physical, requestFilter, version);

        var reduction = TimeSeriesSplitPlanner.findNodes(physical);
        assertNotNull(reduction);
        var ranges = List.of(new TsidPrefixRange(8, 0, 128), new TsidPrefixRange(8, 128, 256));
        var split = TimeSeriesSplitPlanner.split(physical, reduction, ranges);

        assertEquals(physical.output(), split.output());
        var merges = split.collect(MergeExec.class);
        assertEquals(1, merges.size());
        var merge = merges.getFirst();
        assertEquals(MergeExec.Kind.FORK, merge.kind());
        assertEquals(2, merge.children().size());
        assertEquals(1 + ranges.size(), split.collect(AggregateExec.class).size());

        var exchanges = merge.collect(ExchangeExec.class);
        assertEquals(2, exchanges.size());
        assertNotEquals(exchanges.get(0).exchange(), exchanges.get(1).exchange());
        for (int i = 0; i < ranges.size(); i++) {
            var fragment = asInstanceOf(FragmentExec.class, exchanges.get(i).child());
            assertSame(reduction.fragment().fragment(), fragment.fragment());
            var filter = asInstanceOf(BoolQueryBuilder.class, fragment.esFilter());
            assertEquals(List.of(requestFilter), filter.must());
            assertEquals(List.of(new TsidPrefixRangeQueryBuilder(ranges.get(i))), filter.filter());
        }

        var execution = asInstanceOf(SubPlan.Merge.class, PlannerUtils.buildSubPlan(split));
        assertEquals(2, execution.children().size());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
