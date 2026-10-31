/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.ColumnStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector.JoinStrategy;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CostBasedOptimizerTests extends ESTestCase {
    public void testSmallBuildPrefersMaterializedForLargeProbe() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(100, 1_024, 16));

        PlanDecisions decisions = optimize(join, statistics, 1_000_000_000L);

        assertEquals(JoinStrategy.MATERIALIZED, decisions.join(join).orElseThrow());
    }

    public void testLargeBuildUsesPartitionedJoin() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(20_000_000, 2_000_000_000d, 100));

        PlanDecisions decisions = optimize(join, statistics, 128L * 1024 * 1024);

        assertEquals(JoinStrategy.PARTITIONED, decisions.join(join).orElseThrow());
    }

    public void testDistributedAggregateCanFeedPartitionedJoin() {
        Attribute leftKey = attribute("group_key");
        Attribute rightKey = attribute("join_key");
        EsRelation input = relation("input", leftKey);
        Aggregate aggregate = new Aggregate(Source.EMPTY, input, List.of(leftKey), List.of(leftKey));
        SubqueryLookupJoin join = subqueryJoin(aggregate, relation("build", rightKey), leftKey, rightKey);

        PlanStats inputStats = stats(100_000_000, 6_400_000_000d, 64, leftKey, Estimate.exact(10_000_000));

        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(input, inputStats);
        statistics.put(aggregate, stats(10_000_000, 1_000_000_000d, 64));
        statistics.put(join.right(), stats(20_000_000, 2_000_000_000d, 100));

        PlanDecisions decisions = optimize(join, statistics, 128L * 1024 * 1024);

        assertEquals(JoinStrategy.PARTITIONED, decisions.join(join).orElseThrow());
        assertEquals(AggMode.DISTRIBUTED, decisions.aggregate(aggregate.aggregates()).orElseThrow());
    }

    public void testUnknownBuildSizeKeepsMaterializedIncumbent() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000, 8_000_000, 80));
        statistics.put(join.right(), stats(Estimate.UNKNOWN, Estimate.UNKNOWN, Estimate.UNKNOWN));

        PlanDecisions decisions = optimize(join, statistics, 128L * 1024 * 1024);

        assertEquals(JoinStrategy.MATERIALIZED, decisions.join(join).orElseThrow());
    }

    public void testUnknownGroupingNdvKeepsCoordinatorAggregate() {
        Attribute key = attribute("group_key");
        EsRelation input = relation("input", key);
        Aggregate aggregate = new Aggregate(Source.EMPTY, input, List.of(key), List.of(key));
        PlanStats inputStats = stats(100_000_000, 6_400_000_000d, 64, key, Estimate.UNKNOWN);

        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(input, inputStats);

        PlanDecisions decisions = optimize(aggregate, statistics, 128L * 1024 * 1024);

        assertEquals(AggMode.COORDINATOR, decisions.aggregate(aggregate.aggregates()).orElseThrow());
    }

    private static PlanDecisions optimize(LogicalPlan plan, Map<LogicalPlan, PlanStats> statistics, long materializeLimitBytes) {
        Configuration configuration = EsqlTestUtils.configuration(
            new QueryPragmas(
                Settings.builder().put(QueryPragmas.CBO.getKey(), true).put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2).build()
            )
        );
        CboContext context = new CboContext(configuration, TransportVersion.current(), 4, materializeLimitBytes);
        return new CostBasedOptimizer().optimizeWithStatistics(plan, context, statistics);
    }

    private static SubqueryLookupJoin subqueryJoin(LogicalPlan left, LogicalPlan right, Attribute leftKey, Attribute rightKey) {
        return new SubqueryLookupJoin(
            Source.EMPTY,
            left,
            right,
            JoinTypes.LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );
    }

    private static EsRelation relation(String name, Attribute key) {
        return new EsRelation(Source.EMPTY, name, IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(key));
    }

    private static Attribute attribute(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.LONG);
    }

    private static PlanStats stats(double rows, double bytes, double avgRowBytes) {
        return stats(Estimate.exact(rows), Estimate.exact(bytes), Estimate.exact(avgRowBytes));
    }

    private static PlanStats stats(Estimate rows, Estimate bytes, Estimate avgRowBytes) {
        return new PlanStats(rows, avgRowBytes, Map.of(), bytes);
    }

    private static PlanStats stats(double rows, double bytes, double avgRowBytes, Attribute attribute, Estimate ndv) {
        return new PlanStats(
            Estimate.exact(rows),
            Estimate.exact(avgRowBytes),
            Map.of(
                attribute.id(),
                new ColumnStats(ndv, Estimate.UNKNOWN, Optional.empty(), Optional.empty(), Estimate.UNKNOWN, Estimate.UNKNOWN)
            ),
            Estimate.exact(bytes)
        );
    }
}
