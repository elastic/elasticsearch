/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;

public class AdaptiveStrategyTests extends ESTestCase {

    private final AdaptiveStrategy strategy = new AdaptiveStrategy();

    public void testSingleSplitReturnsCoordinator() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testEmptySplitsReturnsCoordinator() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            List.of(),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testAggregationWithMultipleSplitsDistributes() {
        PhysicalPlan source = createExternalSourceExec();
        PhysicalPlan planWithAgg = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithAgg,
            createSplits(4),
            createNodes(2),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(2, plan.nodeAssignments().size());
    }

    public void testLimitOnlyReturnsCoordinator() {
        PhysicalPlan source = createExternalSourceExec();
        Literal limitExpr = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        PhysicalPlan planWithLimit = new LimitExec(Source.EMPTY, source, limitExpr, null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithLimit,
            createSplits(5),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testManySplitsNoAggregationDistributes() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(10),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(3, plan.nodeAssignments().size());
    }

    public void testFewSplitsNoAggregationStaysLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(2),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testNoEligibleNodesReturnsLocal() {
        PhysicalPlan source = createExternalSourceExec();
        PhysicalPlan planWithAgg = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithAgg,
            createSplits(5),
            DiscoveryNodes.builder().build(),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testDistributionIsEvenRoundRobin() {
        PhysicalPlan source = createExternalSourceExec();
        PhysicalPlan planWithAgg = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithAgg,
            createSplits(6),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            assertEquals(2, assigned.size());
        }
    }

    private static ExternalSourceExec createExternalSourceExec() {
        return new ExternalSourceExec(Source.EMPTY, "s3://bucket/data/*.parquet", "parquet", List.of(), Map.of(), Map.of(), null);
    }

    private static List<ExternalSplit> createSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
            );
        }
        return splits;
    }

    private static DiscoveryNodes createNodes(int count) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < count; i++) {
            builder.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return builder.build();
    }
}
