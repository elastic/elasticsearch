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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;

public class RoundRobinStrategyTests extends ESTestCase {

    private final RoundRobinStrategy strategy = new RoundRobinStrategy();

    public void testEvenDistribution() {
        List<ExternalSplit> splits = createSplits(6);
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(3, plan.nodeAssignments().size());
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            assertEquals(2, assigned.size());
        }
    }

    public void testUnevenDistribution() {
        List<ExternalSplit> splits = createSplits(5);
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(3, plan.nodeAssignments().size());
        int total = 0;
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            total += assigned.size();
        }
        assertEquals(5, total);
    }

    public void testWrapsAround() {
        List<ExternalSplit> splits = createSplits(7);
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(2), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(2, plan.nodeAssignments().size());
        List<List<ExternalSplit>> values = List.copyOf(plan.nodeAssignments().values());
        assertEquals(4, values.get(0).size());
        assertEquals(3, values.get(1).size());
    }

    public void testSingleNodeGetsAllSplits() {
        List<ExternalSplit> splits = createSplits(5);
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(1), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(1, plan.nodeAssignments().size());
        assertEquals(5, plan.nodeAssignments().values().iterator().next().size());
    }

    public void testEmptySplitsReturnsLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), List.of(), createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testNoEligibleNodesReturnsLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createPlan(),
            createSplits(3),
            DiscoveryNodes.builder().build(),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    private static PhysicalPlan createPlan() {
        ExternalSourceExec source = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/*.parquet",
            "parquet",
            List.of(),
            Map.of(),
            Map.of(),
            null
        );
        return new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);
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
