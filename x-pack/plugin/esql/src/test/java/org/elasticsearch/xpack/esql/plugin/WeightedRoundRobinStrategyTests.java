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

public class WeightedRoundRobinStrategyTests extends ESTestCase {

    private final WeightedRoundRobinStrategy strategy = new WeightedRoundRobinStrategy();

    public void testBalancedAssignmentBySize() {
        List<ExternalSplit> splits = List.of(
            createSplit("file1.parquet", 1000),
            createSplit("file2.parquet", 500),
            createSplit("file3.parquet", 300),
            createSplit("file4.parquet", 200)
        );
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(2), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(2, plan.nodeAssignments().size());

        long[] loads = new long[2];
        int idx = 0;
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            for (ExternalSplit s : assigned) {
                loads[idx] += s.estimatedSizeInBytes();
            }
            idx++;
        }
        long maxLoad = Math.max(loads[0], loads[1]);
        long minLoad = Math.min(loads[0], loads[1]);
        assertTrue("max load imbalance too high: " + maxLoad + " vs " + minLoad, maxLoad <= minLoad * 2);
    }

    public void testSingleLargeFilePlacedFirst() {
        List<ExternalSplit> splits = List.of(
            createSplit("small1.parquet", 100),
            createSplit("small2.parquet", 100),
            createSplit("large.parquet", 10000),
            createSplit("small3.parquet", 100)
        );
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        int totalAssigned = 0;
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            totalAssigned += assigned.size();
        }
        assertEquals(4, totalAssigned);
    }

    public void testAllEqualSizesDegradesToEvenDistribution() {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            splits.add(createSplit("file" + i + ".parquet", 1024));
        }
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            assertEquals(2, assigned.size());
        }
    }

    public void testNoSizeInfoFallsBackToRoundRobin() {
        List<ExternalSplit> splits = createSplitsWithoutSize(4);
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, createNodes(2), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        int totalAssigned = 0;
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            totalAssigned += assigned.size();
        }
        assertEquals(4, totalAssigned);
    }

    public void testEmptySplitsReturnsLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), List.of(), createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testNoNodesReturnsLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createPlan(),
            List.of(createSplit("file.parquet", 1024)),
            DiscoveryNodes.builder().build(),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testDeterministic() {
        List<ExternalSplit> splits = List.of(createSplit("a.parquet", 500), createSplit("b.parquet", 300), createSplit("c.parquet", 200));
        DiscoveryNodes nodes = createNodes(2);
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), splits, nodes, QueryPragmas.EMPTY);

        ExternalDistributionPlan plan1 = strategy.planDistribution(context);
        ExternalDistributionPlan plan2 = strategy.planDistribution(context);

        assertEquals(plan1.nodeAssignments(), plan2.nodeAssignments());
    }

    private static ExternalSplit createSplit(String name, long sizeBytes) {
        return new FileSplit("parquet", StoragePath.of("s3://bucket/" + name), 0, sizeBytes, ".parquet", Map.of(), Map.of());
    }

    private static List<ExternalSplit> createSplitsWithoutSize(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, -1, ".parquet", Map.of(), Map.of())
            );
        }
        return splits;
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

    private static DiscoveryNodes createNodes(int count) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < count; i++) {
            builder.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return builder.build();
    }
}
