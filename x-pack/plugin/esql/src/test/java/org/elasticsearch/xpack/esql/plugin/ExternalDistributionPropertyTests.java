/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;

/**
 * Randomized property tests for external source distribution strategies.
 * Validates mathematical invariants of split assignment: completeness,
 * bounded imbalance, determinism, and boundary conditions.
 */
public class ExternalDistributionPropertyTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testAllSplitsAssignedExactlyOnce() {
        int splitCount = between(1, 200);
        int nodeCount = between(1, 20);
        List<ExternalSplit> splits = createSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        assertTrue(plan.distributed());

        Set<ExternalSplit> assigned = new HashSet<>();
        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            for (ExternalSplit split : nodeSplits) {
                assertTrue("Split assigned more than once: " + split, assigned.add(split));
            }
            totalAssigned += nodeSplits.size();
        }
        assertEquals("All splits must be assigned", splitCount, totalAssigned);
        assertEquals("Every original split must appear", splitCount, assigned.size());
        for (ExternalSplit split : splits) {
            assertTrue("Original split missing from assignments: " + split, assigned.contains(split));
        }
    }

    public void testMaxLoadImbalance() {
        int splitCount = between(1, 200);
        int nodeCount = between(1, 20);
        List<ExternalSplit> splits = createSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        int maxAllowed = (int) Math.ceil((double) splitCount / nodeCount);
        for (Map.Entry<String, List<ExternalSplit>> entry : plan.nodeAssignments().entrySet()) {
            assertTrue(
                "Node " + entry.getKey() + " has " + entry.getValue().size() + " splits, max allowed is " + maxAllowed,
                entry.getValue().size() <= maxAllowed
            );
        }
    }

    public void testDeterministic() {
        int splitCount = between(2, 100);
        int nodeCount = between(1, 10);
        List<ExternalSplit> splits = createSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan1 = RoundRobinStrategy.assignRoundRobin(splits, nodes);
        ExternalDistributionPlan plan2 = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        assertEquals("Same inputs must produce same node set", plan1.nodeAssignments().keySet(), plan2.nodeAssignments().keySet());
        for (String nodeId : plan1.nodeAssignments().keySet()) {
            assertEquals(
                "Node " + nodeId + " must get same splits on both runs",
                plan1.nodeAssignments().get(nodeId),
                plan2.nodeAssignments().get(nodeId)
            );
        }
    }

    public void testSingleSplitAlwaysLocal() {
        int nodeCount = between(1, 10);
        List<ExternalSplit> splits = createSplits(1);
        DiscoveryNodes nodes = createNodes(nodeCount);

        for (String strategyName : List.of("adaptive", "coordinator_only")) {
            ExternalDistributionStrategy strategy = resolveStrategy(strategyName);
            ExternalDistributionContext context = new ExternalDistributionContext(createAggPlan(splits), splits, nodes, QueryPragmas.EMPTY);
            ExternalDistributionPlan plan = strategy.planDistribution(context);
            assertFalse("Strategy '" + strategyName + "' must return LOCAL for single split", plan.distributed());
        }
    }

    public void testEmptyNodesReturnsLocal() {
        int splitCount = between(1, 50);
        List<ExternalSplit> splits = createSplits(splitCount);
        DiscoveryNodes emptyNodes = DiscoveryNodes.builder().build();

        for (String strategyName : List.of("adaptive", "round_robin")) {
            ExternalDistributionStrategy strategy = resolveStrategy(strategyName);
            ExternalDistributionContext context = new ExternalDistributionContext(
                createAggPlan(splits),
                splits,
                emptyNodes,
                QueryPragmas.EMPTY
            );
            ExternalDistributionPlan plan = strategy.planDistribution(context);
            assertFalse("Strategy '" + strategyName + "' must return LOCAL when no eligible nodes", plan.distributed());
        }
    }

    public void testEmptySplitsReturnsLocal() {
        int nodeCount = between(1, 10);
        List<ExternalSplit> splits = List.of();
        DiscoveryNodes nodes = createNodes(nodeCount);

        for (String strategyName : List.of("adaptive", "round_robin", "coordinator_only")) {
            ExternalDistributionStrategy strategy = resolveStrategy(strategyName);
            ExternalDistributionContext context = new ExternalDistributionContext(
                createExternalSourceExec(),
                splits,
                nodes,
                QueryPragmas.EMPTY
            );
            ExternalDistributionPlan plan = strategy.planDistribution(context);
            assertFalse("Strategy '" + strategyName + "' must return LOCAL for empty splits", plan.distributed());
        }
    }

    public void testRoundRobinAssignsToAllNodes() {
        int nodeCount = between(2, 10);
        int splitCount = between(nodeCount, nodeCount * 10);
        List<ExternalSplit> splits = createSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        assertEquals("All nodes must receive assignments", nodeCount, plan.nodeAssignments().size());
        for (Map.Entry<String, List<ExternalSplit>> entry : plan.nodeAssignments().entrySet()) {
            assertFalse("Node " + entry.getKey() + " must have at least one split", entry.getValue().isEmpty());
        }
    }

    public void testMoreNodesThanSplits() {
        int nodeCount = between(5, 20);
        int splitCount = between(1, nodeCount - 1);
        List<ExternalSplit> splits = createSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        assertTrue(plan.distributed());
        int nodesWithSplits = 0;
        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            if (nodeSplits.isEmpty() == false) {
                nodesWithSplits++;
            }
            totalAssigned += nodeSplits.size();
        }
        assertEquals("All splits must be assigned", splitCount, totalAssigned);
        assertEquals("Only splitCount nodes should receive splits", splitCount, nodesWithSplits);
    }

    public void testAdaptiveDistributesWithAggregationAndMultipleSplits() {
        int splitCount = between(2, 100);
        int nodeCount = between(1, 10);
        List<ExternalSplit> splits = createSplits(splitCount);
        DiscoveryNodes nodes = createNodes(nodeCount);

        AdaptiveStrategy strategy = new AdaptiveStrategy();
        ExternalDistributionContext context = new ExternalDistributionContext(createAggPlan(splits), splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue("Adaptive must distribute with aggregation and multiple splits", plan.distributed());

        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            totalAssigned += nodeSplits.size();
        }
        assertEquals("All splits must be assigned", splitCount, totalAssigned);
    }

    public void testCoordinatorOnlyNeverDistributes() {
        int splitCount = between(1, 100);
        int nodeCount = between(1, 10);
        List<ExternalSplit> splits = createSplits(splitCount);
        DiscoveryNodes nodes = createNodes(nodeCount);

        CoordinatorOnlyStrategy strategy = CoordinatorOnlyStrategy.INSTANCE;
        ExternalDistributionContext context = new ExternalDistributionContext(createAggPlan(splits), splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse("CoordinatorOnly must never distribute", plan.distributed());
    }

    public void testStrategyResolutionFromPragma() {
        assertTrue(resolveStrategy("adaptive") instanceof AdaptiveStrategy);
        assertTrue(resolveStrategy("coordinator_only") instanceof CoordinatorOnlyStrategy);
        assertTrue(resolveStrategy("round_robin") instanceof RoundRobinStrategy);
        assertTrue(resolveStrategy("") instanceof AdaptiveStrategy);
        assertTrue("Unknown value should fall back to adaptive", resolveStrategy("unknown_value") instanceof AdaptiveStrategy);
    }

    // --- Helpers ---

    private static ExternalDistributionStrategy resolveStrategy(String name) {
        Settings settings = Settings.builder().put("external_distribution", name).build();
        return ComputeService.resolveExternalDistributionStrategy(new QueryPragmas(settings));
    }

    private static ExternalSourceExec createExternalSourceExec() {
        return new ExternalSourceExec(
            SRC,
            "s3://bucket/data/*.parquet",
            "parquet",
            List.of(
                new FieldAttribute(SRC, "name", new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            Map.of(),
            Map.of(),
            null
        );
    }

    private static AggregateExec createAggPlan(List<ExternalSplit> splits) {
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        return new AggregateExec(SRC, source, List.of(), List.of(), AggregatorMode.INITIAL, source.output(), null);
    }

    private static List<ExternalSplit> createSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit(
                    "parquet",
                    StoragePath.of("s3://bucket/file" + i + ".parquet"),
                    0,
                    1024 * (i + 1),
                    ".parquet",
                    Map.of(),
                    Map.of()
                )
            );
        }
        return splits;
    }

    private static List<DiscoveryNode> createNodeList(int count) {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return nodes;
    }

    private static DiscoveryNodes createNodes(int count) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < count; i++) {
            builder.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return builder.build();
    }
}
