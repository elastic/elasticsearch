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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SplitCoalescer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;

/**
 * Node selection offset by {@link SiblingPlacement#stride(int, int)}, which is what keeps the
 * leaves of a merge from converging on one node.
 */
public class ExternalDistributionRotationTests extends ESTestCase {

    /**
     * One split per producer leaves round-robin no room to spread within a producer, so spreading
     * can only come from where each producer starts.
     */
    public void testSingleSplitProducersSpreadAcrossNodes() {
        DiscoveryNodes nodes = createNodes(4);
        RoundRobinStrategy strategy = new RoundRobinStrategy();

        Map<String, Integer> rotated = new LinkedHashMap<>();
        Map<String, Integer> unrotated = new LinkedHashMap<>();
        for (int producer = 0; producer < 8; producer++) {
            accumulate(rotated, strategy.planDistribution(context(createSplits(1), nodes, new SiblingPlacement(producer, 8, true))));
            accumulate(unrotated, strategy.planDistribution(context(createSplits(1), nodes, new SiblingPlacement(0, 8, true))));
        }

        assertEquals(List.of(2, 2, 2, 2), List.copyOf(rotated.values()));
        assertEquals(List.of(8, 0, 0, 0), List.copyOf(unrotated.values()));
    }

    /**
     * Plus-one only slides a three-wide window and overloads the middle nodes; stride starts each
     * producer after the previous producer's last split.
     */
    public void testFewSplitsOnManyNodesUseStrideNotPlusOne() {
        DiscoveryNodes nodes = createNodes(10);
        List<DiscoveryNode> eligible = eligible(nodes);
        List<ExternalSplit> splits = createSplits(3);

        Set<String> strideNodes = new LinkedHashSet<>();
        Set<String> plusOneNodes = new LinkedHashSet<>();
        for (int producer = 0; producer < 3; producer++) {
            int stride = new SiblingPlacement(producer, 3, true).stride(splits.size(), eligible.size());
            accumulateNodes(strideNodes, RoundRobinStrategy.assignRoundRobin(splits, eligible, stride));
            accumulateNodes(plusOneNodes, RoundRobinStrategy.assignRoundRobin(splits, eligible, producer));
        }

        assertEquals(9, strideNodes.size());
        assertEquals(5, plusOneNodes.size());
    }

    public void testRotationZeroAssignsFromTheFirstNode() {
        List<ExternalSplit> splits = createSplits(7);
        var nodeList = eligible(createNodes(3));

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodeList, 0);
        List<String> nodeIds = List.copyOf(plan.nodeAssignments().keySet());
        assertEquals(3, plan.nodeAssignments().get(nodeIds.get(0)).size());
        assertEquals(2, plan.nodeAssignments().get(nodeIds.get(1)).size());
        assertEquals(2, plan.nodeAssignments().get(nodeIds.get(2)).size());
        assertEquals(List.of(splits.get(0), splits.get(3), splits.get(6)), plan.nodeAssignments().get(nodeIds.get(0)));
        assertEquals(List.of(splits.get(1), splits.get(4)), plan.nodeAssignments().get(nodeIds.get(1)));
        assertEquals(List.of(splits.get(2), splits.get(5)), plan.nodeAssignments().get(nodeIds.get(2)));
    }

    public void testRoundRobinStaysEvenForEveryRotation() {
        var nodeList = eligible(createNodes(4));
        List<ExternalSplit> splits = createSplits(12);

        for (int rotation = 0; rotation < 8; rotation++) {
            var plan = RoundRobinStrategy.assignRoundRobin(splits, nodeList, rotation);
            for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
                assertEquals("rotation " + rotation, 3, assigned.size());
            }
        }
    }

    /**
     * Rotation may permute which node carries which load but must not worsen the packing, so the
     * sorted claim-cost multiset is the invariant rather than any single node's load.
     */
    public void testWeightedPackingQualityIsRotationInvariant() {
        var nodeList = eligible(createNodes(3));
        List<ExternalSplit> splits = List.of(sized(1000), sized(500), sized(300), sized(200));

        List<Long> baseline = sortedLoads(WeightedRoundRobinStrategy.assignByWeight(splits, nodeList, 0));
        for (int rotation = 1; rotation < 6; rotation++) {
            assertEquals(
                "rotation " + rotation,
                baseline,
                sortedLoads(WeightedRoundRobinStrategy.assignByWeight(splits, nodeList, rotation))
            );
        }
    }

    /** The opening tie is what rotation redirects, and the largest split is always part of that tie. */
    public void testWeightedPlacesLargestSplitOnRotatedNode() {
        var nodeList = eligible(createNodes(4));
        ExternalSplit largest = sized(9000);
        List<ExternalSplit> splits = List.of(sized(100), largest, sized(200), sized(300));

        for (int rotation = 0; rotation < 4; rotation++) {
            var plan = WeightedRoundRobinStrategy.assignByWeight(splits, nodeList, rotation);
            String expectedNode = nodeList.get(rotation).getId();
            assertTrue("rotation " + rotation, plan.nodeAssignments().get(expectedNode).contains(largest));
        }
    }

    /** Callers read assignments in node order, so rotation must move splits between nodes without reordering them. */
    public void testAssignmentKeyOrderIsIndependentOfRotation() {
        var nodeList = eligible(createNodes(4));
        List<ExternalSplit> splits = createSplits(6);
        List<String> expected = List.copyOf(RoundRobinStrategy.assignRoundRobin(splits, nodeList, 0).nodeAssignments().keySet());

        for (int rotation = 1; rotation < 6; rotation++) {
            assertEquals(expected, List.copyOf(RoundRobinStrategy.assignRoundRobin(splits, nodeList, rotation).nodeAssignments().keySet()));
            assertEquals(
                expected,
                List.copyOf(WeightedRoundRobinStrategy.assignByWeight(splits, nodeList, rotation).nodeAssignments().keySet())
            );
        }
    }

    /** Sibling counts are unbounded by the node count, so a stride past the last node has to wrap. */
    public void testStridePastNodeCountWraps() {
        var nodeList = eligible(createNodes(3));
        List<ExternalSplit> splits = createSplits(5);

        assertEquals(RoundRobinStrategy.assignRoundRobin(splits, nodeList, 0), RoundRobinStrategy.assignRoundRobin(splits, nodeList, 3));
        assertEquals(RoundRobinStrategy.assignRoundRobin(splits, nodeList, 1), RoundRobinStrategy.assignRoundRobin(splits, nodeList, 7));
        assertEquals(0, new SiblingPlacement(3, 6, true).stride(1, 3));
        assertEquals(1, new SiblingPlacement(4, 6, true).stride(1, 3));
    }

    public void testForkBranchesRotate() {
        var nodeList = eligible(createNodes(10));
        List<ExternalSplit> splits = createSplits(3);

        Set<String> nodes = new LinkedHashSet<>();
        for (int branch = 0; branch < 2; branch++) {
            int stride = SiblingPlacement.forMerge(MergeExec.Kind.FORK, branch, 2).stride(splits.size(), nodeList.size());
            accumulateNodes(nodes, RoundRobinStrategy.assignRoundRobin(splits, nodeList, stride));
        }
        assertEquals(6, nodes.size());
    }

    /**
     * A sibling with fewer splits than an earlier one may share nodes with it. Stride uses this
     * producer's own split count, not its siblings'.
     */
    public void testUnequalSplitCountsMayShareNodes() {
        var nodeList = eligible(createNodes(10));
        List<ExternalSplit> three = createSplits(3);
        List<ExternalSplit> one = createSplits(1);

        int stride0 = new SiblingPlacement(0, 3, true).stride(three.size(), nodeList.size());
        int stride1 = new SiblingPlacement(1, 3, true).stride(one.size(), nodeList.size());
        int stride2 = new SiblingPlacement(2, 3, true).stride(one.size(), nodeList.size());

        assertEquals(Set.of("node-0", "node-1", "node-2"), assignedNodeIds(RoundRobinStrategy.assignRoundRobin(three, nodeList, stride0)));
        assertEquals(Set.of("node-1"), assignedNodeIds(RoundRobinStrategy.assignRoundRobin(one, nodeList, stride1)));
        assertEquals(Set.of("node-2"), assignedNodeIds(RoundRobinStrategy.assignRoundRobin(one, nodeList, stride2)));
    }

    public void testNegativeIndexRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SiblingPlacement(-1, 4, true));
        assertEquals("index must not be negative", e.getMessage());
    }

    public void testIndexAtLeastCountRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SiblingPlacement(4, 4, true));
        assertEquals("index [4] must be less than count [4]", e.getMessage());
    }

    public void testHasSourceSiblings() {
        assertFalse(SiblingPlacement.SINGLE.hasSourceSiblings());
        assertFalse(new SiblingPlacement(0, 1, true).hasSourceSiblings());
        assertFalse(new SiblingPlacement(0, 4, false).hasSourceSiblings());
        assertFalse(new SiblingPlacement(3, 4, false).hasSourceSiblings());
        assertTrue(new SiblingPlacement(0, 4, true).hasSourceSiblings());
        assertTrue(new SiblingPlacement(3, 4, true).hasSourceSiblings());
    }

    public void testForMerge() {
        assertEquals(new SiblingPlacement(1, 3, false), SiblingPlacement.forMerge(MergeExec.Kind.FORK, 1, 3));
        assertEquals(new SiblingPlacement(1, 3, true), SiblingPlacement.forMerge(MergeExec.Kind.UNION, 1, 3));
    }

    private static ExternalDistributionContext context(List<ExternalSplit> splits, DiscoveryNodes nodes, SiblingPlacement placement) {
        return new ExternalDistributionContext(createPlan(), splits, nodes, QueryPragmas.EMPTY, placement);
    }

    private static List<DiscoveryNode> eligible(DiscoveryNodes nodes) {
        List<DiscoveryNode> list = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes);
        list.sort(Comparator.comparing(DiscoveryNode::getId));
        return list;
    }

    private static void accumulate(Map<String, Integer> totals, ExternalDistributionPlan plan) {
        assertTrue(plan.distributed());
        for (var entry : plan.nodeAssignments().entrySet()) {
            totals.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
        }
    }

    private static void accumulateNodes(Set<String> nodes, ExternalDistributionPlan plan) {
        assertTrue(plan.distributed());
        nodes.addAll(assignedNodeIds(plan));
    }

    private static Set<String> assignedNodeIds(ExternalDistributionPlan plan) {
        Set<String> ids = new LinkedHashSet<>();
        for (var entry : plan.nodeAssignments().entrySet()) {
            if (entry.getValue().isEmpty() == false) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

    private static List<Long> sortedLoads(ExternalDistributionPlan plan) {
        List<Long> loads = new ArrayList<>();
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            long load = 0;
            for (ExternalSplit split : assigned) {
                load += SplitCoalescer.claimCost(split);
            }
            loads.add(load);
        }
        Collections.sort(loads);
        return loads;
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

    private static ExternalSplit sized(long bytes) {
        return new FileSplit("parquet", StoragePath.of("s3://bucket/f" + bytes + ".parquet"), 0, bytes, ".parquet", Map.of(), Map.of());
    }

    private static DiscoveryNodes createNodes(int count) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < count; i++) {
            builder.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return builder.build();
    }
}
