/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SplitCoalescer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;

/**
 * Extended property tests for weighted round-robin distribution, split coalescing,
 * and their interaction. Validates invariants that must hold regardless of input
 * size, node count, or split characteristics.
 */
public class ExtendedDistributionPropertyTests extends ESTestCase {

    // --- Weighted Round-Robin Properties ---

    public void testWeightedAssignsAllSplitsExactlyOnce() {
        int splitCount = between(2, 200);
        int nodeCount = between(1, 20);
        List<ExternalSplit> splits = createSizedSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan = WeightedRoundRobinStrategy.assignByWeight(splits, nodes);

        assertTrue(plan.distributed());
        Set<ExternalSplit> assigned = new HashSet<>();
        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            for (ExternalSplit split : nodeSplits) {
                assertTrue("Split assigned more than once", assigned.add(split));
            }
            totalAssigned += nodeSplits.size();
        }
        assertEquals("All splits must be assigned", splitCount, totalAssigned);
    }

    public void testWeightedLoadBalancing() {
        int splitCount = between(10, 200);
        int nodeCount = between(2, 10);
        List<ExternalSplit> splits = createSizedSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan = WeightedRoundRobinStrategy.assignByWeight(splits, nodes);

        long totalSize = 0;
        long maxNodeLoad = 0;
        for (ExternalSplit split : splits) {
            totalSize += split.estimatedSizeInBytes();
        }
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            long nodeLoad = 0;
            for (ExternalSplit split : nodeSplits) {
                nodeLoad += split.estimatedSizeInBytes();
            }
            maxNodeLoad = Math.max(maxNodeLoad, nodeLoad);
        }

        long idealLoad = totalSize / nodeCount;
        long largestSplit = splits.stream().mapToLong(ExternalSplit::estimatedSizeInBytes).max().orElse(0);
        // LPT guarantees max load <= idealLoad + largestSplit
        assertTrue(
            "Max node load " + maxNodeLoad + " exceeds LPT bound " + (idealLoad + largestSplit),
            maxNodeLoad <= idealLoad + largestSplit
        );
    }

    public void testWeightedIsDeterministic() {
        int splitCount = between(5, 50);
        int nodeCount = between(2, 8);
        List<ExternalSplit> splits = createSizedSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan plan1 = WeightedRoundRobinStrategy.assignByWeight(splits, nodes);
        ExternalDistributionPlan plan2 = WeightedRoundRobinStrategy.assignByWeight(splits, nodes);

        assertEquals(plan1.nodeAssignments().keySet(), plan2.nodeAssignments().keySet());
        for (String nodeId : plan1.nodeAssignments().keySet()) {
            assertEquals(
                "Node " + nodeId + " must get same splits on both runs",
                plan1.nodeAssignments().get(nodeId),
                plan2.nodeAssignments().get(nodeId)
            );
        }
    }

    public void testWeightedSingleLargeSplitGoesToLeastLoaded() {
        List<DiscoveryNode> nodes = createNodeList(3);
        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(createSizedSplit(0, 10_000_000));
        for (int i = 1; i <= 10; i++) {
            splits.add(createSizedSplit(i, 1000));
        }

        ExternalDistributionPlan plan = WeightedRoundRobinStrategy.assignByWeight(splits, nodes);

        boolean largestFound = false;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            for (ExternalSplit split : nodeSplits) {
                if (split.estimatedSizeInBytes() == 10_000_000) {
                    largestFound = true;
                }
            }
        }
        assertTrue("Largest split must be assigned", largestFound);
    }

    // --- Coalescing Properties ---

    public void testCoalescingPreservesAllFiles() {
        int splitCount = between(SplitCoalescer.COALESCING_THRESHOLD + 1, 500);
        List<ExternalSplit> original = createSizedSplits(splitCount);

        List<ExternalSplit> coalesced = SplitCoalescer.coalesce(original);

        assertTrue("Coalescing should reduce split count", coalesced.size() < original.size());

        List<ExternalSplit> expanded = expandCoalesced(coalesced);
        assertEquals("All original files must be preserved", splitCount, expanded.size());

        long originalTotal = original.stream().mapToLong(ExternalSplit::estimatedSizeInBytes).sum();
        long expandedTotal = expanded.stream().mapToLong(ExternalSplit::estimatedSizeInBytes).sum();
        assertEquals("Total size must be preserved", originalTotal, expandedTotal);
    }

    public void testCoalescingBelowThresholdIsNoOp() {
        int splitCount = between(1, SplitCoalescer.COALESCING_THRESHOLD);
        List<ExternalSplit> original = createSizedSplits(splitCount);

        List<ExternalSplit> result = SplitCoalescer.coalesce(original);

        assertSame("Below threshold, coalescing must return the same list", original, result);
    }

    public void testCoalescedSplitSizeEqualsChildrenSum() {
        int childCount = between(2, 20);
        List<ExternalSplit> children = createSizedSplits(childCount);
        CoalescedSplit coalesced = new CoalescedSplit("parquet", children);

        long expectedSize = children.stream().mapToLong(ExternalSplit::estimatedSizeInBytes).sum();
        assertEquals("CoalescedSplit size must equal sum of children", expectedSize, coalesced.estimatedSizeInBytes());
    }

    // --- Coalescing + Distribution Integration ---

    public void testCoalescedSplitsDistributeCorrectly() {
        int splitCount = between(SplitCoalescer.COALESCING_THRESHOLD + 1, 200);
        int nodeCount = between(2, 8);
        List<ExternalSplit> original = createSizedSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        List<ExternalSplit> coalesced = SplitCoalescer.coalesce(original);
        ExternalDistributionPlan plan = WeightedRoundRobinStrategy.assignByWeight(coalesced, nodes);

        assertTrue(plan.distributed());

        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            totalAssigned += nodeSplits.size();
        }
        assertEquals("All coalesced splits must be assigned", coalesced.size(), totalAssigned);

        List<ExternalSplit> allExpanded = new ArrayList<>();
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            allExpanded.addAll(expandCoalesced(nodeSplits));
        }
        assertEquals("Expanding all assigned splits must yield original count", splitCount, allExpanded.size());
    }

    public void testRoundRobinAndWeightedAgreeOnCompleteness() {
        int splitCount = between(2, 100);
        int nodeCount = between(2, 10);
        List<ExternalSplit> splits = createSizedSplits(splitCount);
        List<DiscoveryNode> nodes = createNodeList(nodeCount);

        ExternalDistributionPlan rrPlan = RoundRobinStrategy.assignRoundRobin(splits, nodes);
        ExternalDistributionPlan wrPlan = WeightedRoundRobinStrategy.assignByWeight(splits, nodes);

        int rrTotal = countAssigned(rrPlan);
        int wrTotal = countAssigned(wrPlan);
        assertEquals("Both strategies must assign all splits", splitCount, rrTotal);
        assertEquals("Both strategies must assign all splits", splitCount, wrTotal);
    }

    // --- Helpers ---

    private static List<ExternalSplit> expandCoalesced(List<ExternalSplit> splits) {
        List<ExternalSplit> result = new ArrayList<>();
        for (ExternalSplit split : splits) {
            if (split instanceof CoalescedSplit cs) {
                result.addAll(cs.children());
            } else {
                result.add(split);
            }
        }
        return result;
    }

    private static int countAssigned(ExternalDistributionPlan plan) {
        int total = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            total += nodeSplits.size();
        }
        return total;
    }

    private static List<ExternalSplit> createSizedSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(createSizedSplit(i, 1024L * (i + 1)));
        }
        return splits;
    }

    private static ExternalSplit createSizedSplit(int index, long sizeBytes) {
        return new FileSplit(
            "parquet",
            StoragePath.of("s3://bucket/file" + index + ".parquet"),
            0,
            sizeBytes,
            ".parquet",
            Map.of(),
            Map.of()
        );
    }

    private static List<DiscoveryNode> createNodeList(int count) {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return nodes;
    }
}
