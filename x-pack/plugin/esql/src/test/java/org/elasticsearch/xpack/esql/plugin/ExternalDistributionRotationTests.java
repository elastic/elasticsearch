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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;

/**
 * Node selection offset by {@code producerIndex}, which is what keeps the source producers of a fan-in from converging
 * on one node.
 *
 * <p>Each producer discovers its own splits and plans its own distribution with no view of its siblings, and both
 * assignment functions used to begin at the first eligible node: round-robin from index zero, and LPT by resolving the
 * opening all-zero-load tie toward index zero. Every producer therefore placed its first and largest split on the same
 * node, and a query over many small datasets loaded one node in proportion to the number of datasets. Offsetting the
 * start by the producer's own index spreads those first splits without giving up either function's balance, which the
 * invariance cases below pin.
 */
public class ExternalDistributionRotationTests extends ESTestCase {

    /**
     * The case the rotation exists for: one split per producer leaves round-robin no room to spread within a producer,
     * so spreading can only come from where each producer starts. Asserted against the unrotated behaviour to show the
     * whole load used to land on a single node.
     */
    public void testSingleSplitProducersSpreadAcrossNodes() {
        DiscoveryNodes nodes = createNodes(4);
        RoundRobinStrategy strategy = new RoundRobinStrategy();

        Map<String, Integer> rotated = new LinkedHashMap<>();
        Map<String, Integer> unrotated = new LinkedHashMap<>();
        for (int producer = 0; producer < 8; producer++) {
            accumulate(rotated, strategy.planDistribution(context(createSplits(1), nodes, producer, 8)));
            accumulate(unrotated, strategy.planDistribution(context(createSplits(1), nodes, 0, 8)));
        }

        assertEquals(List.of(2, 2, 2, 2), List.copyOf(rotated.values()));
        assertEquals(List.of(8, 0, 0, 0), List.copyOf(unrotated.values()));
    }

    /** The same spreading reaches the default strategy, not only the explicitly selected round-robin one. */
    public void testAdaptiveStrategyRotatesAcrossProducers() {
        DiscoveryNodes nodes = createNodes(4);
        AdaptiveStrategy strategy = new AdaptiveStrategy();

        Map<String, Integer> rotated = new LinkedHashMap<>();
        Map<String, Integer> unrotated = new LinkedHashMap<>();
        for (int producer = 0; producer < 4; producer++) {
            accumulate(rotated, strategy.planDistribution(context(createSplits(2), nodes, producer, 4)));
            accumulate(unrotated, strategy.planDistribution(context(createSplits(2), nodes, 0, 4)));
        }

        assertEquals(List.of(2, 2, 2, 2), List.copyOf(rotated.values()));
        assertEquals("two of four nodes idle without rotation", List.of(4, 4, 0, 0), List.copyOf(unrotated.values()));
    }

    public void testRotationZeroMatchesUnrotatedOverload() {
        List<ExternalSplit> splits = createSplits(7);
        var nodeList = eligible(createNodes(3));

        assertEquals(RoundRobinStrategy.assignRoundRobin(splits, nodeList), RoundRobinStrategy.assignRoundRobin(splits, nodeList, 0));
        assertEquals(
            WeightedRoundRobinStrategy.assignByWeight(splits, nodeList),
            WeightedRoundRobinStrategy.assignByWeight(splits, nodeList, 0)
        );
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
     * Rotation may permute which node carries which load but must not worsen the packing, so the sorted load multiset
     * is the invariant rather than any single node's load.
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

    /** Producer counts are unbounded by the node count, so an index past the last node has to wrap. */
    public void testProducerIndexBeyondNodeCountWraps() {
        var nodeList = eligible(createNodes(3));
        List<ExternalSplit> splits = createSplits(5);

        assertEquals(RoundRobinStrategy.assignRoundRobin(splits, nodeList, 0), RoundRobinStrategy.assignRoundRobin(splits, nodeList, 3));
        assertEquals(RoundRobinStrategy.assignRoundRobin(splits, nodeList, 1), RoundRobinStrategy.assignRoundRobin(splits, nodeList, 7));
    }

    public void testNegativeProducerIndexRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalDistributionContext(createPlan(), createSplits(2), createNodes(2), QueryPragmas.EMPTY, -1, 4)
        );
        assertEquals("producerIndex must not be negative", e.getMessage());
    }

    // ---------------------------------------------------------------------------------------------

    private static ExternalDistributionContext context(
        List<ExternalSplit> splits,
        DiscoveryNodes nodes,
        int producerIndex,
        int producerCount
    ) {
        return new ExternalDistributionContext(createPlan(), splits, nodes, QueryPragmas.EMPTY, producerIndex, producerCount);
    }

    private static List<DiscoveryNode> eligible(DiscoveryNodes nodes) {
        return NodeEligibilityStrategy.DATA_NODES_ONLY.eligibleNodes(nodes);
    }

    /** Adds a plan's per-node split counts into {@code totals}, keeping node order stable across plans. */
    private static void accumulate(Map<String, Integer> totals, ExternalDistributionPlan plan) {
        assertTrue(plan.distributed());
        for (var entry : plan.nodeAssignments().entrySet()) {
            totals.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
        }
    }

    private static List<Long> sortedLoads(ExternalDistributionPlan plan) {
        List<Long> loads = new ArrayList<>();
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            long load = 0;
            for (ExternalSplit split : assigned) {
                load += split.estimatedSizeInBytes();
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
