/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Distributes external splits across data nodes using a Longest Processing Time (LPT)
 * algorithm that considers {@link ExternalSplit#estimatedSizeInBytes()} for load balancing.
 * When all splits report size information, larger splits are assigned first to the node
 * with the least accumulated load. Falls back to plain round-robin when size info is absent.
 */
public final class WeightedRoundRobinStrategy implements ExternalDistributionStrategy {

    private final NodeEligibilityStrategy eligibility;

    public WeightedRoundRobinStrategy(NodeEligibilityStrategy eligibility) {
        if (eligibility == null) {
            throw new IllegalArgumentException("eligibility must not be null");
        }
        this.eligibility = eligibility;
    }

    public WeightedRoundRobinStrategy() {
        this(NodeEligibilityStrategy.DATA_NODES_ONLY);
    }

    @Override
    public ExternalDistributionPlan planDistribution(ExternalDistributionContext context) {
        List<ExternalSplit> splits = context.splits();
        if (splits.isEmpty()) {
            return ExternalDistributionPlan.LOCAL;
        }

        List<DiscoveryNode> nodes = eligibility.eligibleNodes(context.availableNodes());
        if (nodes.isEmpty()) {
            return ExternalDistributionPlan.LOCAL;
        }

        boolean allHaveSize = true;
        for (ExternalSplit split : splits) {
            if (split.estimatedSizeInBytes() <= 0) {
                allHaveSize = false;
                break;
            }
        }

        if (allHaveSize == false) {
            return RoundRobinStrategy.assignRoundRobin(splits, nodes, context.producerIndex());
        }

        return assignByWeight(splits, nodes, context.producerIndex());
    }

    static ExternalDistributionPlan assignByWeight(List<ExternalSplit> splits, List<DiscoveryNode> nodes) {
        return assignByWeight(splits, nodes, 0);
    }

    /**
     * Packs splits largest-first onto the least-loaded node, resolving ties toward {@code rotation} rather than toward
     * the first node. Every node starts at zero load, so the largest split is always a tie: a fixed resolution sends the
     * largest split of every independently planned producer to the same node. Rotation moves only which node wins a
     * tie. Each split still lands on a least-loaded node, so the packing is as balanced for any rotation.
     */
    static ExternalDistributionPlan assignByWeight(List<ExternalSplit> splits, List<DiscoveryNode> nodes, int rotation) {
        List<ExternalSplit> sorted = new ArrayList<>(splits);
        sorted.sort(Comparator.comparingLong(ExternalSplit::estimatedSizeInBytes).reversed());

        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        long[] nodeLoads = new long[nodes.size()];
        for (DiscoveryNode node : nodes) {
            assignments.put(node.getId(), new ArrayList<>());
        }

        int offset = Math.floorMod(rotation, nodes.size());
        for (ExternalSplit split : sorted) {
            int minIdx = offset;
            for (int step = 1; step < nodeLoads.length; step++) {
                int candidate = (offset + step) % nodeLoads.length;
                if (nodeLoads[candidate] < nodeLoads[minIdx]) {
                    minIdx = candidate;
                }
            }
            assignments.get(nodes.get(minIdx).getId()).add(split);
            nodeLoads[minIdx] += split.estimatedSizeInBytes();
        }

        return new ExternalDistributionPlan(assignments, true);
    }
}
