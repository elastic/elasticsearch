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
            return RoundRobinStrategy.assignRoundRobin(splits, nodes);
        }

        return assignByWeight(splits, nodes);
    }

    static ExternalDistributionPlan assignByWeight(List<ExternalSplit> splits, List<DiscoveryNode> nodes) {
        List<ExternalSplit> sorted = new ArrayList<>(splits);
        sorted.sort(Comparator.comparingLong(ExternalSplit::estimatedSizeInBytes).reversed());

        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        long[] nodeLoads = new long[nodes.size()];
        for (DiscoveryNode node : nodes) {
            assignments.put(node.getId(), new ArrayList<>());
        }

        for (ExternalSplit split : sorted) {
            int minIdx = 0;
            for (int i = 1; i < nodeLoads.length; i++) {
                if (nodeLoads[i] < nodeLoads[minIdx]) {
                    minIdx = i;
                }
            }
            assignments.get(nodes.get(minIdx).getId()).add(split);
            nodeLoads[minIdx] += split.estimatedSizeInBytes();
        }

        return new ExternalDistributionPlan(assignments, true);
    }
}
