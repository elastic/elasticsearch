/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.esql.datasources.SplitCoalescer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Distributes external splits across eligible remote workers using Longest Processing Time (LPT)
 * on {@link SplitCoalescer#claimCost(ExternalSplit)} (stored bytes plus a per-leaf open cost).
 * When all splits report size information, higher-cost splits are assigned first to the node
 * with the least accumulated claim cost. Weighing by stored bytes alone would dump every
 * many-leaf group of tiny files onto one node. Falls back to plain round-robin when size info
 * is absent. An empty eligible-worker set returns {@code LOCAL} so the coordinator runs the scan
 * itself.
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
        this(NodeEligibilityStrategy.EXTERNAL_WORKER_NODES);
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
            // Unknown size is negative. Zero is an empty file; it still has open cost.
            if (split.estimatedSizeInBytes() < 0) {
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
        int n = splits.size();
        Integer[] order = new Integer[n];
        long[] costs = new long[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
            costs[i] = SplitCoalescer.claimCost(splits.get(i));
        }
        Arrays.sort(order, Comparator.comparingLong((Integer i) -> costs[i]).reversed());

        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        long[] nodeLoads = new long[nodes.size()];
        for (DiscoveryNode node : nodes) {
            assignments.put(node.getId(), new ArrayList<>());
        }

        for (int idx : order) {
            int minIdx = 0;
            for (int i = 1; i < nodeLoads.length; i++) {
                if (nodeLoads[i] < nodeLoads[minIdx]) {
                    minIdx = i;
                }
            }
            assignments.get(nodes.get(minIdx).getId()).add(splits.get(idx));
            nodeLoads[minIdx] += costs[idx];
        }

        return new ExternalDistributionPlan(assignments, true);
    }
}
