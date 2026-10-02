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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Distributes external splits evenly across eligible remote workers in round-robin order.
 * Falls back to coordinator-only when there are no splits or no eligible workers,
 * including an index-only cluster where the coordinator then runs the scan itself.
 */
public final class RoundRobinStrategy implements ExternalDistributionStrategy {

    private final NodeEligibilityStrategy eligibility;

    public RoundRobinStrategy(NodeEligibilityStrategy eligibility) {
        if (eligibility == null) {
            throw new IllegalArgumentException("eligibility must not be null");
        }
        this.eligibility = eligibility;
    }

    public RoundRobinStrategy() {
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

        int stride = context.placement().stride(splits.size(), nodes.size());
        return assignRoundRobin(splits, nodes, stride);
    }

    static ExternalDistributionPlan assignRoundRobin(List<ExternalSplit> splits, List<DiscoveryNode> nodes) {
        return assignRoundRobin(splits, nodes, 0);
    }

    static ExternalDistributionPlan assignRoundRobin(List<ExternalSplit> splits, List<DiscoveryNode> nodes, int rotation) {
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        for (DiscoveryNode node : nodes) {
            assignments.put(node.getId(), new ArrayList<>());
        }
        int n = nodes.size();
        for (int i = 0; i < splits.size(); i++) {
            String nodeId = nodes.get(Math.floorMod(i + rotation, n)).getId();
            assignments.get(nodeId).add(splits.get(i));
        }
        return new ExternalDistributionPlan(assignments, true);
    }
}
