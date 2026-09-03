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
 * Distributes external splits evenly across eligible data nodes in round-robin order.
 * Falls back to coordinator-only when there are no splits or no eligible nodes.
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

        return assignRoundRobin(splits, nodes, context.producerIndex());
    }

    static ExternalDistributionPlan assignRoundRobin(List<ExternalSplit> splits, List<DiscoveryNode> nodes) {
        return assignRoundRobin(splits, nodes, 0);
    }

    /**
     * Assigns splits round-robin beginning at {@code rotation} rather than at the first node, so that callers planning
     * several producers independently can offset each one and spread their first splits instead of piling them onto one
     * node. Splits remain evenly spread within this call for any rotation, and the assignment map keeps the node order
     * it was given: rotation changes which node receives a split, not the shape or ordering of the result.
     */
    static ExternalDistributionPlan assignRoundRobin(List<ExternalSplit> splits, List<DiscoveryNode> nodes, int rotation) {
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        for (DiscoveryNode node : nodes) {
            assignments.put(node.getId(), new ArrayList<>());
        }
        int offset = Math.floorMod(rotation, nodes.size());
        for (int i = 0; i < splits.size(); i++) {
            String nodeId = nodes.get((i + offset) % nodes.size()).getId();
            assignments.get(nodeId).add(splits.get(i));
        }
        return new ExternalDistributionPlan(assignments, true);
    }
}
