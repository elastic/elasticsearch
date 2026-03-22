/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.function.Function;

/**
 * A read-only view of the allocation infrastructure for a given context. An efficient way
 * to make multiple queries to the allocator.
 */
public class AllocationQueryContext {

    private final AllocationDeciders allocationDeciders;
    private final RoutingAllocation routingAllocation;
    private final Function<ShardRouting, ShardAllocationDecision> explainShardAllocationFunction;

    public AllocationQueryContext(
        RoutingAllocation routingAllocation,
        AllocationDeciders allocationDeciders,
        ShardsAllocator shardAllocator
    ) {
        this.allocationDeciders = allocationDeciders;
        this.routingAllocation = routingAllocation;
        this.explainShardAllocationFunction = shardAllocator.explainShardAllocationFunction(routingAllocation);
    }

    public ClusterState state() {
        return routingAllocation.getClusterState();
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node) {
        return allocationDeciders.canAllocate(shardRouting, node, routingAllocation);
    }

    public Decision canRemain(ShardRouting shardRouting, RoutingNode node) {
        return allocationDeciders.canRemain(shardRouting, node, routingAllocation);
    }

    public boolean debugDecision() {
        return routingAllocation.debugDecision();
    }

    public void debugDecision(boolean debugDecision) {
        routingAllocation.debugDecision(debugDecision);
    }

    public Decision decision(Decision decision, String deciderLabel, String reason, Object... params) {
        return routingAllocation.decision(decision, deciderLabel, reason, params);
    }

    public DesiredNodes desiredNodes() {
        return routingAllocation.desiredNodes();
    }

    public RoutingNodes routingNodes() {
        return routingAllocation.routingNodes();
    }

    public Metadata metadata() {
        return routingAllocation.metadata();
    }

    public ShardAllocationDecision explainShardAllocation(RoutingAllocation.DebugMode debugMode, ShardRouting shardRouting) {
        assert debugMode == RoutingAllocation.DebugMode.ON || debugMode == RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS;
        final var originalDebugMode = routingAllocation.getDebugMode();
        routingAllocation.setDebugMode(debugMode);
        try {
            return explainShardAllocationFunction.apply(shardRouting);
        } finally {
            routingAllocation.setDebugMode(originalDebugMode);
        }
    }
}
