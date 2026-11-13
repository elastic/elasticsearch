/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.HashMap;
import java.util.Map;

/// The search shards relocating to a node: how many come from each node marked for removal, by node id, and how many from the other
/// nodes.
record SearchRelocationsToTarget(Map<String, Integer> fromShuttingDownNodes, int fromNonShuttingDownNodes) {

    /// Counts the search shards relocating to `targetNodeId`, which are INITIALIZING on it with their source set.
    static SearchRelocationsToTarget count(ClusterState state, String targetNodeId) {
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        if (targetNode == null) {
            return new SearchRelocationsToTarget(Map.of(), 0);
        }
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        final Map<String, Integer> fromShuttingDownNodes = new HashMap<>();
        int fromNonShuttingDownNodes = 0;
        for (ShardRouting shard : targetNode.initializing()) {
            final String sourceNodeId = shard.relocatingNodeId();
            if (sourceNodeId == null || shard.isPromotableToPrimary()) {
                continue;
            }
            if (nodeShutdowns.isNodeMarkedForRemoval(sourceNodeId)) {
                fromShuttingDownNodes.merge(sourceNodeId, 1, Integer::sum);
            } else {
                fromNonShuttingDownNodes++;
            }
        }
        return new SearchRelocationsToTarget(fromShuttingDownNodes, fromNonShuttingDownNodes);
    }

    /// The relocations from all the nodes marked for removal.
    int allFromShuttingDownNodes() {
        return fromShuttingDownNodes.values().stream().mapToInt(Integer::intValue).sum();
    }
}
