/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;

/// Counts the search shards relocating to a node, which are INITIALIZING on it with their source set.
final class SearchShardRelocationCounts {

    private SearchShardRelocationCounts() {}

    /// The relocations from the node `sourceNodeId` to `targetNodeId`.
    static int from(ClusterState state, String sourceNodeId, String targetNodeId) {
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        if (targetNode == null) {
            return 0;
        }
        int count = 0;
        for (ShardRouting shard : targetNode.initializing()) {
            if (sourceNodeId.equals(searchRelocationSource(shard))) {
                count++;
            }
        }
        return count;
    }

    /// The relocations to `targetNodeId` from all the nodes marked for removal.
    static int fromShuttingDownNodes(ClusterState state, String targetNodeId) {
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        if (targetNode == null) {
            return 0;
        }
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        int count = 0;
        for (ShardRouting shard : targetNode.initializing()) {
            final String sourceNodeId = searchRelocationSource(shard);
            if (sourceNodeId != null && nodeShutdowns.isNodeMarkedForRemoval(sourceNodeId)) {
                count++;
            }
        }
        return count;
    }

    /// The relocations to `targetNodeId` from the nodes that are not marked for removal.
    static int fromNodesNotShuttingDown(ClusterState state, String targetNodeId) {
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        if (targetNode == null) {
            return 0;
        }
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        int count = 0;
        for (ShardRouting shard : targetNode.initializing()) {
            final String sourceNodeId = searchRelocationSource(shard);
            if (sourceNodeId != null && nodeShutdowns.isNodeMarkedForRemoval(sourceNodeId) == false) {
                count++;
            }
        }
        return count;
    }

    /// The node that `shard` is relocating from, or `null` when it is not a relocating search shard.
    @Nullable
    private static String searchRelocationSource(ShardRouting shard) {
        return shard.isPromotableToPrimary() ? null : shard.relocatingNodeId();
    }
}
