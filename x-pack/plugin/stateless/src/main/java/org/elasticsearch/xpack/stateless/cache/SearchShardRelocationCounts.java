/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;

/// Counts the search shards relocating between nodes, as seen from the `RoutingNode` of the source.
final class SearchShardRelocationCounts {

    private SearchShardRelocationCounts() {}

    /// The relocations from the node `sourceNodeId` to `targetNodeId`.
    static int from(ClusterState state, String sourceNodeId, String targetNodeId) {
        final var discoveryNode = state.nodes().get(sourceNodeId);
        final var sourceNode = state.getRoutingNodes().node(sourceNodeId);
        // a search node only holds search shards, as a promotable shard is only allowed on an index node
        if (discoveryNode == null || discoveryNode.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()) == false || sourceNode == null) {
            return 0;
        }
        int count = 0;
        for (final ShardRouting shard : sourceNode.relocating()) {
            if (targetNodeId.equals(shard.relocatingNodeId())) {
                count++;
            }
        }
        return count;
    }

    /// The relocations to `targetNodeId` from all the nodes marked for removal.
    static int fromShuttingDownNodes(ClusterState state, String targetNodeId) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        int count = 0;
        for (final String nodeId : nodeShutdowns.getAllNodeIds()) {
            if (nodeShutdowns.isNodeMarkedForRemoval(nodeId)) {
                count += from(state, nodeId, targetNodeId);
            }
        }
        return count;
    }
}
