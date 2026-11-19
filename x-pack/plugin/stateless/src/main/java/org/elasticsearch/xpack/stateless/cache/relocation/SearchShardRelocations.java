/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.function.Predicate;

/// Counts the search shards relocating to a node, as seen from the `RoutingNode` of the target, where they are the shards initializing
/// with their source set.
final class SearchShardRelocations {

    private SearchShardRelocations() {}

    /// The relocations from the node `sourceNodeId` to `targetNodeId`.
    static int countBetween(ClusterState state, String sourceNodeId, String targetNodeId) {
        return countToFromSourcesMatching(state, targetNodeId, sourceNodeId::equals);
    }

    /// The relocations to `targetNodeId` from all the nodes marked for removal.
    static int countFromShuttingDownNodesTo(ClusterState state, String targetNodeId) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        return countToFromSourcesMatching(state, targetNodeId, nodeShutdowns::isNodeMarkedForRemoval);
    }

    /// The relocations to `targetNodeId` from the nodes that are not marked for removal.
    static int countFromNodesNotShuttingDownTo(ClusterState state, String targetNodeId) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        return countToFromSourcesMatching(state, targetNodeId, sourceNodeId -> nodeShutdowns.isNodeMarkedForRemoval(sourceNodeId) == false);
    }

    /// The relocations to `targetNodeId` from the nodes that match `sourceNodeFilter`.
    private static int countToFromSourcesMatching(ClusterState state, String targetNodeId, Predicate<String> sourceNodeFilter) {
        final var discoveryNode = state.nodes().get(targetNodeId);
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        // a search node only holds search shards, as a promotable shard is only allowed on an index node
        if (discoveryNode == null || discoveryNode.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()) == false || targetNode == null) {
            return 0;
        }
        int count = 0;
        for (final ShardRouting shard : targetNode.initializing()) {
            final String sourceNodeId = shard.relocatingNodeId();
            if (sourceNodeId != null && sourceNodeFilter.test(sourceNodeId)) {
                count++;
            }
        }
        return count;
    }
}
