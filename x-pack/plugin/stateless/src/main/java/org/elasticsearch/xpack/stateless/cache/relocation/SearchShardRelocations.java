/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;

import java.util.Optional;
import java.util.function.Predicate;

/// Counts the shards initializing on a search node, as seen from its `RoutingNode`, where the relocations are the ones with their source
/// set, and the search shards of a node that are yet to start relocating.
final class SearchShardRelocations {

    private SearchShardRelocations() {}

    /// The relocations from the node `sourceNodeId` to `targetNodeId`.
    static int countBetween(ClusterState state, String sourceNodeId, String targetNodeId) {
        return countInitializing(state, targetNodeId, shard -> sourceNodeId.equals(shard.relocatingNodeId()));
    }

    /// The relocations to `targetNodeId` from all the nodes marked for removal.
    static int countFromShuttingDownNodesTo(ClusterState state, String targetNodeId) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        return countInitializing(
            state,
            targetNodeId,
            shard -> shard.relocatingNodeId() != null && nodeShutdowns.isNodeMarkedForRemoval(shard.relocatingNodeId())
        );
    }

    /// Whether a search node marked for removal still holds shards that have not begun to relocate. The master holds them back while it
    /// throttles, so the relocations to a target that the cluster state shows may be only some of the ones the target will get.
    static boolean hasPendingRelocationsOnShuttingDownNodes(ClusterState state) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        for (final String nodeId : nodeShutdowns.getAllNodeIds()) {
            if (nodeShutdowns.isNodeMarkedForRemoval(nodeId) && countPendingRelocationsOnShuttingDownNode(state, nodeId) > 0) {
                return true;
            }
        }
        return false;
    }

    /// The search shards started on `nodeId`, which must be marked for removal, that have not begun to relocate yet. Shards that are not
    /// started yet (initializing) cannot relocate, so they do not count.
    static int countPendingRelocationsOnShuttingDownNode(ClusterState state, String nodeId) {
        assert state.metadata().nodeShutdowns().isNodeMarkedForRemoval(nodeId) : "node [" + nodeId + "] is not marked for removal";
        return searchRoutingNode(state, nodeId).map(node -> node.numberOfShardsWithState(ShardRoutingState.STARTED)).orElse(0);
    }

    /// The shards initializing on `targetNodeId` that match `shardFilter`.
    private static int countInitializing(ClusterState state, String targetNodeId, Predicate<ShardRouting> shardFilter) {
        return searchRoutingNode(state, targetNodeId).map(
            node -> (int) node.shardsWithState(ShardRoutingState.INITIALIZING).filter(shardFilter).count()
        ).orElse(0);
    }

    /// The `RoutingNode` of `nodeId` if it is a search node in the cluster. A search node only holds search shards, as a promotable shard
    /// is only allowed on an index node.
    private static Optional<RoutingNode> searchRoutingNode(ClusterState state, String nodeId) {
        return Optional.ofNullable(state.nodes().get(nodeId))
            .filter(discoveryNode -> discoveryNode.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()))
            .map(discoveryNode -> state.getRoutingNodes().node(nodeId));
    }
}
