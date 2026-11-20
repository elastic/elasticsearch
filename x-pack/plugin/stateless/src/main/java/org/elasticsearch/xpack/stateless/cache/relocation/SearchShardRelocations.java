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

/// Counts the search shards relocating to a node, as seen from the `RoutingNode` of the target, where they are the shards initializing
/// with their source set, and the search shards of a node that are yet to start relocating.
final class SearchShardRelocations {

    private SearchShardRelocations() {}

    /// The relocations from the node `sourceNodeId` to `targetNodeId`.
    static int countBetween(ClusterState state, String sourceNodeId, String targetNodeId) {
        return countRelocationsToTarget(state, targetNodeId, sourceNodeId::equals);
    }

    /// The relocations to `targetNodeId` from all the nodes marked for removal.
    static int countFromShuttingDownNodesTo(ClusterState state, String targetNodeId) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        return countRelocationsToTarget(state, targetNodeId, nodeShutdowns::isNodeMarkedForRemoval);
    }

    /// The shards initializing on `targetNodeId` other than the relocations from the nodes marked for removal: the relocations from the
    /// other nodes, the recoveries of unassigned shards and the snapshot restores, which use the slots of the target as well.
    static int countInitializingNotFromShuttingDownNodes(ClusterState state, String targetNodeId) {
        return countWithState(state, targetNodeId, ShardRoutingState.INITIALIZING) - countFromShuttingDownNodesTo(state, targetNodeId);
    }

    /// The search shards started on `nodeId`, which have not begun to relocate yet.
    static int countPendingRelocationsOn(ClusterState state, String nodeId) {
        return countWithState(state, nodeId, ShardRoutingState.STARTED);
    }

    /// Whether a search node marked for removal still holds shards that have not begun to relocate. The master holds them back while it
    /// throttles, so the relocations to a target that the cluster state shows may be only some of the ones the target will get.
    static boolean hasPendingRelocationsOnShuttingDownNodes(ClusterState state) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        for (final String nodeId : nodeShutdowns.getAllNodeIds()) {
            if (nodeShutdowns.isNodeMarkedForRemoval(nodeId) && countPendingRelocationsOn(state, nodeId) > 0) {
                return true;
            }
        }
        return false;
    }

    /// The relocations to `targetNodeId` from the nodes that match `sourceNodeFilter`.
    private static int countRelocationsToTarget(ClusterState state, String targetNodeId, Predicate<String> sourceNodeFilter) {
        return searchRoutingNode(state, targetNodeId).map(node -> countInitializingRelocations(node, sourceNodeFilter)).orElse(0);
    }

    private static int countInitializingRelocations(RoutingNode targetNode, Predicate<String> sourceNodeFilter) {
        int count = 0;
        for (final ShardRouting shard : targetNode.initializing()) {
            final String sourceNodeId = shard.relocatingNodeId();
            if (sourceNodeId != null && sourceNodeFilter.test(sourceNodeId)) {
                count++;
            }
        }
        return count;
    }

    /// The shards of the search node `nodeId` in the state `shardState`.
    private static int countWithState(ClusterState state, String nodeId, ShardRoutingState shardState) {
        return searchRoutingNode(state, nodeId).map(node -> node.numberOfShardsWithState(shardState)).orElse(0);
    }

    /// The `RoutingNode` of `nodeId` if it is a search node in the cluster. A search node only holds search shards, as a promotable shard
    /// is only allowed on an index node.
    private static Optional<RoutingNode> searchRoutingNode(ClusterState state, String nodeId) {
        return Optional.ofNullable(state.nodes().get(nodeId))
            .filter(discoveryNode -> discoveryNode.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()))
            .map(discoveryNode -> state.getRoutingNodes().node(nodeId));
    }
}
