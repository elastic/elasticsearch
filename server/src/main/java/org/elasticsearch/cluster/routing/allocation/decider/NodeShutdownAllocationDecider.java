/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * An allocation decider that prevents shards from being allocated to a node that is in the process of shutting down.
 *
 * No shards can be allocated to or remain on a node which is shutting down for removal.
 * Shards can be allocated to or remain on a node scheduled for a restart.
 */
public class NodeShutdownAllocationDecider extends AllocationDecider {

    private static final String NAME = "node_shutdown";

    private static final Decision YES_EMPTY_SHUTDOWN_METADATA = Decision.single(Decision.Type.YES, NAME, "no nodes are shutting down");
    private static final Decision YES_NODE_NOT_SHUTTING_DOWN = Decision.single(Decision.Type.YES, NAME, "this node is not shutting down");

    /**
     * Determines if a shard can be allocated to a particular node, based on whether that node is shutting down or not.
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return getDecision(allocation, node.nodeId(), false);
    }

    /**
     * Applies the same rules as {@link NodeShutdownAllocationDecider#canAllocate(ShardRouting, RoutingNode, RoutingAllocation)} to
     * determine if shards can remain on their current node.
     */
    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }

    /**
     * Prevents indices from being auto-expanded to nodes which are in the process of shutting down, regardless of whether they're shutting
     * down for restart or removal.
     */
    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return getDecision(allocation, node.getId(), true);
    }

    private static Decision getDecision(RoutingAllocation allocation, String nodeId, boolean canAllocateBeforeReplacementIsReady) {
        final var nodeShutdowns = allocation.metadata().nodeShutdowns().getAll();
        if (nodeShutdowns.isEmpty()) {
            return YES_EMPTY_SHUTDOWN_METADATA;
        }

        final SingleNodeShutdownMetadata thisNodeShutdownMetadata = nodeShutdowns.get(nodeId);
        if (thisNodeShutdownMetadata == null) {
            return YES_NODE_NOT_SHUTTING_DOWN;
        }

        return switch (thisNodeShutdownMetadata.getType()) {
            case REMOVE, SIGTERM -> allocation.decision(Decision.NO, NAME, "node [%s] is preparing to be removed from the cluster", nodeId);
            case REPLACE -> canAllocateBeforeReplacementIsReady
                && allocation.getClusterState().getNodes().hasByName(thisNodeShutdownMetadata.getTargetNodeName()) == false
                    ? allocation.decision(
                        Decision.YES,
                        NAME,
                        "node [%s] is preparing to be removed from the cluster, but replacement is not yet present",
                        nodeId
                    )
                    : allocation.decision(Decision.NO, NAME, "node [%s] is preparing to be removed from the cluster", nodeId);
            case RESTART -> allocation.decision(
                Decision.YES,
                NAME,
                "node [%s] is preparing to restart, but will remain in the cluster",
                nodeId
            );
        };
    }
}
