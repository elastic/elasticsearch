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
 * An allocation decider that prevents shards from being allocated to a
 * node that is in the process of shutting down.
 *
 * In short: No shards can be allocated to, or remain on, a node which is
 * shutting down for removal. Primary shards cannot be allocated to, or remain
 * on, a node which is shutting down for restart.
 */
public class NodeShutdownAllocationDecider extends AllocationDecider {

    private static final String NAME = "node_shutdown";

    /**
     * Determines if a shard can be allocated to a particular node, based on whether that node is shutting down or not.
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final SingleNodeShutdownMetadata thisNodeShutdownMetadata = allocation.nodeShutdowns().get(node.nodeId());

        if (thisNodeShutdownMetadata == null) {
            // There's no shutdown metadata for this node, return yes.
            return allocation.decision(Decision.YES, NAME, "this node is not currently shutting down");
        }

        return switch (thisNodeShutdownMetadata.getType()) {
            case REPLACE, REMOVE -> allocation.decision(
                Decision.NO,
                NAME,
                "node [%s] is preparing to be removed from the cluster",
                node.nodeId()
            );
            case RESTART -> allocation.decision(
                Decision.YES,
                NAME,
                "node [%s] is preparing to restart, but will remain in the cluster",
                node.nodeId()
            );
        };
    }

    /**
     * Applies the same rules as {@link NodeShutdownAllocationDecider#canAllocate(ShardRouting, RoutingNode, RoutingAllocation)} to
     * determine if shards can remain on their current node.
     */
    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return this.canAllocate(shardRouting, node, allocation);
    }

    /**
     * Prevents indices from being auto-expanded to nodes which are in the process of shutting down, regardless of whether they're shutting
     * down for restart or removal.
     */
    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        SingleNodeShutdownMetadata thisNodeShutdownMetadata = allocation.nodeShutdowns().get(node.getId());

        if (thisNodeShutdownMetadata == null) {
            return allocation.decision(Decision.YES, NAME, "node [%s] is not preparing for removal from the cluster", node.getId());
        }

        return switch (thisNodeShutdownMetadata.getType()) {
            case RESTART -> allocation.decision(
                Decision.YES,
                NAME,
                "node [%s] is not preparing for removal from the cluster (is " + "restarting)",
                node.getId()
            );
            case REPLACE, REMOVE -> allocation.decision(
                Decision.NO,
                NAME,
                "node [%s] is preparing for removal from the cluster",
                node.getId()
            );
        };
    }

}
