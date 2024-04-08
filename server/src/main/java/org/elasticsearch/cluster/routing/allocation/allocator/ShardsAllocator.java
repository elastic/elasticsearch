/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.MoveDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;

/**
 * <p>
 * A {@link ShardsAllocator} is the main entry point for shard allocation on nodes in the cluster.
 * The allocator makes basic decision where a shard instance will be allocated, if already allocated instances
 * need to relocate to other nodes due to node failures or due to rebalancing decisions.
 * </p>
 */
public interface ShardsAllocator {

    /**
     * Allocates shards to nodes in the cluster. An implementation of this method should:
     * - assign unassigned shards
     * - relocate shards that cannot stay on a node anymore
     * - relocate shards to find a good shard balance in the cluster
     *
     * @param allocation current node allocation
     */
    void allocate(RoutingAllocation allocation);

    /**
     * Allocates shards to nodes in the cluster. An implementation of this method should:
     * - assign unassigned shards
     * - relocate shards that cannot stay on a node anymore
     * - relocate shards to find a good shard balance in the cluster
     *
     * @param allocation current node allocation
     * @param listener listener to be executed once async allocation is completed
     */
    default void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
        allocate(allocation);
        listener.onResponse(null);
    }

    /**
     * Execute allocation commands
     */
    default RoutingExplanations execute(RoutingAllocation allocation, AllocationCommands commands, boolean explain, boolean retryFailed) {
        var originalDebugMode = allocation.getDebugMode();
        allocation.debugDecision(true);
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);

        try {
            if (retryFailed) {
                allocation.routingNodes().resetFailedCounter(allocation.changes());
            }
            return commands.execute(allocation, explain);
        } finally {
            // revert the ignore disable flag, since when rerouting, we want the original setting to take place
            allocation.ignoreDisable(false);
            allocation.setDebugMode(originalDebugMode);
        }
    }

    /**
     * Returns the decision for where a shard should reside in the cluster.  If the shard is unassigned,
     * then the {@link AllocateUnassignedDecision} will be non-null.  If the shard is not in the unassigned
     * state, then the {@link MoveDecision} will be non-null.
     *
     * This method is primarily used by the cluster allocation explain API to provide detailed explanations
     * for the allocation of a single shard.  Implementations of the {@link #allocate(RoutingAllocation)} method
     * may use the results of this method implementation to decide on allocating shards in the routing table
     * to the cluster.
     *
     * If an implementation of this interface does not support explaining decisions for a single shard through
     * the cluster explain API, then this method should throw a {@code UnsupportedOperationException}.
     */
    ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation);
}
