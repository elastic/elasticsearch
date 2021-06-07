/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.ArrayList;
import java.util.List;

/**
 * An abstract class that implements basic functionality for allocating
 * shards to nodes based on shard copies that already exist in the cluster.
 *
 * Individual implementations of this class are responsible for providing
 * the logic to determine to which nodes (if any) those shards are allocated.
 */
public abstract class BaseGatewayShardAllocator {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Allocate an unassigned shard to nodes (if any) where valid copies of the shard already exist.
     * It is up to the individual implementations of {@link #makeAllocationDecision(ShardRouting, RoutingAllocation, Logger)}
     * to make decisions on assigning shards to nodes.
     * @param shardRouting the shard to allocate
     * @param allocation the allocation state container object
     * @param unassignedAllocationHandler handles the allocation of the current shard
     */
    public void allocateUnassigned(ShardRouting shardRouting, RoutingAllocation allocation,
                                   ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler) {
        final AllocateUnassignedDecision allocateUnassignedDecision = makeAllocationDecision(shardRouting, allocation, logger);

        if (allocateUnassignedDecision.isDecisionTaken() == false) {
            // no decision was taken by this allocator
            return;
        }

        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            unassignedAllocationHandler.initialize(allocateUnassignedDecision.getTargetNode().getId(),
                allocateUnassignedDecision.getAllocationId(),
                getExpectedShardSize(shardRouting, allocation),
                allocation.changes());
        } else {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    protected long getExpectedShardSize(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return allocation.snapshotShardSizeInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else {
                return ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
            }
        } else {
            return allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
    }

    /**
     * Make a decision on the allocation of an unassigned shard.  This method is used by
     * {@link #allocateUnassigned(ShardRouting, RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler)} to make decisions
     * about whether or not the shard can be allocated by this allocator and if so, to which node it will be allocated.
     *
     * @param unassignedShard  the unassigned shard to allocate
     * @param allocation       the current routing state
     * @param logger           the logger
     * @return an {@link AllocateUnassignedDecision} with the final decision of whether to allocate and details of the decision
     */
    public abstract AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard,
                                                                      RoutingAllocation allocation,
                                                                      Logger logger);

    /**
     * Builds decisions for all nodes in the cluster, so that the explain API can provide information on
     * allocation decisions for each node, while still waiting to allocate the shard (e.g. due to fetching shard data).
     */
    protected static List<NodeAllocationResult> buildDecisionsForAllNodes(ShardRouting shard, RoutingAllocation allocation) {
        List<NodeAllocationResult> results = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            results.add(new NodeAllocationResult(node.node(), null, decision));
        }
        return results;
    }
}
