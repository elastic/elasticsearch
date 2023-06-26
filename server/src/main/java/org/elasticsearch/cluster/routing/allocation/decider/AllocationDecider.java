/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;

import java.util.Optional;
import java.util.Set;

/**
 * {@link AllocationDecider} is an abstract base class that allows to make
 * dynamic cluster- or index-wide shard allocation decisions on a per-node
 * basis.
 */
public abstract class AllocationDecider {
    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced to the given allocation. The default is
     * {@link Decision#ALWAYS}.
     */
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be remain
     * on the given node. The default is {@link Decision#ALWAYS}.
     */
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be allocated at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be allocated at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether shards of the given index should be auto-expanded to this node at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the cluster can execute
     * re-balanced operations at all.
     * {@link Decision#ALWAYS}.
     */
    public Decision canRebalance(RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given primary shard can be
     * forcibly allocated on the given node. This method should only be called
     * for unassigned primary shards where the node has a shard copy on disk.
     *
     * Note: all implementations that override this behavior should take into account
     * the results of {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)}
     * before making a decision on force allocation, because force allocation should only
     * be considered if all deciders return {@link Decision#NO}.
     */
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        assert shardRouting.unassigned() : "must not call canForceAllocatePrimary on an assigned shard " + shardRouting;
        Decision decision = canAllocate(shardRouting, node, allocation);
        if (decision.type() == Type.NO) {
            // On a NO decision, by default, we allow force allocating the primary.
            return allocation.decision(
                Decision.YES,
                decision.label(),
                "primary shard [%s] allowed to force allocate on node [%s]",
                shardRouting.shardId(),
                node.nodeId()
            );
        } else {
            // On a THROTTLE/YES decision, we use the same decision instead of forcing allocation
            return decision;
        }
    }

    /**
     * Returns a {@link Decision} whether the given shard can be forced to the
     * given node in the event that the shard's source node is being replaced.
     * This allows nodes using a replace-type node shutdown to
     * override certain deciders in the interest of moving the shard away from
     * a node that *must* be removed.
     *
     * It defaults to returning "YES" and must be overridden by deciders that
     * opt-out to having their other NO decisions *not* overridden while vacating.
     *
     * The caller is responsible for first checking:
     * - that a replacement is ongoing
     * - the shard routing's current node is the source of the replacement
     */
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.YES;
    }

    /**
     * Returns a {@link Decision} whether the given replica shard can be
     * allocated to the given node when there is an existing retention lease
     * already existing on the node (meaning it has been allocated there previously)
     *
     * This method does not actually check whether there is a retention lease,
     * that is the responsibility of the caller.
     *
     * It defaults to the same value as {@code canAllocate}.
     */
    public Decision canAllocateReplicaWhenThereIsRetentionLease(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }

    /**
     * Returns a {@code empty()} if shard could be initially allocated anywhere or {@code Optional.of(Set.of(nodeIds))} if shard could be
     * initially allocated only on subset of a nodes.
     *
     * This might be required for splitting or shrinking index as resulting shards have to be on the same node as a source shard.
     */
    public Optional<Set<String>> getForcedInitialShardAllocationToNodes(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Optional.empty();
    }
}
