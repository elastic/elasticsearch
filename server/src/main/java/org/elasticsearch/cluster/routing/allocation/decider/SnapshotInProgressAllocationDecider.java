/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.shard.ShardId;

/**
 * This {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider} prevents shards that
 * are currently been snapshotted to be moved to other nodes.
 */
public class SnapshotInProgressAllocationDecider extends AllocationDecider {

    public static final String NAME = "snapshot_in_progress";

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced to the given allocation. The default is
     * {@link Decision#ALWAYS}.
     */
    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canMove(shardRouting, allocation);
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#ALWAYS}.
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canMove(shardRouting, allocation);
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }

    private static final Decision YES_NOT_RUNNING = Decision.single(Decision.Type.YES, NAME, "no snapshots are currently running");
    private static final Decision YES_NOT_SNAPSHOTTED = Decision.single(Decision.Type.YES, NAME, "the shard is not being snapshotted");

    private static Decision canMove(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (allocation.isSimulating()) {
            return allocation.decision(Decision.YES, NAME, "allocation is always enabled when simulating");
        }

        if (shardRouting.primary() == false) {
            // Only primary shards are snapshotted
            return YES_NOT_SNAPSHOTTED;
        }

        SnapshotsInProgress snapshotsInProgress = allocation.getClusterState().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null || snapshotsInProgress.isEmpty()) {
            // Snapshots are not running
            return YES_NOT_RUNNING;
        }

        final ShardId shardId = shardRouting.shardId();
        return snapshotsInProgress.asStream()
            .filter(entry -> entry.hasShardsInInitState() && entry.isClone() == false)
            .map(entry -> entry.shards().get(shardId))
            .filter(
                shardSnapshotStatus -> shardSnapshotStatus != null
                    && shardSnapshotStatus.state().completed() == false
                    && shardSnapshotStatus.nodeId() != null
                    && shardSnapshotStatus.nodeId().equals(shardRouting.currentNodeId())
            )
            .findAny()
            .map(
                shardSnapshotStatus -> allocation.decision(
                    Decision.THROTTLE,
                    NAME,
                    "waiting for snapshotting of shard [%s] to complete on this node [%s]",
                    shardId,
                    shardSnapshotStatus.nodeId()
                )
            )
            .orElse(YES_NOT_SNAPSHOTTED);
    }
}
