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

import java.util.Objects;

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

        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(allocation.getClusterState());
        if (snapshotsInProgress.isEmpty()) {
            // Snapshots are not running
            return YES_NOT_RUNNING;
        }

        for (final var entriesByRepo : snapshotsInProgress.entriesByRepo()) {
            for (final var entry : entriesByRepo) {
                if (entry.isClone()) {
                    // clones do not run on data nodes
                    continue;
                }

                if (entry.hasShardsInInitState() == false) {
                    // snapshot has no running shards
                    // NB if all shards are paused this permits them to move even if the corresponding nodes aren't shutting down
                    continue;
                }

                final var shardSnapshotStatus = entry.shards().get(shardRouting.shardId());

                if (shardSnapshotStatus == null) {
                    // snapshot is not snapshotting shard to allocate
                    continue;
                }

                if (shardSnapshotStatus.state().completed()) {
                    // shard snapshot is complete

                    /* TODO we want to block INIT and ABORTED shards from moving because they are still doing work on the data node, but can
                     *  we allow a WAITING shard to move? But if we allow all WAITING shards to move, they may move forever and never reach
                     *  INIT state, so maybe only permit a move if the node is shutting down for removal?
                     */
                    continue;
                }

                if (Objects.equals(shardSnapshotStatus.nodeId(), shardRouting.currentNodeId())) {
                    return allocation.decision(
                        Decision.THROTTLE,
                        NAME,
                        "waiting for snapshot [%s] of shard [%s] to complete on this node [%s]",
                        entry.snapshot(),
                        shardRouting.shardId(),
                        shardRouting.currentNodeId()
                    );
                }
            }
        }

        return YES_NOT_SNAPSHOTTED;
    }
}
