/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
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

        if (shardRouting.currentNodeId() == null) {
            // Shard is not assigned to a node
            return YES_NOT_SNAPSHOTTED;
        }

        for (final var entriesByRepo : snapshotsInProgress.entriesByRepo()) {
            for (final var entry : entriesByRepo) {
                if (entry.isClone()) {
                    // clones do not run on data nodes
                    continue;
                }

                if (entry.hasShardsInInitState() == false) {
                    // this snapshot has no running shard snapshots
                    // (NB this means we let ABORTED shards move without waiting for them to complete)
                    continue;
                }

                final var shardSnapshotStatus = entry.shards().get(shardRouting.shardId());

                if (shardSnapshotStatus == null) {
                    // this snapshot is not snapshotting the shard to allocate
                    continue;
                }

                if (shardSnapshotStatus.state().completed()) {
                    // this shard snapshot is complete
                    continue;
                }

                if (Objects.equals(shardRouting.currentNodeId(), shardSnapshotStatus.nodeId()) == false) {
                    // this shard snapshot is allocated to a different node
                    continue;
                }

                if (shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL) {
                    // this shard snapshot is paused pending the removal of its assigned node
                    final var nodeShutdown = allocation.metadata().nodeShutdowns().get(shardRouting.currentNodeId());
                    if (nodeShutdown != null && nodeShutdown.getType() != SingleNodeShutdownMetadata.Type.RESTART) {
                        // NB we check metadata().nodeShutdowns() too because if the node was marked for removal and then that mark was
                        // removed then the shard can still be PAUSED_FOR_NODE_REMOVAL while there are other shards on the node which
                        // haven't finished pausing yet. In that case the shard is about to go back into INIT state again, so we should keep
                        // it where it is.
                        continue;
                    }
                }

                return allocation.decision(
                    Decision.THROTTLE,
                    NAME,
                    "waiting for snapshot [%s] of shard [%s] to complete on node [%s]",
                    entry.snapshot(),
                    shardRouting.shardId(),
                    shardRouting.currentNodeId()
                );
            }
        }

        return YES_NOT_SNAPSHOTTED;
    }
}
