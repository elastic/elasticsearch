/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * This {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider} prevents shards that
 * are currently been snapshotted to be moved to other nodes.
 */
public class SnapshotInProgressAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(SnapshotInProgressAllocationDecider.class);

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

    private Decision canMove(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            // Only primary shards are snapshotted

            SnapshotsInProgress snapshotsInProgress = allocation.custom(SnapshotsInProgress.TYPE);
            if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
                // Snapshots are not running
                return allocation.decision(Decision.YES, NAME, "no snapshots are currently running");
            }

            for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
                if (snapshot.isClone()) {
                    continue;
                }
                SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = snapshot.shards().get(shardRouting.shardId());
                if (shardSnapshotStatus != null && shardSnapshotStatus.state().completed() == false &&
                        shardSnapshotStatus.nodeId() != null && shardSnapshotStatus.nodeId().equals(shardRouting.currentNodeId())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Preventing snapshotted shard [{}] from being moved away from node [{}]",
                                shardRouting.shardId(), shardSnapshotStatus.nodeId());
                    }
                    return allocation.decision(Decision.THROTTLE, NAME,
                        "waiting for snapshotting of shard [%s] to complete on this node [%s]",
                        shardRouting.shardId(), shardSnapshotStatus.nodeId());
                }
            }
        }
        return allocation.decision(Decision.YES, NAME, "the shard is not being snapshotted");
    }

}
