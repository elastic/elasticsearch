/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * This {@link AllocationDecider} prevents shards that have failed to be
 * restored from a snapshot to be allocated.
 */
public class RestoreInProgressAllocationDecider extends AllocationDecider {

    public static final String NAME = "restore_in_progress";

    @Override
    public Decision canAllocate(final ShardRouting shardRouting, final RoutingNode node, final RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(final ShardRouting shardRouting, final RoutingAllocation allocation) {
        final RecoverySource recoverySource = shardRouting.recoverySource();
        if (recoverySource == null || recoverySource.getType() != RecoverySource.Type.SNAPSHOT) {
            return allocation.decision(Decision.YES, NAME, "ignored as shard is not being recovered from a snapshot");
        }

        final RecoverySource.SnapshotRecoverySource source = (RecoverySource.SnapshotRecoverySource) recoverySource;
        if (source.restoreUUID().equals(RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID)) {
            return allocation.decision(Decision.YES, NAME, "not an API-level restore");
        }

        final RestoreInProgress restoresInProgress = allocation.custom(RestoreInProgress.TYPE);

        if (restoresInProgress != null) {
            RestoreInProgress.Entry restoreInProgress = restoresInProgress.get(source.restoreUUID());
            if (restoreInProgress != null) {
                RestoreInProgress.ShardRestoreStatus shardRestoreStatus = restoreInProgress.shards().get(shardRouting.shardId());
                if (shardRestoreStatus != null && shardRestoreStatus.state().completed() == false) {
                    assert shardRestoreStatus.state() != RestoreInProgress.State.SUCCESS
                        : "expected shard [" + shardRouting + "] to be in initializing state but got [" + shardRestoreStatus.state() + "]";
                    return allocation.decision(Decision.YES, NAME, "shard is currently being restored");
                }
            }
        }
        return allocation.decision(
            Decision.NO,
            NAME,
            "shard has failed to be restored from the snapshot [%s] - manually close or delete the index [%s] in order to retry "
                + "to restore the snapshot again or use the reroute API to force the allocation of an empty primary shard. Details: [%s]",
            source.snapshot(),
            shardRouting.getIndexName(),
            shardRouting.unassignedInfo().getDetails()
        );
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        return canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }
}
