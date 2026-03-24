/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.ReferenceDocs;

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

        RestoreInProgress.Entry restoreInProgress = RestoreInProgress.get(allocation.getClusterState()).get(source.restoreUUID());
        if (restoreInProgress != null) {
            RestoreInProgress.ShardRestoreStatus shardRestoreStatus = restoreInProgress.shards().get(shardRouting.shardId());
            if (shardRestoreStatus != null && shardRestoreStatus.state().completed() == false) {
                assert shardRestoreStatus.state() != RestoreInProgress.State.SUCCESS
                    : "expected shard [" + shardRouting + "] to be in initializing state but got [" + shardRestoreStatus.state() + "]";
                return allocation.decision(Decision.YES, NAME, "shard is currently being restored");
            }
        }

        /**
         * POST: the RestoreInProgress.Entry is non-existent. This section differentiates between a restore that failed
         * because of a indexing fault (see {@link AllocationService.applyFailedShards}) or because of an allocation
         * failure.
         */
        UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        if (unassignedInfo.failedAllocations() > 0) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "shard has failed to be restored from the snapshot [%s] - manually close or delete the index [%s] in order to retry "
                    + "to restore the snapshot again or use the reroute API to force the allocation of an empty primary shard. Check the "
                    + "logs for more information about the failure. Details: [%s]",
                source.snapshot(),
                shardRouting.getIndexName(),
                unassignedInfo.details()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "Restore from snapshot failed because the configured constraints prevented allocation on any of the available nodes. "
                    + "Please check constraints applied in index and cluster settings, then retry the restore. "
                    + "See [%s] for more details on using the allocation explain API.",
                ReferenceDocs.ALLOCATION_EXPLAIN_API
            );
        }
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
