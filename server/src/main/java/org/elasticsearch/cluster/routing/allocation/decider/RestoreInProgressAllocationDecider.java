/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.Snapshot;

/**
 * This {@link AllocationDecider} prevents shards that have failed to be
 * restored from a snapshot to be allocated.
 */
public class RestoreInProgressAllocationDecider extends AllocationDecider {

    public static final String NAME = "restore_in_progress";

    /**
     * Creates a new {@link RestoreInProgressAllocationDecider} instance from
     * given settings
     *
     * @param settings {@link Settings} to use
     */
    public RestoreInProgressAllocationDecider(Settings settings) {
        super(settings);
    }

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

        final Snapshot snapshot = ((RecoverySource.SnapshotRecoverySource) recoverySource).snapshot();
        final RestoreInProgress restoresInProgress = allocation.custom(RestoreInProgress.TYPE);

        if (restoresInProgress != null) {
            for (RestoreInProgress.Entry restoreInProgress : restoresInProgress.entries()) {
                if (restoreInProgress.snapshot().equals(snapshot)) {
                    RestoreInProgress.ShardRestoreStatus shardRestoreStatus = restoreInProgress.shards().get(shardRouting.shardId());
                    if (shardRestoreStatus != null && shardRestoreStatus.state().completed() == false) {
                        assert shardRestoreStatus.state() != RestoreInProgress.State.SUCCESS : "expected shard [" + shardRouting
                            + "] to be in initializing state but got [" + shardRestoreStatus.state() + "]";
                        return allocation.decision(Decision.YES, NAME, "shard is currently being restored");
                    }
                    break;
                }
            }
        }
        return allocation.decision(Decision.NO, NAME, "shard has failed to be restored from the snapshot [%s] because of [%s] - " +
            "manually close or delete the index [%s] in order to retry to restore the snapshot again or use the reroute API to force the " +
            "allocation of an empty primary shard", snapshot, shardRouting.unassignedInfo().getDetails(), shardRouting.getIndexName());
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        return canAllocate(shardRouting, node, allocation);
    }
}
