/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.List;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;

public class SearchableSnapshotAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "searchable_snapshot_allocator";

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (shardRouting.primary()
            && (shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
                || shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE)) {
            // we always force snapshot recovery source to use the snapshot-based recovery process on the node

            final Settings indexSettings = allocation.metadata().index(shardRouting.index()).getSettings();
            final IndexId indexId = new IndexId(
                SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
            );
            final SnapshotId snapshotId = new SnapshotId(
                SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
            );
            final String repository = SNAPSHOT_REPOSITORY_SETTING.get(indexSettings);
            final Snapshot snapshot = new Snapshot(repository, snapshotId);

            shardRouting = unassignedAllocationHandler.updateUnassigned(
                shardRouting.unassignedInfo(),
                new RecoverySource.SnapshotRecoverySource(
                    RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    snapshot,
                    Version.CURRENT,
                    indexId
                ),
                allocation.changes()
            );
        }

        final AllocateUnassignedDecision allocateUnassignedDecision = decideAllocation(allocation, shardRouting);

        if (allocateUnassignedDecision.isDecisionTaken() && allocateUnassignedDecision.getAllocationDecision() != AllocationDecision.YES) {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    private AllocateUnassignedDecision decideAllocation(RoutingAllocation allocation, ShardRouting shardRouting) {
        assert shardRouting.unassigned();
        assert ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
            allocation.metadata().getIndexSafe(shardRouting.index()).getSettings()
        ).equals(ALLOCATOR_NAME);

        // let BalancedShardsAllocator take care of allocating this shard
        // TODO: once we have persistent cache, choose a node that has existing data
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        return decideAllocation(routingAllocation, shardRouting);
    }

    @Override
    public void cleanCaches() {}

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

    @Override
    public int getNumberOfInFlightFetches() {
        return 0;
    }
}
