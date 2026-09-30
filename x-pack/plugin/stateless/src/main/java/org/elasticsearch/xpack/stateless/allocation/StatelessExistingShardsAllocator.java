/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.List;
import java.util.function.Predicate;

/**
 * Existing-shards allocator for stateless. Shard data lives in the object store, so this allocator does not
 * recover from local copies. It only defers unassigned snapshot primaries until
 * {@link org.elasticsearch.snapshots.InternalSnapshotsInfoService} has fetched their size (mirroring
 * {@code PrimaryShardAllocator}'s {@code FETCHING_SHARD_DATA} gate); desired balance then assigns them with
 * {@link SnapshotRestoreAllocationDecider} enforcing disk capacity.
 */
public class StatelessExistingShardsAllocator implements ExistingShardsAllocator {

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {}

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (waitingForSnapshotShardSize(shardRouting, allocation)) {
            unassignedAllocationHandler.removeAndIgnore(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, allocation.changes());
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        if (waitingForSnapshotShardSize(unassignedShard, routingAllocation)) {
            // No per-node decider walk: capacity answers are meaningless until size is known.
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
        }
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    private static boolean waitingForSnapshotShardSize(ShardRouting shard, RoutingAllocation allocation) {
        return shard.primary()
            && shard.unassigned()
            && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
            && allocation.snapshotShardSizeInfo().getShardSize(shard) == null;
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
