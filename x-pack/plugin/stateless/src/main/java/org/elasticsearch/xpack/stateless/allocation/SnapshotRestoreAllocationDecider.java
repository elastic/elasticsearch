/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.xpack.stateless.IndexingDiskController;

/**
 * Prevents snapshot restores from being admitted without space for their local files and a node-wide reserve.
 * <p>
 * During desired-balance simulation, records each shard's disk feasibility in the allocation-scoped result. Explain and reconciliation do
 * not mutate this result.
 */
public class SnapshotRestoreAllocationDecider extends AllocationDecider {
    private static final String NAME = "stateless_snapshot_restore_storage";

    private final RelativeByteSizeValue indexingReservedDisk;

    public SnapshotRestoreAllocationDecider(Settings settings) {
        this.indexingReservedDisk = IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING.get(settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shard, RoutingNode node, RoutingAllocation allocation) {
        if (shard.primary() == false
            || shard.unassigned() == false
            || shard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT
            || node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return Decision.YES;
        }
        Long shardSize = allocation.snapshotShardSizeInfo().getShardSize(shard);
        if (shardSize == null) {
            return allocation.decision(
                allocation.isSimulating() ? Decision.NOT_PREFERRED : Decision.THROTTLE,
                NAME,
                "snapshot shard size is still being fetched"
            );
        }
        if (shardSize == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            return allocation.decision(Decision.NO, NAME, "snapshot shard size is permanently unavailable");
        }
        var disk = allocation.clusterInfo().getNodeMostAvailableDiskUsages().get(node.nodeId());
        if (disk == null) {
            return allocation.decision(
                allocation.isSimulating() ? Decision.NOT_PREFERRED : Decision.THROTTLE,
                NAME,
                "node disk information is unavailable"
            );
        }
        // freeBytes does not yet reflect space that initializing shards on this node will occupy once
        // recovery finishes. Do not credit relocating-away shards: their files still use disk until deleted.
        long committed = DiskThresholdDecider.sizeOfUnaccountedShards(
            node,
            false,
            disk.path(),
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.globalRoutingTable(),
            allocation.unaccountedSearchableSnapshotSize(node)
        );
        long freeAfterRestore = disk.freeBytes() - committed - shardSize;
        long indexingReservedBytes = indexingReservedDisk.calculateValue(ByteSizeValue.ofBytes(disk.totalBytes()), null).getBytes();
        boolean fits = freeAfterRestore >= indexingReservedBytes;
        if (allocation.isSimulating()) {
            allocation.recordSnapshotRestoreDiskDecision(shard.shardId(), shardSize, fits);
        }
        return allocation.decision(
            fits ? Decision.YES : allocation.isSimulating() ? Decision.NO : Decision.THROTTLE,
            NAME,
            "snapshot restore storage: free [%d] bytes, incoming commitments [%d] bytes, shard [%d] bytes, indexing reserved [%d] bytes",
            disk.freeBytes(),
            committed,
            shardSize,
            indexingReservedBytes
        );
    }
}
