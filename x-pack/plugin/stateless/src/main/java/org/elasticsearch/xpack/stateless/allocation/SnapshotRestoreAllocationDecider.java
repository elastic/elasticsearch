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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;

/** Prevents snapshot restores from being admitted without space for their local files and a node-wide reserve. */
public class SnapshotRestoreAllocationDecider extends AllocationDecider {
    private static final String NAME = "stateless_snapshot_restore_storage";

    private final RelativeByteSizeValue reservedDisk;

    public SnapshotRestoreAllocationDecider(RelativeByteSizeValue reservedDisk) {
        this.reservedDisk = reservedDisk;
    }

    @Override
    public Decision canAllocate(ShardRouting shard, RoutingNode node, RoutingAllocation allocation) {
        if (shard.primary() == false
            || shard.unassigned() == false
            || shard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT
            || node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return Decision.YES;
        }
        Long size = allocation.snapshotShardSizeInfo().getShardSize(shard);
        // Still-fetching (null) is deferred by StatelessExistingShardsAllocator before we run.
        assert size != null : "snapshot shard size should be fetched before capacity decisions";
        if (size == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            return allocation.decision(Decision.NO, NAME, "snapshot shard size is permanently unavailable");
        }
        var disk = allocation.clusterInfo().getNodeMostAvailableDiskUsages().get(node.nodeId());
        if (disk == null) {
            var missingDiskDecision = allocation.isSimulating() ? Decision.NOT_PREFERRED : Decision.THROTTLE;
            return allocation.decision(missingDiskDecision, NAME, "node disk information is unavailable");
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
        long usable = disk.freeBytes() - committed;
        // Same reserve IndexingDiskController uses as its flush/throttle floor (percent of filesystem total, or absolute).
        long headroom = reservedDisk.isAbsolute()
            ? reservedDisk.getAbsolute().getBytes()
            : reservedDisk.calculateValue(ByteSizeValue.ofBytes(disk.totalBytes()), null).getBytes();
        boolean fits = usable >= headroom && size <= usable - headroom;
        return allocation.decision(
            fits ? Decision.YES : allocation.isSimulating() ? Decision.NO : Decision.THROTTLE,
            NAME,
            "snapshot restore storage: free [%d] bytes, incoming commitments [%d] bytes, shard [%d] bytes, headroom [%d] bytes",
            disk.freeBytes(),
            committed,
            size,
            headroom
        );
    }
}
