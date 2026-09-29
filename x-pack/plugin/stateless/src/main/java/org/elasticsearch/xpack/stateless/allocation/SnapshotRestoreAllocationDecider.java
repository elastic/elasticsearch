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
        // Unknown size (null) is deferred by StatelessExistingShardsAllocator. A failed size fetch and missing disk
        // stats cannot be evaluated per node: NOT_PREFERRED in simulation still allows a tentative desired
        // assignment; THROTTLE in reconciliation avoids RestoreService failing the API restore on DECIDERS_NO.
        Decision missingInformationDecision = allocation.isSimulating() ? Decision.NOT_PREFERRED : Decision.THROTTLE;
        Long size = allocation.snapshotShardSizeInfo().getShardSize(shard);
        if (size == null || size == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            return allocation.decision(missingInformationDecision, NAME, "snapshot shard size is unavailable");
        }
        var disk = allocation.clusterInfo().getNodeMostAvailableDiskUsages().get(node.nodeId());
        if (disk == null) {
            return allocation.decision(missingInformationDecision, NAME, "node disk information is unavailable");
        }
        // Include recoveries assigned since the last stats refresh; never credit outgoing files before deletion.
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
        // Reject undersized targets in simulation so a previous desired assignment can be reconsidered. During
        // reconciliation, THROTTLE keeps the API restore pending rather than marking it failed.
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
