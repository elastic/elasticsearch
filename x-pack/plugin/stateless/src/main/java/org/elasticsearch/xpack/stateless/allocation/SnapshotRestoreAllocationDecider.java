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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.IndexingDiskController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Prevents snapshot restores from being admitted without space for their local files and a node-wide reserve.
 * <p>
 * During live (non-simulation) allocation, tracks disk shortfalls for restore shards that were THROTTLEd so
 * autoscaling can raise indexing memory demand. State is only updated from {@link #canAllocate}; simulations
 * do not write. Entries are cleared when a shard fits (YES) or is no longer an unassigned snapshot primary.
 */
public class SnapshotRestoreAllocationDecider extends AllocationDecider {
    private static final String NAME = "stateless_snapshot_restore_storage";

    private final RelativeByteSizeValue indexingReservedDisk;
    /**
     * Per-shard disk bytes still needed to admit the shard onto some indexing node under the reserve rule.
     * Updated only from live {@link #canAllocate} decisions.
     */
    private final ConcurrentHashMap<ShardId, Long> unmetDiskShortfalls = new ConcurrentHashMap<>();

    public SnapshotRestoreAllocationDecider(Settings settings) {
        this.indexingReservedDisk = IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING.get(settings);
    }

    /**
     * Disk bytes currently tracked as unmet for snapshot restores blocked by this decider.
     * Safe to read from the metrics poll; only {@link #canAllocate} mutates the map.
     */
    public long unmetDiskShortfallBytes() {
        return unmetDiskShortfalls.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Snapshot of per-shard unmet disk shortfalls for restores blocked by this decider.
     */
    public Map<ShardId, Long> unmetDiskShortfalls() {
        return Map.copyOf(unmetDiskShortfalls);
    }

    @Override
    public Decision canAllocate(ShardRouting shard, RoutingNode node, RoutingAllocation allocation) {
        final boolean live = allocation.isSimulating() == false;
        if (live) {
            pruneStaleUnmetShortfalls(allocation);
        }
        if (shard.primary() == false
            || shard.unassigned() == false
            || shard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT
            || node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return Decision.YES;
        }
        Long shardSize = allocation.snapshotShardSizeInfo().getShardSize(shard);
        // Still-fetching (null) is deferred by StatelessExistingShardsAllocator before we run.
        assert shardSize != null : "snapshot shard size should be fetched before capacity decisions";
        if (shardSize == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            if (live) {
                unmetDiskShortfalls.remove(shard.shardId());
            }
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
        long freeAfterRestore = disk.freeBytes() - committed - shardSize;
        long indexingReservedBytes = indexingReservedDisk.calculateValue(ByteSizeValue.ofBytes(disk.totalBytes()), null).getBytes();
        boolean fits = freeAfterRestore >= indexingReservedBytes;
        if (live) {
            if (fits) {
                unmetDiskShortfalls.remove(shard.shardId());
            } else {
                long shortfall = indexingReservedBytes - freeAfterRestore;
                assert shortfall > 0 : shortfall;
                unmetDiskShortfalls.merge(shard.shardId(), shortfall, Math::min);
            }
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

    private void pruneStaleUnmetShortfalls(RoutingAllocation allocation) {
        if (unmetDiskShortfalls.isEmpty()) {
            return;
        }
        unmetDiskShortfalls.keySet().removeIf(shardId -> isUnassignedSnapshotPrimary(allocation, shardId) == false);
    }

    private static boolean isUnassignedSnapshotPrimary(RoutingAllocation allocation, ShardId shardId) {
        for (ShardRouting unassigned : allocation.routingNodes().unassigned()) {
            if (unassigned.shardId().equals(shardId)
                && unassigned.primary()
                && unassigned.recoverySource() != null
                && unassigned.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return true;
            }
        }
        return false;
    }
}
