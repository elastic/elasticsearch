/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.ClusterInfo.NodeAndPath;
import org.elasticsearch.cluster.ClusterInfo.NodeAndShard;
import org.elasticsearch.cluster.ClusterInfo.ReservedSpace;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.CopyOnFirstWriteMap;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.shouldReserveSpaceForInitializingShard;
import static org.elasticsearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;

public class ClusterInfoSimulator {

    private final RoutingAllocation allocation;

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    private final CopyOnFirstWriteMap<String, Long> shardSizes;
    private final Map<ShardId, Long> shardDataSetSizes;
    private final Map<NodeAndShard, String> dataPath;
    private Map<NodeAndPath, ReservedSpace> reservedSpace;

    public ClusterInfoSimulator(RoutingAllocation allocation) {
        this.allocation = allocation;
        this.leastAvailableSpaceUsage = getAdjustedDiskSpace(allocation.clusterInfo(), ClusterInfo::getNodeLeastAvailableDiskUsages);
        this.mostAvailableSpaceUsage = getAdjustedDiskSpace(allocation.clusterInfo(), ClusterInfo::getNodeMostAvailableDiskUsages);
        this.shardSizes = new CopyOnFirstWriteMap<>(allocation.clusterInfo().shardSizes);
        this.shardDataSetSizes = Map.copyOf(allocation.clusterInfo().shardDataSetSizes);
        this.dataPath = Map.copyOf(allocation.clusterInfo().dataPath);
        this.reservedSpace = Map.copyOf(allocation.clusterInfo().reservedSpace);
    }

    /**
     * This adds the reserved space to the actual disk usage
     * as all initializing shards are going to be started during simulation.
     */
    private static Map<String, DiskUsage> getAdjustedDiskSpace(
        ClusterInfo clusterInfo,
        Function<ClusterInfo, Map<String, DiskUsage>> getter
    ) {
        var availableSpaceUsage = new HashMap<>(getter.apply(clusterInfo));
        for (var entry : availableSpaceUsage.entrySet()) {
            var diskUsage = entry.getValue();
            var reservedSpace = clusterInfo.getReservedSpace(diskUsage.nodeId(), diskUsage.path());
            if (reservedSpace != ReservedSpace.EMPTY) {
                entry.setValue(updateWithFreeBytes(diskUsage, -reservedSpace.total()));
            }
        }
        return availableSpaceUsage;
    }

    /**
     * Must be called all shards that are in progress of initializations are processed
     */
    public void discardReservedSpace() {
        reservedSpace = Map.of();
    }

    /**
     * This method updates disk usage to reflect shard relocations and new replica initialization.
     * In case of a single data path both mostAvailableSpaceUsage and leastAvailableSpaceUsage are update to reflect the change.
     * In case of multiple data path only mostAvailableSpaceUsage as it is used in calculation in
     * {@link org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider} for allocating new shards.
     * This assumes the worst case (all shards are placed on a single most used disk) and prevents node overflow.
     * Balance is later recalculated with a refreshed cluster info containing actual shards placement.
     */
    public void simulateShardStarted(ShardRouting shard) {
        assert shard.initializing();

        var shardId = shard.shardId();
        var size = getExpectedShardSize(
            shard,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            getClusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
        if (size != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            if (shard.relocatingNodeId() != null) {
                // relocation
                modifyDiskUsage(shard.relocatingNodeId(), shardId, size);
                modifyDiskUsage(shard.currentNodeId(), shardId, -size);
            } else {
                // new shard
                if (shouldReserveSpaceForInitializingShard(shard, allocation.metadata())) {
                    modifyDiskUsage(shard.currentNodeId(), shardId, -size);
                }
                shardSizes.put(
                    shardIdentifierFromRouting(shard),
                    allocation.metadata().getIndexSafe(shard.index()).ignoreDiskWatermarks() ? 0 : size
                );
            }
        }
    }

    private void modifyDiskUsage(String nodeId, ShardId shardId, long delta) {
        if (delta == 0) {
            return;
        }
        var diskUsage = mostAvailableSpaceUsage.get(nodeId);
        if (diskUsage == null) {
            return;
        }
        var path = diskUsage.getPath();
        updateDiskUsage(leastAvailableSpaceUsage, nodeId, path, shardId, delta);
        updateDiskUsage(mostAvailableSpaceUsage, nodeId, path, shardId, delta);
    }

    private void updateDiskUsage(Map<String, DiskUsage> availableSpaceUsage, String nodeId, String path, ShardId shardId, long delta) {
        if (reservedSpace.getOrDefault(new NodeAndPath(nodeId, path), ReservedSpace.EMPTY).containsShardId(shardId)) {
            // space is already reserved and accounted for
            return;
        }
        var usage = availableSpaceUsage.get(nodeId);
        if (usage != null && Objects.equals(usage.getPath(), path)) {
            // ensure new value is within bounds
            availableSpaceUsage.put(nodeId, updateWithFreeBytes(usage, delta));
        }
    }

    private static DiskUsage updateWithFreeBytes(DiskUsage usage, long delta) {
        // free bytes might go out of range in case when multiple data path are used
        // we might not know exact disk used to allocate a shard and conservatively update
        // most used disk on a target node and least used disk on a source node
        var freeBytes = withinRange(0, usage.getTotalBytes(), usage.freeBytes() + delta);
        return usage.copyWithFreeBytes(freeBytes);
    }

    private static long withinRange(long min, long max, long value) {
        return Math.max(min, Math.min(max, value));
    }

    public ClusterInfo getClusterInfo() {
        return new ClusterInfo(
            leastAvailableSpaceUsage,
            mostAvailableSpaceUsage,
            shardSizes.toImmutableMap(),
            shardDataSetSizes,
            dataPath,
            Map.of()
        );
    }
}
