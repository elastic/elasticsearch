/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.ClusterInfo.NodeAndShard;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.CopyOnFirstWriteMap;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

    public ClusterInfoSimulator(RoutingAllocation allocation) {
        this.allocation = allocation;
        this.leastAvailableSpaceUsage = new HashMap<>(allocation.clusterInfo().getNodeLeastAvailableDiskUsages());
        this.mostAvailableSpaceUsage = new HashMap<>(allocation.clusterInfo().getNodeMostAvailableDiskUsages());
        this.shardSizes = new CopyOnFirstWriteMap<>(allocation.clusterInfo().shardSizes);
        this.shardDataSetSizes = Map.copyOf(allocation.clusterInfo().shardDataSetSizes);
        this.dataPath = Map.copyOf(allocation.clusterInfo().dataPath);
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
                modifyDiskUsage(shard.relocatingNodeId(), size);
                modifyDiskUsage(shard.currentNodeId(), -size);
            } else {
                // new shard
                if (shouldReserveSpaceForInitializingShard(shard, allocation.metadata())) {
                    modifyDiskUsage(shard.currentNodeId(), -size);
                }
                shardSizes.put(
                    shardIdentifierFromRouting(shard),
                    allocation.metadata().getIndexSafe(shard.index()).ignoreDiskWatermarks() ? 0 : size
                );
            }
        }
    }

    private void modifyDiskUsage(String nodeId, long freeDelta) {
        if (freeDelta == 0) {
            return;
        }
        var diskUsage = mostAvailableSpaceUsage.get(nodeId);
        if (diskUsage == null) {
            return;
        }
        var path = diskUsage.getPath();
        updateDiskUsage(leastAvailableSpaceUsage, nodeId, path, freeDelta);
        updateDiskUsage(mostAvailableSpaceUsage, nodeId, path, freeDelta);
    }

    private void updateDiskUsage(Map<String, DiskUsage> availableSpaceUsage, String nodeId, String path, long freeDelta) {
        var usage = availableSpaceUsage.get(nodeId);
        if (usage != null && Objects.equals(usage.getPath(), path)) {
            // ensure new value is within bounds
            availableSpaceUsage.put(nodeId, updateWithFreeBytes(usage, freeDelta));
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
