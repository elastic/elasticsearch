/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ClusterInfoSimulator {

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    private final Map<String, Long> shardSizes;
    private final Map<ShardId, Long> shardDataSetSizes;
    private final Map<ShardRouting, String> routingToDataPath;

    public ClusterInfoSimulator(ClusterInfo clusterInfo) {
        this.leastAvailableSpaceUsage = new HashMap<>(clusterInfo.getNodeLeastAvailableDiskUsages());
        this.mostAvailableSpaceUsage = new HashMap<>(clusterInfo.getNodeMostAvailableDiskUsages());
        this.shardSizes = new HashMap<>(clusterInfo.shardSizes);
        this.shardDataSetSizes = new HashMap<>(clusterInfo.shardDataSetSizes);
        this.routingToDataPath = new HashMap<>(clusterInfo.routingToDataPath);
    }

    public void simulate(ShardRouting shard) {
        assert shard.initializing();

        var size = getEstimatedShardSize(shard);
        if (size != null && size > 0) {
            if (shard.relocatingNodeId() != null) {
                // relocation
                modifyDiskUsage(shard.relocatingNodeId(), getShardPath(shard.relocatingNodeId(), mostAvailableSpaceUsage), size);
                modifyDiskUsage(shard.currentNodeId(), getShardPath(shard.currentNodeId(), leastAvailableSpaceUsage), -size);
            } else {
                // new shard
                modifyDiskUsage(shard.currentNodeId(), getShardPath(shard.currentNodeId(), leastAvailableSpaceUsage), -size);
                shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shard), size);
            }
        }
    }

    private Long getEstimatedShardSize(ShardRouting routing) {
        if (routing.relocatingNodeId() != null) {
            // relocation existing shard, get size of the source shard
            return shardSizes.get(ClusterInfo.shardIdentifierFromRouting(routing));
        } else if (routing.primary() == false) {
            // initializing new replica, get size of the source primary shard
            return shardSizes.get(ClusterInfo.shardIdentifierFromRouting(routing.shardId(), true));
        } else {
            // initializing new (empty) primary
            return 0L;
        }
    }

    private String getShardPath(String nodeId, Map<String, DiskUsage> defaultSpaceUsage) {
        var diskUsage = defaultSpaceUsage.get(nodeId);
        return diskUsage != null ? diskUsage.getPath() : null;
    }

    private void modifyDiskUsage(String nodeId, String path, long delta) {
        var leastUsage = leastAvailableSpaceUsage.get(nodeId);
        if (leastUsage != null && Objects.equals(leastUsage.getPath(), path)) {
            // ensure new value is within bounds
            leastAvailableSpaceUsage.put(nodeId, updateWithFreeBytes(leastUsage, delta));
        }
        var mostUsage = mostAvailableSpaceUsage.get(nodeId);
        if (mostUsage != null && Objects.equals(mostUsage.getPath(), path)) {
            // ensure new value is within bounds
            mostAvailableSpaceUsage.put(nodeId, updateWithFreeBytes(mostUsage, delta));
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
            shardSizes,
            shardDataSetSizes,
            routingToDataPath,
            Map.of()
        );
    }
}
