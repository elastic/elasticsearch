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
                increaseTargetDiskUsage(shard.relocatingNodeId(), size);
                decreaseSourceDiskUsage(shard.currentNodeId(), size);
                // TODO update shard data path?
            } else {
                // new shard
                increaseTargetDiskUsage(shard.currentNodeId(), size);
                this.shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shard), size);
            }
        }
    }

    private Long getEstimatedShardSize(ShardRouting routing) {
        if (routing.relocatingNodeId() != null) {
            // get size of the source shard
            return shardSizes.get(ClusterInfo.shardIdentifierFromRouting(routing));
        } else if (routing.primary() == false) {
            // get size of the source primary shard
            return shardSizes.get(ClusterInfo.shardIdentifierFromRouting(routing.shardId(), true));
        } else {
            // initializing new (empty) primary
            return 0L;
        }
    }

    private void increaseTargetDiskUsage(String nodeId, long size) {
        var leastUsage = leastAvailableSpaceUsage.get(nodeId);
        var mostUsage = mostAvailableSpaceUsage.get(nodeId);
        if (leastUsage == null || mostUsage == null) {
            return;
        }

        // if single data path is used then it is present (and needs to be updated in both maps)
        // otherwise conservatively decrease only lest available space
        if (Objects.equals(leastUsage.getPath(), mostUsage.getPath())) {
            // single data dir is used
            mostAvailableSpaceUsage.put(nodeId, mostUsage.copyWithFreeBytes(mostUsage.getFreeBytes() - size));
        }
        leastAvailableSpaceUsage.put(nodeId, leastUsage.copyWithFreeBytes(leastUsage.getFreeBytes() - size));
    }

    private void decreaseSourceDiskUsage(String nodeId, long size) {
        var leastUsage = leastAvailableSpaceUsage.get(nodeId);
        var mostUsage = mostAvailableSpaceUsage.get(nodeId);
        if (leastUsage == null || mostUsage == null) {
            return;
        }

        // if single data path is used then it is present (and needs to be updated in both maps)
        // otherwise conservatively increase only most available space
        if (Objects.equals(leastUsage.getPath(), mostUsage.getPath())) {
            // single data dir is used
            leastAvailableSpaceUsage.put(nodeId, leastUsage.copyWithFreeBytes(leastUsage.getFreeBytes() + size));
        }
        mostAvailableSpaceUsage.put(nodeId, mostUsage.copyWithFreeBytes(mostUsage.getFreeBytes() + size));
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
