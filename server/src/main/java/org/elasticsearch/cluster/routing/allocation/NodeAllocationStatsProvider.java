/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;

import java.util.Map;

public class NodeAllocationStatsProvider {
    private final WriteLoadForecaster writeLoadForecaster;

    public NodeAllocationStatsProvider(WriteLoadForecaster writeLoadForecaster) {
        this.writeLoadForecaster = writeLoadForecaster;
    }

    public Map<String, NodeAllocationStats> stats(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        @Nullable DesiredBalance desiredBalance
    ) {
        var stats = Maps.<String, NodeAllocationStats>newMapWithExpectedSize(clusterState.getRoutingNodes().size());
        for (RoutingNode node : clusterState.getRoutingNodes()) {
            int shards = 0;
            int undesiredShards = 0;
            double forecastedWriteLoad = 0.0;
            long forecastedDiskUsage = 0;
            long currentDiskUsage = 0;
            for (ShardRouting shardRouting : node) {
                if (shardRouting.relocating()) {
                    continue;
                }
                shards++;
                IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(shardRouting.index());
                if (isDesiredAllocation(desiredBalance, shardRouting) == false) {
                    undesiredShards++;
                }
                long shardSize = clusterInfo.getShardSize(shardRouting.shardId(), shardRouting.primary(), 0);
                forecastedWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                forecastedDiskUsage += Math.max(indexMetadata.getForecastedShardSizeInBytes().orElse(0), shardSize);
                currentDiskUsage += shardSize;

            }
            stats.put(
                node.nodeId(),
                new NodeAllocationStats(
                    shards,
                    desiredBalance != null ? undesiredShards : -1,
                    forecastedWriteLoad,
                    forecastedDiskUsage,
                    currentDiskUsage
                )
            );
        }

        return stats;
    }

    private static boolean isDesiredAllocation(DesiredBalance desiredBalance, ShardRouting shardRouting) {
        if (desiredBalance == null) {
            return true;
        }
        var assignment = desiredBalance.getAssignment(shardRouting.shardId());
        if (assignment == null) {
            return false;
        }
        return assignment.nodeIds().contains(shardRouting.currentNodeId());
    }
}
