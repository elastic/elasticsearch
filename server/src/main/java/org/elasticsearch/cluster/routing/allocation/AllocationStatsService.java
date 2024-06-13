/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;

import java.util.Map;

public class AllocationStatsService {

    private final ClusterService clusterService;
    private final ClusterInfoService clusterInfoService;
    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator;
    private final WriteLoadForecaster writeLoadForecaster;

    public AllocationStatsService(
        ClusterService clusterService,
        ClusterInfoService clusterInfoService,
        ShardsAllocator shardsAllocator,
        WriteLoadForecaster writeLoadForecaster
    ) {
        this.clusterService = clusterService;
        this.clusterInfoService = clusterInfoService;
        this.desiredBalanceShardsAllocator = shardsAllocator instanceof DesiredBalanceShardsAllocator allocator ? allocator : null;
        this.writeLoadForecaster = writeLoadForecaster;
    }

    public Map<String, NodeAllocationStats> stats() {
        var state = clusterService.state();
        var info = clusterInfoService.getClusterInfo();
        var desiredBalance = desiredBalanceShardsAllocator != null ? desiredBalanceShardsAllocator.getDesiredBalance() : null;

        var stats = Maps.<String, NodeAllocationStats>newMapWithExpectedSize(state.getRoutingNodes().size());
        for (RoutingNode node : state.getRoutingNodes()) {
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
                IndexMetadata indexMetadata = state.metadata().getIndexSafe(shardRouting.index());
                if (isDesiredAllocation(desiredBalance, shardRouting) == false) {
                    undesiredShards++;
                }
                long shardSize = info.getShardSize(shardRouting.shardId(), shardRouting.primary(), 0);
                forecastedWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                forecastedDiskUsage += Math.max(indexMetadata.getForecastedShardSizeInBytes().orElse(0), shardSize);
                currentDiskUsage += shardSize;

            }
            stats.put(
                node.nodeId(),
                new NodeAllocationStats(
                    shards,
                    desiredBalanceShardsAllocator != null ? undesiredShards : -1,
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
