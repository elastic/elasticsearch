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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Calculates the allocation weights and usage stats for each node: see {@link NodeAllocationStatsAndWeight} for details.
 */
public class NodeAllocationStatsAndWeightsCalculator {
    private final WriteLoadForecaster writeLoadForecaster;

    private volatile float indexBalanceFactor;
    private volatile float shardBalanceFactor;
    private volatile float writeLoadBalanceFactor;
    private volatile float diskUsageBalanceFactor;

    /**
     * Node shard allocation stats and the total node weight.
     */
    public record NodeAllocationStatsAndWeight(
        int shards,
        int undesiredShards,
        double forecastedIngestLoad,
        long forecastedDiskUsage,
        long currentDiskUsage,
        float currentNodeWeight
    ) {}

    public NodeAllocationStatsAndWeightsCalculator(WriteLoadForecaster writeLoadForecaster, ClusterSettings clusterSettings) {
        this.writeLoadForecaster = writeLoadForecaster;
        clusterSettings.initializeAndWatch(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, value -> this.shardBalanceFactor = value);
        clusterSettings.initializeAndWatch(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, value -> this.indexBalanceFactor = value);
        clusterSettings.initializeAndWatch(
            BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING,
            value -> this.writeLoadBalanceFactor = value
        );
        clusterSettings.initializeAndWatch(
            BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING,
            value -> this.diskUsageBalanceFactor = value
        );
    }

    /**
     * Returns a map of node IDs to {@link NodeAllocationStatsAndWeight}.
     */
    public Map<String, NodeAllocationStatsAndWeight> nodesAllocationStatsAndWeights(
        Metadata metadata,
        RoutingNodes routingNodes,
        ClusterInfo clusterInfo,
        @Nullable DesiredBalance desiredBalance
    ) {
        var weightFunction = new WeightFunction(shardBalanceFactor, indexBalanceFactor, writeLoadBalanceFactor, diskUsageBalanceFactor);
        var avgShardsPerNode = WeightFunction.avgShardPerNode(metadata, routingNodes);
        var avgWriteLoadPerNode = WeightFunction.avgWriteLoadPerNode(writeLoadForecaster, metadata, routingNodes);
        var avgDiskUsageInBytesPerNode = WeightFunction.avgDiskUsageInBytesPerNode(clusterInfo, metadata, routingNodes);

        var nodeAllocationStatsAndWeights = Maps.<String, NodeAllocationStatsAndWeight>newMapWithExpectedSize(routingNodes.size());
        for (RoutingNode node : routingNodes) {
            int shards = 0;
            int undesiredShards = 0;
            double forecastedWriteLoad = 0.0;
            long forecastedDiskUsage = 0;
            long currentDiskUsage = 0;
            for (ShardRouting shardRouting : node) {
                if (shardRouting.relocating()) {
                    // Skip the shard if it is moving off this node. The node running recovery will count it.
                    continue;
                }
                ++shards;
                IndexMetadata indexMetadata = metadata.getIndexSafe(shardRouting.index());
                if (isDesiredAllocation(desiredBalance, shardRouting) == false) {
                    undesiredShards++;
                }
                long shardSize = clusterInfo.getShardSize(shardRouting.shardId(), shardRouting.primary(), 0);
                forecastedWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                forecastedDiskUsage += Math.max(indexMetadata.getForecastedShardSizeInBytes().orElse(0), shardSize);
                currentDiskUsage += shardSize;
            }
            float currentNodeWeight = weightFunction.calculateNodeWeight(
                shards,
                avgShardsPerNode,
                forecastedWriteLoad,
                avgWriteLoadPerNode,
                currentDiskUsage,
                avgDiskUsageInBytesPerNode
            );
            nodeAllocationStatsAndWeights.put(
                node.nodeId(),
                new NodeAllocationStatsAndWeight(
                    shards,
                    // It's part of a public API contract for an 'undesired_shards' field that -1 will be returned if an allocator other
                    // than the desired balance allocator is used.
                    desiredBalance != null ? undesiredShards : -1,
                    forecastedWriteLoad,
                    forecastedDiskUsage,
                    currentDiskUsage,
                    currentNodeWeight
                )
            );
        }

        return nodeAllocationStatsAndWeights;
    }

    /**
     * Checks whether a shard is currently allocated to a node that is wanted by the desired balance decision.
     */
    private static boolean isDesiredAllocation(@Nullable DesiredBalance desiredBalance, ShardRouting shardRouting) {
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
