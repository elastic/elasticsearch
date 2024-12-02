/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.index.shard.ShardId;

/**
 * This class is the primary weight function used to create balanced over nodes and shards in the cluster.
 * Currently this function has 3 properties:
 * <ul>
 * <li><code>index balance</code> - balance property over shards per index</li>
 * <li><code>shard balance</code> - balance property over shards per cluster</li>
 * </ul>
 * <p>
 * Each of these properties are expressed as factor such that the properties factor defines the relative
 * importance of the property for the weight function. For example if the weight function should calculate
 * the weights only based on a global (shard) balance the index balance can be set to {@code 0.0} and will
 * in turn have no effect on the distribution.
 * </p>
 * The weight per index is calculated based on the following formula:
 * <ul>
 * <li>
 * <code>weight<sub>index</sub>(node, index) = indexBalance * (node.numShards(index) - avgShardsPerNode(index))</code>
 * </li>
 * <li>
 * <code>weight<sub>node</sub>(node, index) = shardBalance * (node.numShards() - avgShardsPerNode)</code>
 * </li>
 * </ul>
 * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index)</code>
 */
public class WeightFunction {

    private final float theta0;
    private final float theta1;
    private final float theta2;
    private final float theta3;

    public WeightFunction(float shardBalance, float indexBalance, float writeLoadBalance, float diskUsageBalance) {
        float sum = shardBalance + indexBalance + writeLoadBalance + diskUsageBalance;
        if (sum <= 0.0f) {
            throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
        }
        theta0 = shardBalance / sum;
        theta1 = indexBalance / sum;
        theta2 = writeLoadBalance / sum;
        theta3 = diskUsageBalance / sum;
    }

    float weight(BalancedShardsAllocator.Balancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
        final float weightIndex = node.numShards(index) - balancer.avgShardsPerNode(index);
        final float nodeWeight = nodeWeight(
            node.numShards(),
            balancer.avgShardsPerNode(),
            node.writeLoad(),
            balancer.avgWriteLoadPerNode(),
            node.diskUsageInBytes(),
            balancer.avgDiskUsageInBytesPerNode()
        );
        return nodeWeight + theta1 * weightIndex;
    }

    public float nodeWeight(
        int nodeNumShards,
        float avgShardsPerNode,
        double nodeWriteLoad,
        double avgWriteLoadPerNode,
        double diskUsageInBytes,
        double avgDiskUsageInBytesPerNode
    ) {
        final float weightShard = nodeNumShards - avgShardsPerNode;
        final float ingestLoad = (float) (nodeWriteLoad - avgWriteLoadPerNode);
        final float diskUsage = (float) (diskUsageInBytes - avgDiskUsageInBytesPerNode);
        return theta0 * weightShard + theta2 * ingestLoad + theta3 * diskUsage;
    }

    float minWeightDelta(float shardWriteLoad, float shardSizeBytes) {
        return theta0 * 1 + theta1 * 1 + theta2 * shardWriteLoad + theta3 * shardSizeBytes;
    }

    public static float avgShardPerNode(Metadata metadata, RoutingNodes routingNodes) {
        return ((float) metadata.getTotalNumberOfShards()) / routingNodes.size();
    }

    public static double avgWriteLoadPerNode(WriteLoadForecaster writeLoadForecaster, Metadata metadata, RoutingNodes routingNodes) {
        return getTotalWriteLoad(writeLoadForecaster, metadata) / routingNodes.size();
    }

    public static double avgDiskUsageInBytesPerNode(ClusterInfo clusterInfo, Metadata metadata, RoutingNodes routingNodes) {
        return ((double) getTotalDiskUsageInBytes(clusterInfo, metadata) / routingNodes.size());
    }

    private static double getTotalWriteLoad(WriteLoadForecaster writeLoadForecaster, Metadata metadata) {
        double writeLoad = 0.0;
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            writeLoad += getIndexWriteLoad(writeLoadForecaster, indexMetadata);
        }
        return writeLoad;
    }

    private static double getIndexWriteLoad(WriteLoadForecaster writeLoadForecaster, IndexMetadata indexMetadata) {
        var shardWriteLoad = writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
        return shardWriteLoad * numberOfCopies(indexMetadata);
    }

    private static int numberOfCopies(IndexMetadata indexMetadata) {
        return indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas());
    }

    private static long getTotalDiskUsageInBytes(ClusterInfo clusterInfo, Metadata metadata) {
        long totalDiskUsageInBytes = 0;
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            totalDiskUsageInBytes += getIndexDiskUsageInBytes(clusterInfo, indexMetadata);
        }
        return totalDiskUsageInBytes;
    }

    // Visible for testing
    static long getIndexDiskUsageInBytes(ClusterInfo clusterInfo, IndexMetadata indexMetadata) {
        if (indexMetadata.ignoreDiskWatermarks()) {
            // disk watermarks are ignored for partial searchable snapshots
            // and is equivalent to indexMetadata.isPartialSearchableSnapshot()
            return 0;
        }
        final long forecastedShardSize = indexMetadata.getForecastedShardSizeInBytes().orElse(-1L);
        long totalSizeInBytes = 0;
        int shardCount = 0;
        for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), shard);
            final long primaryShardSize = Math.max(forecastedShardSize, clusterInfo.getShardSize(shardId, true, -1L));
            if (primaryShardSize != -1L) {
                totalSizeInBytes += primaryShardSize;
                shardCount++;
            }
            final long replicaShardSize = Math.max(forecastedShardSize, clusterInfo.getShardSize(shardId, false, -1L));
            if (replicaShardSize != -1L) {
                totalSizeInBytes += replicaShardSize * indexMetadata.getNumberOfReplicas();
                shardCount += indexMetadata.getNumberOfReplicas();
            }
        }
        if (shardCount == numberOfCopies(indexMetadata)) {
            return totalSizeInBytes;
        }
        return shardCount == 0 ? 0 : (totalSizeInBytes / shardCount) * numberOfCopies(indexMetadata);
    }
}
