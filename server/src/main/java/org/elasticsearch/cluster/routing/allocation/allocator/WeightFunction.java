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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.index.shard.ShardId;

public interface WeightFunction {

    float calculateNodeWeightWithIndex(
        BalancedShardsAllocator.Balancer balancer,
        BalancedShardsAllocator.ModelNode node,
        BalancedShardsAllocator.ProjectIndex index
    );

    float calculateNodeWeight(
        RoutingNode node,
        int nodeNumShards,
        float avgShardsPerNode,
        double nodeWriteLoad,
        double avgWriteLoadPerNode,
        double diskUsageInBytes,
        double avgDiskUsageInBytesPerNode
    );

    float minWeightDelta(BalancedShardsAllocator.ModelNode targetNode, float shardWriteLoad, float shardSizeBytes);

    static float avgShardPerNode(Metadata metadata, RoutingNodes routingNodes) {
        return ((float) metadata.getTotalNumberOfShards()) / routingNodes.size();
    }

    static double avgWriteLoadPerNode(WriteLoadForecaster writeLoadForecaster, Metadata metadata, RoutingNodes routingNodes) {
        return getTotalWriteLoad(writeLoadForecaster, metadata) / routingNodes.size();
    }

    static double avgDiskUsageInBytesPerNode(ClusterInfo clusterInfo, Metadata metadata, RoutingNodes routingNodes) {
        return ((double) getTotalDiskUsageInBytes(clusterInfo, metadata) / routingNodes.size());
    }

    private static double getTotalWriteLoad(WriteLoadForecaster writeLoadForecaster, Metadata metadata) {
        double writeLoad = 0.0;
        for (ProjectMetadata project : metadata.projects().values()) {
            for (IndexMetadata indexMetadata : project.indices().values()) {
                writeLoad += getIndexWriteLoad(writeLoadForecaster, indexMetadata);
            }
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
        for (ProjectMetadata project : metadata.projects().values()) {
            for (IndexMetadata indexMetadata : project.indices().values()) {
                totalDiskUsageInBytes += getIndexDiskUsageInBytes(clusterInfo, indexMetadata);
            }
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
