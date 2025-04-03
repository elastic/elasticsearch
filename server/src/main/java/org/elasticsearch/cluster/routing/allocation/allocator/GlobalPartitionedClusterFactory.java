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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;

import java.util.Collection;
import java.util.List;

public class GlobalPartitionedClusterFactory implements PartitionedClusterFactory {

    private final BalancerSettings balancerSettings;

    public GlobalPartitionedClusterFactory(BalancerSettings balancerSettings) {
        this.balancerSettings = balancerSettings;
    }

    @Override
    public PartitionedCluster create(
        WriteLoadForecaster writeLoadForecaster,
        ClusterInfo clusterInfo,
        Metadata metadata,
        RoutingNodes routingNodes
    ) {
        return new GlobalPartitionedCluster(writeLoadForecaster, clusterInfo, metadata, routingNodes);
    }

    private class GlobalPartitionedCluster implements PartitionedCluster {

        private final Metadata metadata;
        private final RoutingNodes routingNodes;
        private final GlobalClusterPartition thePartition;

        GlobalPartitionedCluster(
            WriteLoadForecaster writeLoadForecaster,
            ClusterInfo clusterInfo,
            Metadata metadata,
            RoutingNodes routingNodes
        ) {
            this.metadata = metadata;
            this.routingNodes = routingNodes;
            float avgShardPerNode = WeightFunction.avgShardPerNode(metadata, routingNodes);
            double avgWriteLoadPerNode = WeightFunction.avgWriteLoadPerNode(writeLoadForecaster, metadata, routingNodes);
            double avgDiskUsageInBytesPerNode = WeightFunction.avgDiskUsageInBytesPerNode(clusterInfo, metadata, routingNodes);
            this.thePartition = new GlobalClusterPartition(
                balancerSettings,
                avgShardPerNode,
                avgWriteLoadPerNode,
                avgDiskUsageInBytesPerNode
            );
        }

        @Override
        public ClusterPartition partitionForShard(ShardRouting shard) {
            return thePartition;
        }

        @Override
        public ClusterPartition partitionForNode(RoutingNode node) {
            return thePartition;
        }

        private IndexMetadata indexMetadata(BalancedShardsAllocator.ProjectIndex index) {
            return metadata.getProject(index.project()).index(index.indexName());
        }

        public PartitionedNodeSorter createPartitionedNodeSorter(
            BalancedShardsAllocator.ModelNode[] modelNodes,
            BalancedShardsAllocator.Balancer balancer
        ) {
            return new GlobalPartitionedNodeSorter(new BalancedShardsAllocator.NodeSorter(modelNodes, thePartition, balancer));
        }

        private static class GlobalPartitionedNodeSorter implements PartitionedNodeSorter {

            private final BalancedShardsAllocator.NodeSorter nodeSorter;

            private GlobalPartitionedNodeSorter(BalancedShardsAllocator.NodeSorter nodeSorter) {
                this.nodeSorter = nodeSorter;
            }

            @Override
            public Collection<BalancedShardsAllocator.NodeSorter> allNodeSorters() {
                return List.of(nodeSorter);
            }

            @Override
            public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                return nodeSorter;
            }
        }

        private class GlobalClusterPartition implements ClusterPartition {

            private final WeightFunction weightFunction;
            private final float avgShardsPerNode;
            private final double avgWriteLoadPerNode;
            private final double avgDiskUsageInBytesPerNode;

            private GlobalClusterPartition(
                BalancerSettings balancerSettings,
                float avgShardsPerNode,
                double avgWriteLoadPerNode,
                double avgDiskUsageInBytesPerNode
            ) {
                this.weightFunction = new WeightFunction(
                    this,
                    balancerSettings.getShardBalanceFactor(),
                    balancerSettings.getIndexBalanceFactor(),
                    balancerSettings.getWriteLoadBalanceFactor(),
                    balancerSettings.getDiskUsageBalanceFactor()
                );
                this.avgShardsPerNode = avgShardsPerNode;
                this.avgWriteLoadPerNode = avgWriteLoadPerNode;
                this.avgDiskUsageInBytesPerNode = avgDiskUsageInBytesPerNode;
            }

            @Override
            public WeightFunction weightFunction() {
                return weightFunction;
            }

            @Override
            public float avgShardsPerNode(BalancedShardsAllocator.ProjectIndex index) {
                return ((float) indexMetadata(index).getTotalNumberOfShards()) / routingNodes.size();
            }

            @Override
            public float avgShardsPerNode() {
                return avgShardsPerNode;
            }

            @Override
            public double avgWriteLoadPerNode() {
                return avgWriteLoadPerNode;
            }

            @Override
            public double avgDiskUsageInBytesPerNode() {
                return avgDiskUsageInBytesPerNode;
            }
        }
    }
}
