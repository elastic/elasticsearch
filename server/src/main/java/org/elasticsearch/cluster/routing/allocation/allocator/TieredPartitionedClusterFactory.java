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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING;

public class TieredPartitionedClusterFactory implements PartitionedClusterFactory {

    public static final Setting<Float> INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.shard.indexing",
        SHARD_BALANCE_FACTOR_SETTING,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.shard.search",
        SHARD_BALANCE_FACTOR_SETTING,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.write_load.indexing",
        WRITE_LOAD_BALANCE_FACTOR_SETTING,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final BalancerSettings balancerSettings;
    private volatile float indexingTierShardBalanceFactor;
    private volatile float searchTierShardBalanceFactor;
    private volatile float indexingTierWriteLoadBalanceFactor;

    public TieredPartitionedClusterFactory(BalancerSettings balancerSettings, ClusterSettings clusterSettings) {
        this.balancerSettings = balancerSettings;
        clusterSettings.initializeAndWatch(
            INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING,
            value -> this.indexingTierShardBalanceFactor = value
        );
        clusterSettings.initializeAndWatch(SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING, value -> this.searchTierShardBalanceFactor = value);
        clusterSettings.initializeAndWatch(
            INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            value -> this.indexingTierWriteLoadBalanceFactor = value
        );
    }

    @Override
    public PartitionedCluster create(
        WriteLoadForecaster writeLoadForecaster,
        ClusterInfo clusterInfo,
        Metadata metadata,
        RoutingNodes routingNodes
    ) {
        return new TieredPartitionedCluster(writeLoadForecaster, clusterInfo, metadata, routingNodes);
    }

    private class TieredPartitionedCluster implements PartitionedCluster {

        private final TieredClusterPartition searchPartition;
        private final TieredClusterPartition indexingPartition;
        private final float avgShardsPerNode;
        private final double avgWriteLoadPerNode;
        private final double avgDiskUsageInBytesPerNode;
        private final Metadata metadata;
        private final RoutingNodes routingNodes;

        private TieredPartitionedCluster(
            WriteLoadForecaster writeLoadForecaster,
            ClusterInfo clusterInfo,
            Metadata metadata,
            RoutingNodes routingNodes
        ) {
            this.metadata = metadata;
            this.routingNodes = routingNodes;
            avgShardsPerNode = WeightFunction.avgShardPerNode(metadata, routingNodes);
            avgWriteLoadPerNode = WeightFunction.avgWriteLoadPerNode(writeLoadForecaster, metadata, routingNodes);
            avgDiskUsageInBytesPerNode = WeightFunction.avgDiskUsageInBytesPerNode(clusterInfo, metadata, routingNodes);
            this.searchPartition = new TieredClusterPartition(searchTierShardBalanceFactor, 0.0f);
            this.indexingPartition = new TieredClusterPartition(indexingTierShardBalanceFactor, indexingTierWriteLoadBalanceFactor);
        }

        @Override
        public ClusterPartition partitionForShard(ShardRouting shard) {
            if (shard.role() == ShardRouting.Role.SEARCH_ONLY) {
                return searchPartition;
            } else if (shard.role() == ShardRouting.Role.INDEX_ONLY) {
                return indexingPartition;
            } else {
                throw new IllegalArgumentException("Unsupported shard role [" + shard.role() + "]");
            }
        }

        @Override
        public ClusterPartition partitionForNode(RoutingNode node) {
            Set<DiscoveryNodeRole> roles = node.node().getRoles();
            if (roles.contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                return searchPartition;
            } else if (roles.contains(DiscoveryNodeRole.INDEX_ROLE)) {
                return indexingPartition;
            } else {
                throw new IllegalArgumentException("Node is neither indexing or search node, roles = " + roles);
            }
        }

        @Override
        public PartitionedNodeSorter createPartitionedNodeSorter(
            BalancedShardsAllocator.ModelNode[] modelNodes,
            BalancedShardsAllocator.Balancer balancer
        ) {
            return new TieredPartitionedNodeSorter(modelNodes, balancer);
        }

        private IndexMetadata indexMetadata(BalancedShardsAllocator.ProjectIndex index) {
            return metadata.getProject(index.project()).index(index.indexName());
        }

        private class TieredPartitionedNodeSorter implements PartitionedNodeSorter {

            private final BalancedShardsAllocator.NodeSorter searchNodeSorter;
            private final BalancedShardsAllocator.NodeSorter indexingNodeSorter;

            TieredPartitionedNodeSorter(BalancedShardsAllocator.ModelNode[] allNodes, BalancedShardsAllocator.Balancer balancer) {
                final BalancedShardsAllocator.ModelNode[] searchNodes = Arrays.stream(allNodes)
                    .filter(n -> n.getRoutingNode().node().hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()))
                    .toArray(BalancedShardsAllocator.ModelNode[]::new);
                final BalancedShardsAllocator.ModelNode[] indexingNodes = Arrays.stream(allNodes)
                    .filter(n -> n.getRoutingNode().node().hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
                    .toArray(BalancedShardsAllocator.ModelNode[]::new);
                searchNodeSorter = new BalancedShardsAllocator.NodeSorter(searchNodes, searchPartition, balancer);
                indexingNodeSorter = new BalancedShardsAllocator.NodeSorter(indexingNodes, searchPartition, balancer);
            }

            @Override
            public Collection<BalancedShardsAllocator.NodeSorter> allNodeSorters() {
                return List.of(searchNodeSorter, indexingNodeSorter);
            }

            @Override
            public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                if (shard.role() == ShardRouting.Role.SEARCH_ONLY) {
                    return searchNodeSorter;
                } else if (shard.role() == ShardRouting.Role.INDEX_ONLY) {
                    return indexingNodeSorter;
                } else {
                    throw new IllegalArgumentException("Unsupported shard role [" + shard.role() + "]");
                }
            }
        }

        private class TieredClusterPartition implements ClusterPartition {

            private final WeightFunction weightFunction;

            private TieredClusterPartition(float shardBalanceFactor, float writeLoadBalanceFactor) {
                this.weightFunction = new WeightFunction(
                    this,
                    shardBalanceFactor,
                    balancerSettings.getIndexBalanceFactor(),
                    writeLoadBalanceFactor,
                    balancerSettings.getDiskUsageBalanceFactor()
                );
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
