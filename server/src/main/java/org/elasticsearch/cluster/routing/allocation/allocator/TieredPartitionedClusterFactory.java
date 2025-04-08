/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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
    public PartitionedCluster create() {
        return new TieredPartitionedCluster();
    }

    private class TieredPartitionedCluster implements PartitionedCluster {

        private final WeightFunction searchWeightFunction;
        private final WeightFunction indexingWeightFunction;

        private TieredPartitionedCluster() {
            this.searchWeightFunction = new WeightFunction(
                searchTierShardBalanceFactor,
                balancerSettings.getIndexBalanceFactor(),
                0.0f,
                balancerSettings.getDiskUsageBalanceFactor()
            );
            this.indexingWeightFunction = new WeightFunction(
                indexingTierShardBalanceFactor,
                balancerSettings.getIndexBalanceFactor(),
                indexingTierWriteLoadBalanceFactor,
                balancerSettings.getDiskUsageBalanceFactor()
            );
        }

        @Override
        public WeightFunction weightFunctionForShard(ShardRouting shard) {
            if (shard.role() == ShardRouting.Role.SEARCH_ONLY) {
                return searchWeightFunction;
            } else if (shard.role() == ShardRouting.Role.INDEX_ONLY) {
                return indexingWeightFunction;
            } else {
                throw new IllegalArgumentException("Unsupported shard role [" + shard.role() + "]");
            }
        }

        @Override
        public WeightFunction weightFunctionForNode(RoutingNode node) {
            Set<DiscoveryNodeRole> roles = node.node().getRoles();
            if (roles.contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                return searchWeightFunction;
            } else if (roles.contains(DiscoveryNodeRole.INDEX_ROLE)) {
                return indexingWeightFunction;
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

        private class TieredPartitionedNodeSorter implements PartitionedNodeSorter {

            private final BalancedShardsAllocator.NodeSorter searchNodeSorter;
            private final BalancedShardsAllocator.NodeSorter indexingNodeSorter;
            private final List<BalancedShardsAllocator.NodeSorter> allNodeSorters;

            TieredPartitionedNodeSorter(BalancedShardsAllocator.ModelNode[] allNodes, BalancedShardsAllocator.Balancer balancer) {
                final BalancedShardsAllocator.ModelNode[] searchNodes = Arrays.stream(allNodes)
                    .filter(n -> n.getRoutingNode().node().getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
                    .toArray(BalancedShardsAllocator.ModelNode[]::new);
                final BalancedShardsAllocator.ModelNode[] indexingNodes = Arrays.stream(allNodes)
                    .filter(n -> n.getRoutingNode().node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
                    .toArray(BalancedShardsAllocator.ModelNode[]::new);
                assert nodePartitionsAreDisjointAndUnionToAll(allNodes, searchNodes, indexingNodes);
                searchNodeSorter = new BalancedShardsAllocator.NodeSorter(searchNodes, searchWeightFunction, balancer);
                indexingNodeSorter = new BalancedShardsAllocator.NodeSorter(indexingNodes, indexingWeightFunction, balancer);
                allNodeSorters = List.of(indexingNodeSorter, searchNodeSorter);
            }

            @Override
            public Collection<BalancedShardsAllocator.NodeSorter> allNodeSorters() {
                return allNodeSorters;
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

        private static boolean nodePartitionsAreDisjointAndUnionToAll(
            BalancedShardsAllocator.ModelNode[] allNodes,
            BalancedShardsAllocator.ModelNode[] searchNodes,
            BalancedShardsAllocator.ModelNode[] indexingNodes
        ) {
            return allNodes.length == searchNodes.length + indexingNodes.length
                && Stream.concat(Arrays.stream(searchNodes), Arrays.stream(indexingNodes))
                    .map(BalancedShardsAllocator.ModelNode::getNodeId)
                    .distinct()
                    .count() == allNodes.length;
        }
    }
}
