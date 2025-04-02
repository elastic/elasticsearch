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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING;

/**
 * Creates {@link PartitionedNodeSorter}s that partition the nodes and shards
 * by serverless search/indexing tiers.
 * <p>
 * This would ultimately live in serverless project.
 */
public class TieredNodeSorterFactory implements NodeSorterFactory {

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
    private final ClusterSettings clusterSettings;

    public TieredNodeSorterFactory(BalancerSettings balancerSettings, ClusterSettings clusterSettings) {
        this.balancerSettings = balancerSettings;
        this.clusterSettings = clusterSettings;
    }

    @Override
    public PartitionedNodeSorter create(BalancedShardsAllocator.ModelNode[] allNodes, BalancedShardsAllocator.Balancer balancer) {
        return new TieredNodeSorter(balancerSettings, allNodes, clusterSettings, balancer);
    }

    private static class TieredNodeSorter implements PartitionedNodeSorter {

        private final BalancedShardsAllocator.NodeSorter searchNodeSorter;
        private final BalancedShardsAllocator.NodeSorter indexingNodeSorter;

        private TieredNodeSorter(
            BalancerSettings balancerSettings,
            BalancedShardsAllocator.ModelNode[] allNodes,
            ClusterSettings clusterSettings,
            BalancedShardsAllocator.Balancer balancer
        ) {
            final BalancedShardsAllocator.ModelNode[] searchNodes = Arrays.stream(allNodes)
                .filter(n -> n.getRoutingNode().node().hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()))
                .toArray(BalancedShardsAllocator.ModelNode[]::new);
            final BalancedShardsAllocator.ModelNode[] indexingNodes = Arrays.stream(allNodes)
                .filter(n -> n.getRoutingNode().node().hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
                .toArray(BalancedShardsAllocator.ModelNode[]::new);
            assert nodePartitionsAreDisjointAndUnionToAll(allNodes, searchNodes, indexingNodes);

            float indexingTierWriteLoadFactor = clusterSettings.get(INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING);
            float searchTierShardBalanceFactor = clusterSettings.get(SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING);
            float indexingTierShardBalanceFactor = clusterSettings.get(INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING);
            this.searchNodeSorter = new BalancedShardsAllocator.NodeSorter(
                searchNodes,
                new WeightFunction(
                    searchTierShardBalanceFactor,
                    balancerSettings.getIndexBalanceFactor(),
                    0.0f,
                    balancerSettings.getDiskUsageBalanceFactor()
                ),
                balancer
            );
            this.indexingNodeSorter = new BalancedShardsAllocator.NodeSorter(
                indexingNodes,
                new WeightFunction(
                    indexingTierShardBalanceFactor,
                    balancerSettings.getIndexBalanceFactor(),
                    indexingTierWriteLoadFactor,
                    balancerSettings.getDiskUsageBalanceFactor()
                ),
                balancer
            );
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
                throw new IllegalStateException("Shard is neither search or indexing shard " + shard.role());
            }
        }

        @Override
        public BalancedShardsAllocator.NodeSorter sorterForNode(BalancedShardsAllocator.ModelNode node) {
            if (node.getRoutingNode().node().hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName())) {
                return searchNodeSorter;
            } else if (node.getRoutingNode().node().hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName())) {
                return indexingNodeSorter;
            } else {
                throw new IllegalStateException("Node has neither search or indexing role " + node);
            }
        }

        private boolean nodePartitionsAreDisjointAndUnionToAll(
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
