/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeights;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.NodeSorters;
import org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.Stateless.STATELESS_SHARD_ROLES;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING;

public class StatelessBalancingWeightsFactory implements BalancingWeightsFactory {

    public static final Setting<Boolean> SEPARATE_WEIGHTS_PER_TIER_ENABLED_SETTING = Setting.boolSetting(
        "serverless.cluster.routing.allocation.balance.separate_weights_enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "serverless.cluster.routing.allocation.balance.shard.indexing_tier",
        SHARD_BALANCE_FACTOR_SETTING,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "serverless.cluster.routing.allocation.balance.shard.search_tier",
        SHARD_BALANCE_FACTOR_SETTING,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "serverless.cluster.routing.allocation.balance.write_load.indexing_tier",
        WRITE_LOAD_BALANCE_FACTOR_SETTING,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "serverless.cluster.routing.allocation.balance.write_load.search_tier",
        0.0f,
        0.0f,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final BalancerSettings balancerSettings;
    private volatile float indexingTierShardBalanceFactor;
    private volatile float searchTierShardBalanceFactor;
    private volatile float indexingTierWriteLoadBalanceFactor;
    private volatile float searchTierWriteLoadBalanceFactor;

    public StatelessBalancingWeightsFactory(BalancerSettings balancerSettings, ClusterSettings clusterSettings) {
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
        clusterSettings.initializeAndWatch(
            SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            value -> this.searchTierWriteLoadBalanceFactor = value
        );
    }

    @Override
    public BalancingWeights create() {
        return new StatelessBalancingWeights();
    }

    private class StatelessBalancingWeights implements BalancingWeights {

        private final WeightFunction searchWeightFunction;
        private final WeightFunction indexingWeightFunction;
        private final boolean diskUsageIgnored;

        private StatelessBalancingWeights() {
            final float diskUsageBalanceFactor = balancerSettings.getDiskUsageBalanceFactor();
            final float indexBalanceFactor = balancerSettings.getIndexBalanceFactor();
            this.searchWeightFunction = new WeightFunction(
                searchTierShardBalanceFactor,
                indexBalanceFactor,
                searchTierWriteLoadBalanceFactor,
                diskUsageBalanceFactor
            );
            this.indexingWeightFunction = new WeightFunction(
                indexingTierShardBalanceFactor,
                indexBalanceFactor,
                indexingTierWriteLoadBalanceFactor,
                diskUsageBalanceFactor
            );
            this.diskUsageIgnored = diskUsageBalanceFactor == 0;
        }

        @Override
        public WeightFunction weightFunctionForShard(ShardRouting shard) {
            assert STATELESS_SHARD_ROLES.contains(shard.role()) : "Non-stateless role encountered [" + shard.role() + "]";
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
            assert roles.stream().filter(r -> r == DiscoveryNodeRole.INDEX_ROLE || r == DiscoveryNodeRole.SEARCH_ROLE).count() == 1
                : "Node has both SEARCH and INDEX role";
            if (roles.contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                return searchWeightFunction;
            } else if (roles.contains(DiscoveryNodeRole.INDEX_ROLE)) {
                return indexingWeightFunction;
            } else {
                throw new IllegalArgumentException("Node is neither indexing or search node, roles = " + roles);
            }
        }

        @Override
        public NodeSorters createNodeSorters(BalancedShardsAllocator.ModelNode[] modelNodes, BalancedShardsAllocator.Balancer balancer) {
            return new ServerlessNodeSorters(modelNodes, balancer);
        }

        @Override
        public boolean diskUsageIgnored() {
            return diskUsageIgnored;
        }

        private class ServerlessNodeSorters implements NodeSorters {

            private final BalancedShardsAllocator.NodeSorter searchNodeSorter;
            private final BalancedShardsAllocator.NodeSorter indexingNodeSorter;

            ServerlessNodeSorters(BalancedShardsAllocator.ModelNode[] allNodes, BalancedShardsAllocator.Balancer balancer) {
                final BalancedShardsAllocator.ModelNode[] searchNodes = Arrays.stream(allNodes)
                    .filter(n -> n.getRoutingNode().node().getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
                    .toArray(BalancedShardsAllocator.ModelNode[]::new);
                final BalancedShardsAllocator.ModelNode[] indexingNodes = Arrays.stream(allNodes)
                    .filter(n -> n.getRoutingNode().node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
                    .toArray(BalancedShardsAllocator.ModelNode[]::new);
                assert nodePartitionsAreDisjointAndUnionToAll(allNodes, searchNodes, indexingNodes);
                searchNodeSorter = new BalancedShardsAllocator.NodeSorter(searchNodes, searchWeightFunction, balancer);
                indexingNodeSorter = new BalancedShardsAllocator.NodeSorter(indexingNodes, indexingWeightFunction, balancer);
            }

            @Override
            public Iterator<BalancedShardsAllocator.NodeSorter> iterator() {
                return List.of(indexingNodeSorter, searchNodeSorter).iterator();
            }

            @Override
            public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                assert STATELESS_SHARD_ROLES.contains(shard.role()) : "Non-stateless role encountered [" + shard.role() + "]";
                if (shard.role() == ShardRouting.Role.SEARCH_ONLY) {
                    return searchNodeSorter;
                } else if (shard.role() == ShardRouting.Role.INDEX_ONLY) {
                    return indexingNodeSorter;
                } else {
                    throw new IllegalArgumentException("Unsupported shard role [" + shard.role() + "]");
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
}
