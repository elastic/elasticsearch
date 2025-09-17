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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StatelessBalancingWeightsFactoryTests extends ESAllocationTestCase {

    private static final Set<DiscoveryNodeRole> INDEXING_ROLES = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE);
    private static final Set<DiscoveryNodeRole> SEARCH_ROLES = Set.of(DiscoveryNodeRole.SEARCH_ROLE);

    public void testWriteLoadIsSettablePerTier() {
        var clusterSettings = statelessBalancingSettings(Settings.EMPTY);
        var balancerSettings = new BalancerSettings(clusterSettings);
        var allocationService = new ESAllocationTestCase.MockAllocationService(
            statelessAllocationDeciders(),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(
                balancerSettings,
                TEST_WRITE_LOAD_FORECASTER,
                new StatelessBalancingWeightsFactory(balancerSettings, clusterSettings)
            ),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        final StatelessTier tierToSetToZero = randomFrom(StatelessTier.values());
        clusterSettings.applySettings(
            Settings.builder()
                .put(tierToSetToZero.writeLoadBalanceSetting.getKey(), 0.0)
                .put(tierToSetToZero.other().writeLoadBalanceSetting.getKey(), 10.0)
                .build()
        );
        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                randomFrom(InitialState.values()),
                aStatelessIndex("heavy-index").indexWriteLoadForecast(8.0),
                aStatelessIndex("light-index-1").indexWriteLoadForecast(1.0),
                aStatelessIndex("light-index-2").indexWriteLoadForecast(2.0),
                aStatelessIndex("light-index-3").indexWriteLoadForecast(3.0),
                aStatelessIndex("zero-write-load-index").indexWriteLoadForecast(0.0),
                aStatelessIndex("no-write-load-index")
            ),
            allocationService
        );

        Map<String, Set<String>> shardsPerNode = getShardsPerNode(clusterState);
        // The tier not accounting for write load should be evenly distributed
        tierToSetToZero.includedNodes.forEach(nodeId -> assertThat(shardsPerNode.get(nodeId), hasSize(3)));
        // The tier still accounting for write load should be skewed by the forecasted write loads
        assertThat(
            tierToSetToZero.other().includedNodes.stream().map(shardsPerNode::get).collect(Collectors.toSet()),
            equalTo(
                Set.of(
                    Set.of("heavy-index"),
                    Set.of("light-index-1", "light-index-2", "light-index-3", "no-write-load-index", "zero-write-load-index")
                )
            )
        );
    }

    public void testShardCountWeightIsSettablePerTier() {
        var clusterSettings = statelessBalancingSettings(Settings.EMPTY);
        var balancerSettings = new BalancerSettings(clusterSettings);
        var allocationService = new ESAllocationTestCase.MockAllocationService(
            statelessAllocationDeciders(),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(
                balancerSettings,
                TEST_WRITE_LOAD_FORECASTER,
                new StatelessBalancingWeightsFactory(balancerSettings, clusterSettings)
            ),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        final StatelessTier tierToSetToZero = randomFrom(StatelessTier.values());
        clusterSettings.applySettings(Settings.builder().put(tierToSetToZero.shardBalanceSetting.getKey(), 0.0).build());
        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                InitialState.ALL_ON_ONE,
                aStatelessIndex("index-1"),
                aStatelessIndex("index-2"),
                aStatelessIndex("index-3"),
                aStatelessIndex("index-4")
            ),
            allocationService
        );

        Map<String, Set<String>> shardsPerNode = getShardsPerNode(clusterState);
        // The tier that we zeroed the shard balance factor for should remain unbalanced
        assertThat(tierToSetToZero.shardCounts(shardsPerNode), equalTo(List.of(0, 4)));
        // The shard count of the other tier should be balanced
        assertThat(tierToSetToZero.other().shardCounts(shardsPerNode), equalTo(List.of(2, 2)));
    }

    private enum StatelessTier {
        INDEXING(
            StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            Set.of("indexing-1", "indexing-2")
        ),
        SEARCH(
            StatelessBalancingWeightsFactory.SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            Set.of("search-1", "search-2")
        );

        private final Setting<?> shardBalanceSetting;
        private final Setting<?> writeLoadBalanceSetting;
        private final Set<String> includedNodes;

        StatelessTier(Setting<?> shardBalanceSetting, Setting<?> writeLoadBalanceSetting, Set<String> includedNodes) {
            this.shardBalanceSetting = shardBalanceSetting;
            this.writeLoadBalanceSetting = writeLoadBalanceSetting;
            this.includedNodes = includedNodes;
        }

        public List<Integer> shardCounts(Map<String, Set<String>> shardsPerNode) {
            return includedNodes.stream().map(shardsPerNode::get).map(Set::size).sorted().collect(Collectors.toList());
        }

        public StatelessTier other() {
            return this == INDEXING ? SEARCH : INDEXING;
        }
    }

    private AllocationDeciders statelessAllocationDeciders() {
        return new AllocationDeciders(List.of(new StatelessAllocationDecider(), new ReplicaAfterPrimaryActiveAllocationDecider()));
    }

    private ClusterSettings statelessBalancingSettings(Settings settings) {
        HashSet<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING);
        return new ClusterSettings(settings, allSettings);
    }

    private static ClusterState createStateWithIndices(InitialState initialState, IndexMetadata.Builder... indexMetadataBuilders) {
        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder(new StatelessShardRoutingRoleStrategy());
        initialState.buildRoutingTable(metadataBuilder, routingTableBuilder, indexMetadataBuilders);
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newIndexingNode("indexing-1"))
                    .add(newIndexingNode("indexing-2"))
                    .add(newSearchNode("search-1"))
                    .add(newSearchNode("search-2"))
            )
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    enum InitialState {
        /**
         * All shards are unassigned. To test the effect of balancing new shards
         */
        UNASSIGNED {
            @Override
            void buildRoutingTable(
                Metadata.Builder metadataBuilder,
                RoutingTable.Builder routingTableBuilder,
                IndexMetadata.Builder... indexMetadataBuilders
            ) {
                // allocate all shards from scratch
                for (var index : indexMetadataBuilders) {
                    var indexMetadata = index.build();
                    metadataBuilder.put(indexMetadata, false);
                    routingTableBuilder.addAsNew(indexMetadata);
                }
            }
        },

        /**
         * Start with all the shards on one node in the relevant tier. To test the effect of balancing on an unbalanced cluster
         */
        ALL_ON_ONE {
            @Override
            void buildRoutingTable(
                Metadata.Builder metadataBuilder,
                RoutingTable.Builder routingTableBuilder,
                IndexMetadata.Builder... indexMetadataBuilders
            ) {
                for (var index : indexMetadataBuilders) {
                    var indexingAllocationId = UUIDs.randomBase64UUID();
                    var searchAllocationId = UUIDs.randomBase64UUID();
                    var indexMetadata = index.putInSyncAllocationIds(0, Set.of(indexingAllocationId, searchAllocationId)).build();
                    metadataBuilder.put(indexMetadata, false);
                    routingTableBuilder.add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(
                                shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "indexing-1", true, ShardRoutingState.STARTED)
                                    .withRole(ShardRouting.Role.INDEX_ONLY)
                                    .withAllocationId(AllocationId.newInitializing(indexingAllocationId))
                                    .build()
                            )
                            .addShard(
                                shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "search-1", false, ShardRoutingState.STARTED)
                                    .withRole(ShardRouting.Role.SEARCH_ONLY)
                                    .withAllocationId(AllocationId.newInitializing(searchAllocationId))
                                    .build()
                            )
                    );
                }
            }
        };

        /**
         * Build the routing table
         */
        abstract void buildRoutingTable(
            Metadata.Builder metadataBuilder,
            RoutingTable.Builder routingTableBuilder,
            IndexMetadata.Builder... indexMetadata
        );
    }

    private static IndexMetadata.Builder aStatelessIndex(String name) {
        Settings.Builder settings = indexSettings(IndexVersion.current(), 1, 1);
        return IndexMetadata.builder(name).settings(settings);
    }

    private static DiscoveryNode newSearchNode(String nodeName) {
        return newNode(nodeName, SEARCH_ROLES);
    }

    private static DiscoveryNode newIndexingNode(String nodeName) {
        return newNode(nodeName, INDEXING_ROLES);
    }

    private static Map<String, Set<String>> getShardsPerNode(ClusterState clusterState) {
        return getPerNode(clusterState, mapping(ShardRouting::getIndexName, toSet()));
    }

    private static <T> Map<String, T> getPerNode(ClusterState clusterState, Collector<ShardRouting, ?, T> collector) {
        return clusterState.getRoutingNodes()
            .stream()
            .collect(Collectors.toMap(RoutingNode::nodeId, it -> StreamSupport.stream(it.spliterator(), false).collect(collector)));
    }
}
