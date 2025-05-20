/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Allocation;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class EnableAllocationTests extends ESAllocationTestCase {

    public void testClusterEnableNone() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name()).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        routingTable = strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

    }

    public void testClusterEnableOnlyPrimaries() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.PRIMARIES.name()).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        routingTable = strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
    }

    public void testIndexEnableNone() {
        AllocationService strategy = createAllocationService(Settings.builder().build());

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("disabled")
                    .settings(
                        settings(IndexVersion.current()).put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name())
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .put(IndexMetadata.builder("enabled").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("disabled"))
            .addAsNew(metadata.getProject().index("enabled"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));
        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> verify only enabled index has been routed");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), "enabled", STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), "disabled", STARTED).size(), equalTo(0));
    }

    public void testEnableClusterBalance() {
        final boolean useClusterSetting = randomBoolean();
        final Rebalance allowedOnes = RandomPicks.randomFrom(random(), EnumSet.of(Rebalance.PRIMARIES, Rebalance.REPLICAS, Rebalance.ALL));
        Settings build = Settings.builder()
            .put(
                CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                // index settings override cluster settings
                useClusterSetting ? Rebalance.NONE : RandomPicks.randomFrom(random(), Rebalance.values())
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 3)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(build, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationService strategy = createAllocationService(build, clusterSettings);
        Settings indexSettings = useClusterSetting
            ? Settings.EMPTY
            : Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE).build();

        logger.info("Building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(indexSettings))
                    .numberOfShards(3)
                    .numberOfReplicas(1)
            )
            .put(
                IndexMetadata.builder("always_disabled")
                    .settings(
                        settings(IndexVersion.current()).put(
                            EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                            Rebalance.NONE
                        )
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .addAsNew(metadata.getProject().index("always_disabled"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(4));
        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(4));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(4));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(0));

        if (useClusterSetting) {
            clusterState = ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(clusterState.metadata())
                        .transientSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), allowedOnes).build())
                )
                .build();
        } else {
            IndexMetadata meta = clusterState.getMetadata().getProject().index("test");
            IndexMetadata meta1 = clusterState.getMetadata().getProject().index("always_disabled");
            clusterState = ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(clusterState.metadata())
                        .removeAllIndices()
                        .put(IndexMetadata.builder(meta1))
                        .put(
                            IndexMetadata.builder(meta)
                                .settings(
                                    Settings.builder()
                                        .put(meta.getSettings())
                                        .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), allowedOnes)
                                        .build()
                                )
                        )
                )
                .build();

        }
        clusterSettings.applySettings(clusterState.metadata().settings());
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(
            "expected 6 shards to be started 2 to relocate useClusterSettings: " + useClusterSetting,
            shardsWithState(clusterState.getRoutingNodes(), STARTED).size(),
            equalTo(6)
        );
        assertThat(
            "expected 2 shards to relocate useClusterSettings: " + useClusterSetting,
            shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(),
            equalTo(2)
        );
        List<ShardRouting> mutableShardRoutings = shardsWithState(clusterState.getRoutingNodes(), RELOCATING);
        switch (allowedOnes) {
            case PRIMARIES:
                for (ShardRouting routing : mutableShardRoutings) {
                    assertTrue("only primaries are allowed to relocate", routing.primary());
                    assertThat("only test index can rebalance", routing.getIndexName(), equalTo("test"));
                }
                break;
            case REPLICAS:
                for (ShardRouting routing : mutableShardRoutings) {
                    assertFalse("only replicas are allowed to relocate", routing.primary());
                    assertThat("only test index can rebalance", routing.getIndexName(), equalTo("test"));
                }
                break;
            case ALL:
                for (ShardRouting routing : mutableShardRoutings) {
                    assertThat("only test index can rebalance", routing.getIndexName(), equalTo("test"));
                }
                break;
            default:
                fail("only replicas, primaries or all are allowed");
        }
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

    }

    public void testEnableClusterBalanceNoReplicas() {
        final boolean useClusterSetting = randomBoolean();
        Settings build = Settings.builder()
            .put(
                CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                // index settings override cluster settings
                useClusterSetting ? Rebalance.NONE : RandomPicks.randomFrom(random(), Rebalance.values())
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 3)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(build, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationService strategy = createAllocationService(build, clusterSettings);
        Settings indexSettings = useClusterSetting
            ? Settings.EMPTY
            : Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE).build();

        logger.info("Building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(indexSettings))
                    .numberOfShards(6)
                    .numberOfReplicas(0)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(6));
        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(6));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(6));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(0));
        metadata = clusterState.metadata();
        if (useClusterSetting) {
            clusterState = ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(metadata)
                        .transientSettings(
                            Settings.builder()
                                .put(
                                    CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                                    randomBoolean() ? Rebalance.PRIMARIES : Rebalance.ALL
                                )
                                .build()
                        )
                )
                .build();
        } else {
            IndexMetadata meta = clusterState.getMetadata().getProject().index("test");
            clusterState = ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(metadata)
                        .removeAllIndices()
                        .put(
                            IndexMetadata.builder(meta)
                                .settings(
                                    Settings.builder()
                                        .put(meta.getSettings())
                                        .put(
                                            EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                                            randomBoolean() ? Rebalance.PRIMARIES : Rebalance.ALL
                                        )
                                        .build()
                                )
                        )
                )
                .build();
        }
        clusterSettings.applySettings(clusterState.metadata().settings());
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(
            "expected 4 primaries to be started and 2 to relocate useClusterSettings: " + useClusterSetting,
            shardsWithState(clusterState.getRoutingNodes(), STARTED).size(),
            equalTo(4)
        );
        assertThat(
            "expected 2 primaries to relocate useClusterSettings: " + useClusterSetting,
            shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(),
            equalTo(2)
        );

    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings) {
        return new MockAllocationService(
            randomAllocationDeciders(settings, clusterSettings),
            new TestGatewayAllocator(),
            createShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }
}
