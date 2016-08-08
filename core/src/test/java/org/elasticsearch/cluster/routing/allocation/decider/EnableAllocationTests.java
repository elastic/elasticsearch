/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Allocation;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class EnableAllocationTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(EnableAllocationTests.class);

    public void testClusterEnableNone() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name())
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
        ).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

    }

    public void testClusterEnableOnlyPrimaries() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.PRIMARIES.name())
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
        ).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
    }

    public void testIndexEnableNone() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("disabled").settings(settings(Version.CURRENT)
                        .put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name()))
                        .numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("enabled").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("disabled"))
                .addAsNew(metaData.index("enabled"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
        ).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("--> start the shards (replicas)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> verify only enabled index has been routed");
        assertThat(clusterState.getRoutingNodes().shardsWithState("enabled", STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState("disabled", STARTED).size(), equalTo(0));
    }

    public void testEnableClusterBalance() {
        final boolean useClusterSetting = randomBoolean();
        final Rebalance allowedOnes = RandomPicks.randomFrom(random(), EnumSet.of(Rebalance.PRIMARIES, Rebalance.REPLICAS, Rebalance.ALL));
        Settings build = Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), useClusterSetting ? Rebalance.NONE: RandomPicks.randomFrom(random(), Rebalance.values())) // index settings override cluster settings
                .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 3)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
                .build();
        ClusterSettings clusterSettings = new ClusterSettings(build, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationService strategy = createAllocationService(build, clusterSettings, random());
        Settings indexSettings = useClusterSetting ? Settings.EMPTY : Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE).build();

        logger.info("Building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT).put(indexSettings)).numberOfShards(3).numberOfReplicas(1))
                .put(IndexMetaData.builder("always_disabled").settings(settings(Version.CURRENT).put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .addAsNew(metaData.index("always_disabled"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
        ).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(4));
        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(4));

        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
                .add(newNode("node3"))
        ).build();
        ClusterState prevState = clusterState;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(0));

        if (useClusterSetting) {
            prevState = clusterState;
            clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder(metaData).transientSettings(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), allowedOnes)
                .build())).build();
        } else {
            prevState = clusterState;
            IndexMetaData meta = clusterState.getMetaData().index("test");
            IndexMetaData meta1 = clusterState.getMetaData().index("always_disabled");
            clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder(metaData).removeAllIndices().put(IndexMetaData.builder(meta1))
                    .put(IndexMetaData.builder(meta).settings(Settings.builder().put(meta.getSettings()).put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), allowedOnes).build())))
                    .build();

        }
        clusterSettings.applySettings(clusterState.metaData().settings());
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat("expected 6 shards to be started 2 to relocate useClusterSettings: " + useClusterSetting, clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(6));
        assertThat("expected 2 shards to relocate useClusterSettings: " + useClusterSetting, clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(2));
        List<ShardRouting> mutableShardRoutings = clusterState.getRoutingNodes().shardsWithState(RELOCATING);
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
        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

    }

    public void testEnableClusterBalanceNoReplicas() {
        final boolean useClusterSetting = randomBoolean();
        Settings build = Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), useClusterSetting ? Rebalance.NONE: RandomPicks.randomFrom(random(), Rebalance.values())) // index settings override cluster settings
                .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 3)
                .build();
        ClusterSettings clusterSettings = new ClusterSettings(build, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationService strategy = createAllocationService(build, clusterSettings, random());
        Settings indexSettings = useClusterSetting ? Settings.EMPTY : Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE).build();

        logger.info("Building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT).put(indexSettings)).numberOfShards(6).numberOfReplicas(0))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
        ).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(6));
        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(6));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> adding one nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
                .add(newNode("node3"))
        ).build();
        ClusterState prevState = clusterState;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(6));
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(0));
        if (useClusterSetting) {
            prevState = clusterState;
            clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder(metaData).transientSettings(Settings.builder()
                    .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), randomBoolean() ? Rebalance.PRIMARIES : Rebalance.ALL)
                    .build())).build();
        } else {
            prevState = clusterState;
            IndexMetaData meta = clusterState.getMetaData().index("test");
            clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder(metaData).removeAllIndices()
                    .put(IndexMetaData.builder(meta).settings(Settings.builder().put(meta.getSettings()).put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), randomBoolean() ? Rebalance.PRIMARIES : Rebalance.ALL).build()))).build();
        }
        clusterSettings.applySettings(clusterState.metaData().settings());
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat("expected 4 primaries to be started and 2 to relocate useClusterSettings: " + useClusterSetting, clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));
        assertThat("expected 2 primaries to relocate useClusterSettings: " + useClusterSetting, clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(2));

    }

}
