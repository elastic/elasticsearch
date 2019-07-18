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

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

public class AwarenessAllocationTests extends ESAllocationTestCase {

    private final Logger logger = LogManager.getLogger(AwarenessAllocationTests.class);

    public void testMoveShardOnceNewNodeWithAttributeAdded1() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded1'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node3"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded2() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded2'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
                .add(newNode("node3", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node4"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node5", singletonMap("rack_id", "3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded3() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .put("cluster.routing.allocation.balance.index", 0.0f)
                .put("cluster.routing.allocation.balance.replica", 1.0f)
                .put("cluster.routing.allocation.balance.primary", 0.0f)
                .build());

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded3'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Initializing shards: {}", clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        logger.info("Started shards: {}", clusterState.getRoutingNodes().shardsWithState(STARTED));
        logger.info("Relocating shards: {}", clusterState.getRoutingNodes().shardsWithState(RELOCATING));
        logger.info("Unassigned shards: {}", clusterState.getRoutingNodes().shardsWithState(UNASSIGNED));

        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(5));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node3"));

        logger.info("--> complete initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> run it again, since we still might have relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, some more relocation should happen");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "3")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), greaterThan(0));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded4() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded4'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(10));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node3"));

        logger.info("--> complete initializing");
        for (int i = 0; i < 2; i++) {
            logger.info("--> complete initializing round: [{}]", i);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(5));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, some more relocation should happen");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "3")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), greaterThan(0));

        logger.info("--> complete relocation");
        for (int i = 0; i < 2; i++) {
            logger.info("--> complete initializing round: [{}]", i);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(5));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded5() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded5'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node3"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(3));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, we will have another relocation");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "3")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node4"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(3));

        logger.info("--> make sure another reroute does not move things");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded6() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded6'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(3))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
                .add(newNode("node3", singletonMap("rack_id", "1")))
                .add(newNode("node4", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node5", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node5"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, we will have another relocation");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node6", singletonMap("rack_id", "3")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node6"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> make sure another reroute does not move things");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));
    }

    public void testFullAwareness1() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.force.rack_id.values", "1,2")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'fullAwareness1'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one rack value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node3"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
    }

    public void testFullAwareness2() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.force.rack_id.values", "1,2")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build());

        logger.info("Building initial routing table for 'fullAwareness2'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
                .add(newNode("node3", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one rack value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node4"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node5", singletonMap("rack_id", "3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
    }

    public void testFullAwareness3() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.awareness.force.rack_id.values", "1,2")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .put("cluster.routing.allocation.balance.index", 0.0f)
                .put("cluster.routing.allocation.balance.replica", 1.0f)
                .put("cluster.routing.allocation.balance.primary", 0.0f)
                .build());

        logger.info("Building initial routing table for 'fullAwareness3'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1", singletonMap("rack_id", "1")))
                .add(newNode("node2", singletonMap("rack_id", "1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(10));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3", singletonMap("rack_id", "2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node3"));

        logger.info("--> complete initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> run it again, since we still might have relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, some more relocation should happen");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node4", singletonMap("rack_id", "3")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), greaterThan(0));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));
    }

    public void testUnbalancedZones() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build());

        logger.info("Building initial routing table for 'testUnbalancedZones'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes in different zones and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("A-0", singletonMap("zone", "a")))
                .add(newNode("B-0", singletonMap("zone", "b")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(5));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(5));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logger.info("--> all replicas are allocated and started since we have on node in each zone");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> add a new node in zone 'a' and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("A-1", singletonMap("zone", "a")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(8));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("A-1"));
        logger.info("--> starting initializing shards on the new node");

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().node("A-1").size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("A-0").size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("B-0").size(), equalTo(5));
    }

    public void testUnassignedShardsWithUnbalancedZones() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .build());

        logger.info("Building initial routing table for 'testUnassignedShardsWithUnbalancedZones'");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(4))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding 5 nodes in different zones and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                        .add(newNode("A-0", singletonMap("zone", "a")))
                        .add(newNode("A-1", singletonMap("zone", "a")))
                        .add(newNode("A-2", singletonMap("zone", "a")))
                        .add(newNode("A-3", singletonMap("zone", "a")))
                        .add(newNode("A-4", singletonMap("zone", "a")))
                        .add(newNode("B-0", singletonMap("zone", "b")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shard (primary)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1)); // Unassigned shard is expected.

        // Cancel all initializing shards and move started primary to another node.
        AllocationCommands commands = new AllocationCommands();
        String primaryNode = null;
        for (ShardRouting routing : clusterState.routingTable().allShards()) {
            if (routing.primary()) {
                primaryNode = routing.currentNodeId();
            } else if (routing.initializing()) {
                commands.add(new CancelAllocationCommand(routing.shardId().getIndexName(), routing.id(), routing.currentNodeId(), false));
            }
        }
        commands.add(new MoveAllocationCommand("test", 0, primaryNode, "A-4"));

        clusterState = strategy.reroute(clusterState, commands, false, false).getClusterState();

        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(4)); // +1 for relocating shard.
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1)); // Still 1 unassigned.
    }

    public void testMultipleAwarenessAttributes() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d")
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .build());

        logger.info("Building initial routing table for 'testUnbalancedZones'");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(
            org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)
        ).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes in different zones and do rerouting");
        Map<String, String> nodeAAttributes = new HashMap<>();
        nodeAAttributes.put("zone", "a");
        nodeAAttributes.put("rack", "c");
        Map<String, String> nodeBAttributes = new HashMap<>();
        nodeBAttributes.put("zone", "b");
        nodeBAttributes.put("rack", "d");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("A-0", nodeAAttributes))
            .add(newNode("B-0", nodeBAttributes))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logger.info("--> all replicas are allocated and started since we have one node in each zone and rack");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
    }
}
