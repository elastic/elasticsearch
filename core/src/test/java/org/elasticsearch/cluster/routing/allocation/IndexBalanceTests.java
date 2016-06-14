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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;

import static org.elasticsearch.cluster.ClusterName.DEFAULT;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class IndexBalanceTests extends ESAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(IndexBalanceTests.class);

    public void testBalanceAllNodesStarted() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).addAsNew(metaData.index("test1")).build();

        ClusterState clusterState = ClusterState.builder(DEFAULT).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test1").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test1").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test1").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3"))).build();

        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable == routingTable, equalTo(true));

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Start the more shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).size(), equalTo(2));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).size(), equalTo(2));
    }

    public void testBalanceIncrementallyStartNodes() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).addAsNew(metaData.index("test1")).build();

        ClusterState clusterState = ClusterState.builder(DEFAULT).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test1").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test1").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test1").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();

        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Add another node and perform rerouting, nothing will happen since primary not started");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node2"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Start the primary shard");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Start the backup shard");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        logger.info("Add another node and perform rerouting, nothing will happen since primary not started");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));

        logger.info("Reroute, nothing should change");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Start the backup shard");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));

        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test1").shards().size(), equalTo(3));

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).size(), equalTo(2));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).size(), equalTo(2));
    }

    public void testBalanceAllNodesStartedAddIndex() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(DEFAULT).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3"))).build();

        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable == routingTable, equalTo(true));

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Start the more shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();
        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
        }

        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(2));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).size(), equalTo(2));

        logger.info("Add new index 3 shards 1 replica");

        prevRoutingTable = routingTable;
        metaData = MetaData.builder(metaData)
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                ))
                .build();
        routingTable = RoutingTable.builder(routingTable)
                .addAsNew(metaData.index("test1"))
                .build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).routingTable(routingTable).build();


        assertThat(routingTable.index("test1").shards().size(), equalTo(3));

        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable == routingTable, equalTo(true));

        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Start the more shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();
        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test1").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().size(), equalTo(1));
        }

        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).size(), equalTo(2));

    }

    public void testBalanceShardsWithDifferentWeightsScenario1() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("small1").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetaData.builder("small2").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetaData.builder("small3").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetaData.builder("small4").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetaData.builder("large").settings(settings(Version.CURRENT)
                .put(BalancedShardsAllocator.INDEX_SHARD_WEIGHT_MULTIPLIER_SETTING.getKey(), 2.0f)).numberOfShards(2).numberOfReplicas(0))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (IndexMetaData indexMetaData : metaData) {
            routingTableBuilder.addAsNew(indexMetaData);
        }
        ClusterState clusterState = startAllShards(strategy, ClusterState.builder(DEFAULT)
            .nodes(DiscoveryNodes.builder()
                .put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).put(newNode("node4")))
            .metaData(metaData).routingTable(routingTableBuilder.build()).build());

        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            assertThat(routingNode.size(), either(equalTo(2)).or(equalTo(3)));
            assertEquals(routingNode.size() == 2, routingNode.shardsWithState("large", ShardRoutingState.STARTED).size() == 1);
        }
    }

    public void testBalanceShardsWithDifferentWeightsScenario2() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("small").settings(settings(Version.CURRENT)
                .put(BalancedShardsAllocator.INDEX_SHARD_WEIGHT_MULTIPLIER_SETTING.getKey(), 1.0f)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetaData.builder("medium").settings(settings(Version.CURRENT)
                .put(BalancedShardsAllocator.INDEX_SHARD_WEIGHT_MULTIPLIER_SETTING.getKey(), 10.0f)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetaData.builder("large").settings(settings(Version.CURRENT)
                .put(BalancedShardsAllocator.INDEX_SHARD_WEIGHT_MULTIPLIER_SETTING.getKey(), 100.0f)).numberOfShards(2).numberOfReplicas(0))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (IndexMetaData indexMetaData : metaData) {
            routingTableBuilder.addAsNew(indexMetaData);
        }
        ClusterState clusterState = startAllShards(strategy, ClusterState.builder(DEFAULT)
            .nodes(DiscoveryNodes.builder()
                .put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).put(newNode("node4")))
            .metaData(metaData).routingTable(routingTableBuilder.build()).build());

        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            assertThat(routingNode.size(), either(equalTo(1)).or(equalTo(2)));
            assertEquals(routingNode.size() == 1, routingNode.shardsWithState("large", ShardRoutingState.STARTED).size() == 1);
        }
    }

    protected ClusterState startAllShards(AllocationService strategy, ClusterState clusterState) {
        RoutingAllocation.Result rerouteResult;
        do {
            rerouteResult = strategy.reroute(clusterState, "dummy reroute");
            clusterState = ClusterState.builder(clusterState).routingResult(rerouteResult).build();
            rerouteResult = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
            clusterState = ClusterState.builder(clusterState).routingResult(rerouteResult).build();
        } while (rerouteResult.changed());
        return clusterState;
    }
}
