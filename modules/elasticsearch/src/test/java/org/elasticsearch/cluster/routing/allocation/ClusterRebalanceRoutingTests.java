/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.annotations.Test;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.cluster.node.DiscoveryNodes.*;
import static org.elasticsearch.cluster.routing.RoutingBuilders.*;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.cluster.routing.allocation.RoutingAllocationTests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@Test
public class ClusterRebalanceRoutingTests {

    private final ESLogger logger = Loggers.getLogger(ClusterRebalanceRoutingTests.class);

    @Test public void testAlways() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceNodeAllocation.ClusterRebalanceType.ALWAYS.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test1").initializeEmpty(metaData.index("test1")))
                .add(indexRoutingTable("test2").initializeEmpty(metaData.index("test2")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will happen (for test1) because we set it to always");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node3").shards().size(), equalTo(1));
        assertThat(routingNodes.node("node3").shards().get(0).shardId().index().name(), equalTo("test1"));
    }


    @Test public void testClusterPrimariesActive1() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceNodeAllocation.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test1").initializeEmpty(metaData.index("test1")))
                .add(indexRoutingTable("test2").initializeEmpty(metaData.index("test2")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to primaries_active");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node3").shards().size(), equalTo(1));
        assertThat(routingNodes.node("node3").shards().get(0).shardId().index().name(), equalTo("test1"));
    }

    @Test public void testClusterPrimariesActive2() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceNodeAllocation.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test1").initializeEmpty(metaData.index("test1")))
                .add(indexRoutingTable("test2").initializeEmpty(metaData.index("test2")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to primaries_active");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node3"), nullValue());
    }

    @Test public void testClusterAllActive1() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceNodeAllocation.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test1").initializeEmpty(metaData.index("test1")))
                .add(indexRoutingTable("test2").initializeEmpty(metaData.index("test2")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the test2 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to all_active");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node3").shards().size(), equalTo(1));
        assertThat(routingNodes.node("node3").shards().get(0).shardId().index().name(), equalTo("test1"));
    }

    @Test public void testClusterAllActive2() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceNodeAllocation.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test1").initializeEmpty(metaData.index("test1")))
                .add(indexRoutingTable("test2").initializeEmpty(metaData.index("test2")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node3"), nullValue());
    }

    @Test public void testClusterAllActive3() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceNodeAllocation.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test1").initializeEmpty(metaData.index("test1")))
                .add(indexRoutingTable("test2").initializeEmpty(metaData.index("test2")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node3"), nullValue());
    }
}