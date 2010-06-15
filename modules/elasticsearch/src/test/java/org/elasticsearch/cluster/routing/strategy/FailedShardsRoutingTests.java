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

package org.elasticsearch.cluster.routing.strategy;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.testng.annotations.Test;

import java.util.List;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.cluster.node.DiscoveryNodes.*;
import static org.elasticsearch.cluster.routing.RoutingBuilders.*;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class FailedShardsRoutingTests {

    private final ESLogger logger = Loggers.getLogger(FailedShardsRoutingTests.class);

    @Test public void testFailures() {
        DefaultShardsRoutingStrategy strategy = new DefaultShardsRoutingStrategy();

        logger.info("Building initial routing table");

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(3).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test").initializeEmpty(metaData.index("test")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("Start the primary shard (on node1)");
        RoutingNodes routingNodes = routingTable.routingNodes(clusterState.metaData());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node1").shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("Start the backup shard (on node2)");
        routingNodes = routingTable.routingNodes(clusterState.metaData());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node2").shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).backupsShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they recover from primary *started* shards in the IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), equalTo("node2"));
        }

        logger.info("Adding third node and reroute");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes()).put(newNode("node3"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(3));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(2));

        logger.info("Fail the shards on node 3");
        routingNodes = routingTable.routingNodes(clusterState.metaData());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShards(clusterState, routingNodes.node("node3").shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node3"), nullValue());

        logger.info("Do another reroute, should try and assign again to node 3");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(3));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(2));

        logger.info("Start the shards on node 3");
        routingNodes = routingTable.routingNodes(clusterState.metaData());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node3").shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(3));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(2));
    }

    @Test public void test10ShardsWith1ReplicaFailure() {
        DefaultShardsRoutingStrategy strategy = new DefaultShardsRoutingStrategy();

        logger.info("Building initial routing table");

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test").initializeEmpty(metaData.index("test")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        logger.info("Adding one node and performing rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1"))).build();

        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).backupsShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Add another node and perform rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes()).put(newNode("node2"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).backupsShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they recover from primary *started* shards in the IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), equalTo("node2"));
        }

        logger.info("Start the primary shard (on node1)");
        RoutingNodes routingNodes = routingTable.routingNodes(clusterState.metaData());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node1").shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).backupsShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they recover from primary *started* shards in the IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), equalTo("node2"));
        }

        logger.info("Reroute, nothing should change");
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        assertThat(prevRoutingTable == routingTable, equalTo(true));

        logger.info("Fail the backup shards");
        routingNodes = routingTable.routingNodes(metaData);
        prevRoutingTable = routingTable;
        List<MutableShardRouting> failedShards = routingNodes.node("node2").shardsWithState(INITIALIZING);
        routingTable = strategy.applyFailedShards(clusterState, failedShards);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).backupsShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they recover from primary *started* shards in the IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), nullValue());
        }

        // fail them again...
        routingTable = strategy.applyFailedShards(clusterState, failedShards);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).backupsShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they recover from primary *started* shards in the IndicesClusterStateService
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), nullValue());
        }
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE);
    }
}
