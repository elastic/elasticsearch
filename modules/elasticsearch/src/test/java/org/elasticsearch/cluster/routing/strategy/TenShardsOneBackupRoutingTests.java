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
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.transport.DummyTransportAddress;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.cluster.node.Nodes.*;
import static org.elasticsearch.cluster.routing.RoutingBuilders.*;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TenShardsOneBackupRoutingTests {

    private final Logger logger = Loggers.getLogger(TenShardsOneBackupRoutingTests.class);

    @Test public void testSingleIndexFirstStartPrimaryThenBackups() {
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

        logger.info("Start the backup shard");
        routingNodes = routingTable.routingNodes(metaData);
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node2").shardsWithState(INITIALIZING));
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
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).backupsShards().get(0).currentNodeId(), equalTo("node2"));
        }
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(10));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(10));

        logger.info("Add another node and perform rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes()).put(newNode("node3"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(10));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(10));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(10));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(10));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(6));

        logger.info("Start the shards on node 3");
        routingNodes = routingTable.routingNodes(clusterState.metaData());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node3").shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = routingTable.routingNodes(metaData);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(10));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(8));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(6));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(6));
    }

    private Node newNode(String nodeId) {
        return new Node(nodeId, DummyTransportAddress.INSTANCE);
    }
}
