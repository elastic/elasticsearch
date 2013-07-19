/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Test;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FailedNodeRoutingTests {

    private final ESLogger logger = Loggers.getLogger(FailedNodeRoutingTests.class);

    @Test
    public void simpleFailedNodeTest() {
        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start 4 nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(RoutingAllocationTests.newNode("node1")).put(RoutingAllocationTests.newNode("node2")).put(RoutingAllocationTests.newNode("node3")).put(RoutingAllocationTests.newNode("node4"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(1));


        logger.info("remove 2 nodes where primaries are allocated, reroute");

        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .remove(routingTable.index("test1").shard(0).primaryShard().currentNodeId())
                .remove(routingTable.index("test2").shard(0).primaryShard().currentNodeId())
        )
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(routingNode.numberOfShardsWithState(INITIALIZING), equalTo(1));
        }
    }

    @Test
    public void simpleFailedNodeTestNoReassign() {
        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start 4 nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(RoutingAllocationTests.newNode("node1")).put(RoutingAllocationTests.newNode("node2")).put(RoutingAllocationTests.newNode("node3")).put(RoutingAllocationTests.newNode("node4"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(1));


        logger.info("remove 2 nodes where primaries are allocated, reroute");

        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .remove(routingTable.index("test1").shard(0).primaryShard().currentNodeId())
                .remove(routingTable.index("test2").shard(0).primaryShard().currentNodeId())
        )
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.rerouteWithNoReassign(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(1));
        }
        assertThat(routingNodes.unassigned().size(), equalTo(2));
    }
}