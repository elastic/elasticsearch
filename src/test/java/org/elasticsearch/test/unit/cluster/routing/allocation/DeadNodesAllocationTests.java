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
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Test;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.unit.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DeadNodesAllocationTests {

    private final ESLogger logger = Loggers.getLogger(DeadNodesAllocationTests.class);

    @Test
    public void simpleDeadNodeOnStartedPrimaryShard() {
        AllocationService allocation = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node1"))
                .put(newNode("node2"))
        ).build();

        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        // starting primaries
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().get(0).state(), equalTo(STARTED));

        logger.info("--> fail node with primary");
        String nodeIdToFail = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String nodeIdRemaining = nodeIdToFail.equals("node1") ? "node2" : "node1";
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode(nodeIdRemaining))
        ).build();

        rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(nodeIdRemaining).shards().get(0).primary(), equalTo(true));
        assertThat(clusterState.routingNodes().node(nodeIdRemaining).shards().get(0).state(), equalTo(STARTED));
    }

    @Test
    public void deadNodeWhileRelocatingOnToNode() {
        AllocationService allocation = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node1"))
                .put(newNode("node2"))
        ).build();

        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        // starting primaries
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().get(0).state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3"))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").shards().size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.routingNodes().node("node3").shards().get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized by killing node3");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode(origPrimaryNodeId))
                .put(newNode(origReplicaNodeId))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node(origReplicaNodeId).shards().get(0).state(), equalTo(STARTED));
    }

    @Test
    public void deadNodeWhileRelocatingOnFromNode() {
        AllocationService allocation = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node1"))
                .put(newNode("node2"))
        ).build();

        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        // starting primaries
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().get(0).state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node3"))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").shards().size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.routingNodes().node("node3").shards().get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on 'origPrimaryNodeId' being relocated");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node3"))
                .put(newNode(origReplicaNodeId))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(origReplicaNodeId).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").shards().get(0).state(), equalTo(INITIALIZING));
    }
}
