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
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DeadNodesAllocationTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(DeadNodesAllocationTests.class);

    @Test
    public void simpleDeadNodeOnStartedPrimaryShard() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
        ).build();

        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        // starting primaries
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.routingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").get(0).state(), equalTo(STARTED));

        logger.info("--> fail node with primary");
        String nodeIdToFail = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String nodeIdRemaining = nodeIdToFail.equals("node1") ? "node2" : "node1";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode(nodeIdRemaining))
        ).build();

        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(nodeIdRemaining).get(0).primary(), equalTo(true));
        assertThat(clusterState.routingNodes().node(nodeIdRemaining).get(0).state(), equalTo(STARTED));
    }

    @Test
    public void deadNodeWhileRelocatingOnToNode() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
        ).build();

        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        // starting primaries
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.routingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").get(0).state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3"))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.routingNodes().node("node3").get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized by killing node3");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode(origPrimaryNodeId))
                .put(newNode(origReplicaNodeId))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node(origReplicaNodeId).get(0).state(), equalTo(STARTED));
    }

    @Test
    public void deadNodeWhileRelocatingOnFromNode() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
        ).build();

        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        // starting primaries
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.routingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").get(0).state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3"))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.routingNodes().node("node3").get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on 'origPrimaryNodeId' being relocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node3"))
                .put(newNode(origReplicaNodeId))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(origReplicaNodeId).get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").get(0).state(), equalTo(INITIALIZING));
    }
}
