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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class FailedShardsRoutingTests extends ElasticsearchTestCase {

    private final ESLogger logger = Loggers.getLogger(FailedShardsRoutingTests.class);

    @Test
    public void testFailedShardPrimaryRelocatingToAndFrom() {
        AllocationService allocation = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder().metaData(metaData).routingTable(routingTable).build();

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
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().get(0).state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3"))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

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
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.routingNodes().node("node3").shards().get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized");
        rerouteResult = allocation.applyFailedShard(clusterState, new ImmutableShardRouting(clusterState.routingNodes().node("node3").shards().get(0)));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingNodes().node("node3").shards().size(), equalTo(0));

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.routingNodes().node("node3").shards().get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node1 being relocated");
        rerouteResult = allocation.applyFailedShard(clusterState, new ImmutableShardRouting(clusterState.routingNodes().node(origPrimaryNodeId).shards().get(0)));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        // check promotion of replica to primary
        assertThat(clusterState.routingNodes().node(origReplicaNodeId).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(origReplicaNodeId));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), anyOf(equalTo(origPrimaryNodeId), equalTo("node3")));
    }

    @Test
    public void failPrimaryStartedCheckReplicaElected() {
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the shards (primaries)");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Start the shards (backups)");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("fail the primary shard, will have no place to be rerouted to (single node), so stays unassigned");
        ShardRouting shardToFail = new ImmutableShardRouting(routingTable.index("test").shard(0).primaryShard());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, shardToFail).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(2));
        assertThat(routingTable.index("test").shard(0).primaryShard().currentNodeId(), not(equalTo(shardToFail.currentNodeId())));
        assertThat(routingTable.index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(routingTable.index("test").shard(0).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
        assertThat(routingTable.index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).replicaShards().get(0).state(), equalTo(UNASSIGNED));

        logger.info("fail the shard again, check that nothing happens");
        assertThat(strategy.applyFailedShard(clusterState, shardToFail).changed(), equalTo(false));
    }

    @Test
    public void firstAllocationFailureSingleNode() {
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding single node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will have no place to be rerouted to (single node), so stays unassigned");
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, new ImmutableShardRouting("test", 0, "node1", true, INITIALIZING, 0)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the shard again, see that nothing happens");
        assertThat(strategy.applyFailedShard(clusterState, new ImmutableShardRouting("test", 0, "node1", true, INITIALIZING, 0)).changed(), equalTo(false));
    }

    @Test
    public void firstAllocationFailureTwoNodes() {
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will start INITIALIZING on the second node");
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, new ImmutableShardRouting("test", 0, "node1", true, INITIALIZING, 0)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo("node2"));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the shard again, see that nothing happens");
        assertThat(strategy.applyFailedShard(clusterState, new ImmutableShardRouting("test", 0, "node1", true, INITIALIZING, 0)).changed(), equalTo(false));
    }

    @Test
    public void rebalanceFailure() {
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the shards (primaries)");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Start the shards (backups)");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Adding third node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));


        logger.info("Fail the shards on node 3");
        ShardRouting shardToFail = routingNodes.node("node3").shards().get(0);
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, new ImmutableShardRouting(shardToFail)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // make sure the failedShard is not INITIALIZING again on node3
        assertThat(routingNodes.node("node3").shards().get(0).shardId(), not(equalTo(shardToFail.shardId())));
    }
}
