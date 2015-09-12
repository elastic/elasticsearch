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
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class FailedShardsRoutingTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(FailedShardsRoutingTests.class);

    @Test
    public void testFailedShardPrimaryRelocatingToAndFrom() {
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
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        // starting replicas
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").get(0).state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                        .put(newNode("node3"))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").get(0).state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                        new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized");
        rerouteResult = allocation.applyFailedShard(clusterState, new ShardRouting(clusterState.getRoutingNodes().node("node3").get(0)));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).get(0).state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> moving primary shard to node3");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(
                        new MoveAllocationCommand(clusterState.routingTable().index("test").shard(0).primaryShard().shardId(), clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3"))
        );
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).get(0).state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").get(0).state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node1 being relocated");
        rerouteResult = allocation.applyFailedShard(clusterState, new ShardRouting(clusterState.getRoutingNodes().node(origPrimaryNodeId).get(0)));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        // check promotion of replica to primary
        assertThat(clusterState.getRoutingNodes().node(origReplicaNodeId).get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(origReplicaNodeId));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), anyOf(equalTo(origPrimaryNodeId), equalTo("node3")));
    }

    @Test
    public void failPrimaryStartedCheckReplicaElected() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the shards (primaries)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Start the shards (backups)");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("fail the primary shard, will have no place to be rerouted to (single node), so stays unassigned");
        ShardRouting shardToFail = new ShardRouting(routingTable.index("test").shard(0).primaryShard());
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, shardToFail).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(2));
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
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

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
        ShardRouting firstShard = clusterState.getRoutingNodes().node("node1").get(0);
        routingTable = strategy.applyFailedShard(clusterState, firstShard).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

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
        assertThat(strategy.applyFailedShard(clusterState, firstShard).changed(), equalTo(false));
    }

    @Test
    public void singleShardMultipleAllocationFailures() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("Building initial routing table");
        int numberOfReplicas = scaledRandomIntBetween(2, 10);
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(numberOfReplicas))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding {} nodes and performing rerouting", numberOfReplicas + 1);
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfReplicas + 1; i++) {
            nodeBuilder.put(newNode("node" + Integer.toString(i)));
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        while (!clusterState.routingTable().shardsWithState(UNASSIGNED).isEmpty()) {
            // start all initializing
            clusterState = ClusterState.builder(clusterState)
                    .routingTable(strategy
                                    .applyStartedShards(clusterState, clusterState.routingTable().shardsWithState(INITIALIZING)).routingTable()
                    )
                    .build();
            // and assign more unassigned
            clusterState = ClusterState.builder(clusterState).routingTable(strategy.reroute(clusterState).routingTable()).build();
        }

        int shardsToFail = randomIntBetween(1, numberOfReplicas);
        ArrayList<FailedRerouteAllocation.FailedShard> failedShards = new ArrayList<>();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        for (int i = 0; i < shardsToFail; i++) {
            String n = "node" + Integer.toString(randomInt(numberOfReplicas));
            logger.info("failing shard on node [{}]", n);
            ShardRouting shardToFail = routingNodes.node(n).get(0);
            failedShards.add(new FailedRerouteAllocation.FailedShard(new ShardRouting(shardToFail), null, null));
        }

        routingTable = strategy.applyFailedShards(clusterState, failedShards).routingTable();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();
        for (FailedRerouteAllocation.FailedShard failedShard : failedShards) {
            if (!routingNodes.node(failedShard.shard.currentNodeId()).isEmpty()) {
                fail("shard " + failedShard + " was re-assigned to it's node");
            }
        }
    }

    @Test
    public void firstAllocationFailureTwoNodes() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        final String nodeHoldingPrimary = routingTable.index("test").shard(0).primaryShard().currentNodeId();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will start INITIALIZING on the second node");
        prevRoutingTable = routingTable;
        final ShardRouting firstShard = clusterState.getRoutingNodes().node(nodeHoldingPrimary).get(0);
        routingTable = strategy.applyFailedShard(clusterState, firstShard).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(prevRoutingTable != routingTable, equalTo(true));

        final String nodeHoldingPrimary2 = routingTable.index("test").shard(0).primaryShard().currentNodeId();
        assertThat(nodeHoldingPrimary2, not(equalTo(nodeHoldingPrimary)));

        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).primaryShard().currentNodeId(), not(equalTo(nodeHoldingPrimary)));
            assertThat(routingTable.index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the shard again, see that nothing happens");
        assertThat(strategy.applyFailedShard(clusterState, firstShard).changed(), equalTo(false));
    }

    @Test
    public void rebalanceFailure() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the shards (primaries)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
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
        routingNodes = clusterState.getRoutingNodes();
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
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));


        logger.info("Fail the shards on node 3");
        ShardRouting shardToFail = routingNodes.node("node3").get(0);
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, new ShardRouting(shardToFail)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // make sure the failedShard is not INITIALIZING again on node3
        assertThat(routingNodes.node("node3").get(0).shardId(), not(equalTo(shardToFail.shardId())));
    }

    @Test
    public void testFailAllReplicasInitializingOnPrimaryFail() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        // add 4 nodes
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).put(newNode("node4"))).build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState).routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        // start primary shards
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // fail the primary shard, check replicas get removed as well...
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        RoutingAllocation.Result routingResult = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(routingResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(routingResult.routingTable()).build();
        // the primary gets allocated on another node, replicas are unassigned
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));

        // start the primary shard
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // simulate another failure coming in, with the "old" shard routing, verify that nothing changes, and we ignore it
        routingResult = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(routingResult.changed(), equalTo(false));
    }

    @Test
    public void testFailAllReplicasInitializingOnPrimaryFailWhileHavingAReplicaToElect() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        // add 4 nodes
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).put(newNode("node4"))).build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState).routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        // start primary shards
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // start another replica shard, while keep one initializing
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.applyStartedShards(clusterState, Collections.singletonList(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0))).routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        // fail the primary shard, check one replica gets elected to primary, others become INITIALIZING (from it)
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        RoutingAllocation.Result routingResult = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(routingResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(routingResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));

        // simulate another failure coming in, with the "old" shard routing, verify that nothing changes, and we ignore it
        routingResult = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(routingResult.changed(), equalTo(false));
    }
}
