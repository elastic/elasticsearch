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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FailedShardsRoutingTests extends ESAllocationTestCase {
    private final Logger logger = Loggers.getLogger(FailedShardsRoutingTests.class);

    public void testFailedShardPrimaryRelocatingToAndFrom() {
        AllocationService allocation = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                        .add(newNode("node1"))
                        .add(newNode("node2"))
        ).build();

        clusterState = allocation.reroute(clusterState, "reroute");

        // starting primaries
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        // starting replicas
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                        .add(newNode("node3"))
        ).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        AllocationService.CommandsResult commandsResult = allocation.reroute(clusterState, new AllocationCommands(
                        new MoveAllocationCommand("test", 0, clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3")),
            false, false);
        assertThat(commandsResult.getClusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized");
        clusterState = allocation.applyFailedShard(clusterState, clusterState.getRoutingNodes().node("node3").iterator().next());

        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> moving primary shard to node3");
        commandsResult = allocation.reroute(clusterState, new AllocationCommands(
                        new MoveAllocationCommand("test", 0, clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), "node3")),
            false, false);
        assertThat(commandsResult.getClusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node1 being relocated");
        clusterState = allocation.applyFailedShard(clusterState, clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next());

        // check promotion of replica to primary
        assertThat(clusterState.getRoutingNodes().node(origReplicaNodeId).iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(origReplicaNodeId));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), anyOf(equalTo(origPrimaryNodeId), equalTo("node3")));
    }

    public void testFailPrimaryStartedCheckReplicaElected() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the shards (primaries)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();


        ClusterState newState = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Start the shards (backups)");
        routingNodes = clusterState.getRoutingNodes();
        newState = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("fail the primary shard, will have no place to be rerouted to (single node), so stays unassigned");
        ShardRouting shardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        newState = strategy.applyFailedShard(clusterState, shardToFail);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), not(equalTo(shardToFail.currentNodeId())));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(UNASSIGNED));
    }

    public void testFirstAllocationFailureSingleNode() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("Adding single node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will have no place to be rerouted to (single node), so stays unassigned");
        ShardRouting firstShard = clusterState.getRoutingNodes().node("node1").iterator().next();
        newState = strategy.applyFailedShard(clusterState, firstShard);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }
    }

    public void testSingleShardMultipleAllocationFailures() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build());

        logger.info("Building initial routing table");
        int numberOfReplicas = scaledRandomIntBetween(2, 10);
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(numberOfReplicas))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("Adding {} nodes and performing rerouting", numberOfReplicas + 1);
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfReplicas + 1; i++) {
            nodeBuilder.add(newNode("node" + Integer.toString(i)));
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        while (!clusterState.routingTable().shardsWithState(UNASSIGNED).isEmpty()) {
            // start all initializing
            clusterState = strategy.applyStartedShards(clusterState, clusterState.routingTable().shardsWithState(INITIALIZING));
            // and assign more unassigned
            clusterState = strategy.reroute(clusterState, "reroute");
        }

        int shardsToFail = randomIntBetween(1, numberOfReplicas);
        ArrayList<FailedShard> failedShards = new ArrayList<>();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        Set<String> failedNodes = new HashSet<>();
        Set<ShardRouting> shardRoutingsToFail = new HashSet<>();
        for (int i = 0; i < shardsToFail; i++) {
            String failedNode = "node" + Integer.toString(randomInt(numberOfReplicas));
            logger.info("failing shard on node [{}]", failedNode);
            ShardRouting shardToFail = routingNodes.node(failedNode).iterator().next();
            if (shardRoutingsToFail.contains(shardToFail) == false) {
                failedShards.add(new FailedShard(shardToFail, null, null));
                failedNodes.add(failedNode);
                shardRoutingsToFail.add(shardToFail);
            }
        }

        clusterState = strategy.applyFailedShards(clusterState, failedShards);
        routingNodes = clusterState.getRoutingNodes();
        for (FailedShard failedShard : failedShards) {
            if (routingNodes.getByAllocationId(failedShard.getRoutingEntry().shardId(),
                                               failedShard.getRoutingEntry().allocationId().getId()) != null) {
                fail("shard " + failedShard + " was not failed");
            }
        }

        for (String failedNode : failedNodes) {
            if (!routingNodes.node(failedNode).isEmpty()) {
                fail("shard was re-assigned to failed node " + failedNode);
            }
        }
    }

    public void testFirstAllocationFailureTwoNodes() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(clusterState));
        clusterState = newState;
        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will start INITIALIZING on the second node");
        final ShardRouting firstShard = clusterState.getRoutingNodes().node(nodeHoldingPrimary).iterator().next();
        newState = strategy.applyFailedShard(clusterState, firstShard);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        final String nodeHoldingPrimary2 = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        assertThat(nodeHoldingPrimary2, not(equalTo(nodeHoldingPrimary)));

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), not(equalTo(nodeHoldingPrimary)));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }
    }

    public void testRebalanceFailure() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the shards (primaries)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        ClusterState newState = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Start the shards (backups)");
        routingNodes = clusterState.getRoutingNodes();
        newState = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), anyOf(equalTo("node1"), equalTo("node2")));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), anyOf(equalTo("node2"), equalTo("node1")));
        }

        logger.info("Adding third node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));


        logger.info("Fail the shards on node 3");
        ShardRouting shardToFail = routingNodes.node("node3").iterator().next();
        newState = strategy.applyFailedShard(clusterState, shardToFail);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // make sure the failedShard is not INITIALIZING again on node3
        assertThat(routingNodes.node("node3").iterator().next().shardId(), not(equalTo(shardToFail.shardId())));
    }

    public void testFailAllReplicasInitializingOnPrimaryFail() {
        AllocationService allocation = createAllocationService(Settings.builder()
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).build();

        ShardId shardId = new ShardId(metaData.index("test").getIndex(), 0);

        // add 4 nodes
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4"))).build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState, "reroute").routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        // start primary shards
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // start one replica so it can take over.
        clusterState = allocation.applyStartedShards(clusterState,
            Collections.singletonList(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0)));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        ShardRouting startedReplica = clusterState.getRoutingNodes().activeReplicaWithHighestVersion(shardId);


        // fail the primary shard, check replicas get removed as well...
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        // the primary gets allocated on another node, replicas are initializing
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));
        assertThat(newPrimaryShard.allocationId(), equalTo(startedReplica.allocationId()));
    }

    public void testFailAllReplicasInitializingOnPrimaryFailWhileHavingAReplicaToElect() {
        AllocationService allocation = createAllocationService(Settings.builder()
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        // add 4 nodes
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        // start primary shards
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // start another replica shard, while keep one initializing
        clusterState = allocation.applyStartedShards(clusterState, Collections.singletonList(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0)));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        // fail the primary shard, check one replica gets elected to primary, others become INITIALIZING (from it)
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));
    }

    public void testReplicaOnNewestVersionIsPromoted() {
        AllocationService allocation = createAllocationService(Settings.builder().build());

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(3)) .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData).routingTable(initialRoutingTable).build();

        ShardId shardId = new ShardId(metaData.index("test").getIndex(), 0);

        // add a single node
        clusterState = ClusterState.builder(clusterState).nodes(
                DiscoveryNodes.builder()
                .add(newNode("node1-5.x", Version.V_5_6_0)))
                .build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState, "reroute").routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(3));

        // start primary shard
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(3));

        // add another 5.6 node
        clusterState = ClusterState.builder(clusterState).nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node2-5.x", Version.V_5_6_0)))
                .build();

        // start the shards, should have 1 primary and 1 replica available
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));

        clusterState = ClusterState.builder(clusterState).nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3-6.x", VersionUtils.randomVersionBetween(random(), Version.V_6_0_0_alpha1, null)))
                .add(newNode("node4-6.x", VersionUtils.randomVersionBetween(random(), Version.V_6_0_0_alpha1, null))))
                .build();

        // start all the replicas
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        ShardRouting startedReplica = clusterState.getRoutingNodes().activeReplicaWithHighestVersion(shardId);
        logger.info("--> all shards allocated, replica that should be promoted: {}", startedReplica);

        // fail the primary shard again and make sure the correct replica is promoted
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShardToFail);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        // the primary gets allocated on another node
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));
        assertThat(newPrimaryShard.allocationId(), equalTo(startedReplica.allocationId()));

        Version replicaNodeVersion = clusterState.nodes().getDataNodes().get(startedReplica.currentNodeId()).getVersion();
        assertNotNull(replicaNodeVersion);
        logger.info("--> shard {} got assigned to node with version {}", startedReplica, replicaNodeVersion);

        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().getDataNodes().values()) {
            if ("node1".equals(cursor.value.getId())) {
                // Skip the node that the primary was on, it doesn't have a replica so doesn't need a version check
                continue;
            }
            Version nodeVer = cursor.value.getVersion();
            assertTrue("expected node [" + cursor.value.getId() + "] with version " + nodeVer + " to be before " + replicaNodeVersion,
                    replicaNodeVersion.onOrAfter(nodeVer));
        }

        startedReplica = clusterState.getRoutingNodes().activeReplicaWithHighestVersion(shardId);
        logger.info("--> failing primary shard a second time, should select: {}", startedReplica);

        // fail the primary shard again, and ensure the same thing happens
        ShardRouting secondPrimaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        newState = allocation.applyFailedShard(clusterState, secondPrimaryShardToFail);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        // the primary gets allocated on another node
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));

        newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(secondPrimaryShardToFail)));
        assertThat(newPrimaryShard.allocationId(), equalTo(startedReplica.allocationId()));

        replicaNodeVersion = clusterState.nodes().getDataNodes().get(startedReplica.currentNodeId()).getVersion();
        assertNotNull(replicaNodeVersion);
        logger.info("--> shard {} got assigned to node with version {}", startedReplica, replicaNodeVersion);

        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().getDataNodes().values()) {
            if (primaryShardToFail.currentNodeId().equals(cursor.value.getId()) ||
                    secondPrimaryShardToFail.currentNodeId().equals(cursor.value.getId())) {
                // Skip the node that the primary was on, it doesn't have a replica so doesn't need a version check
                continue;
            }
            Version nodeVer = cursor.value.getVersion();
            assertTrue("expected node [" + cursor.value.getId() + "] with version " + nodeVer + " to be before " + replicaNodeVersion,
                    replicaNodeVersion.onOrAfter(nodeVer));
        }
    }
}
