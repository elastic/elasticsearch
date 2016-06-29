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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommandRegistry;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.test.ESAllocationTestCase;

import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class AllocationCommandsTests extends ESAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(AllocationCommandsTests.class);

    public void testMoveShardCommand() {
        AllocationService allocation = createAllocationService(Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("start primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("move the shard");
        String existingNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String toNodeId;
        if ("node1".equals(existingNodeId)) {
            toNodeId = "node2";
        } else {
            toNodeId = "node1";
        }
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new MoveAllocationCommand("test", 0, existingNodeId, toNodeId)), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node(existingNodeId).iterator().next().state(), equalTo(ShardRoutingState.RELOCATING));
        assertThat(clusterState.getRoutingNodes().node(toNodeId).iterator().next().state(), equalTo(ShardRoutingState.INITIALIZING));

        logger.info("finish moving the shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.getRoutingNodes().node(existingNodeId).isEmpty(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node(toNodeId).iterator().next().state(), equalTo(ShardRoutingState.STARTED));
    }

    private AbstractAllocateAllocationCommand randomAllocateCommand(String index, int shardId, String node) {
        return randomFrom(
                new AllocateReplicaAllocationCommand(index, shardId, node),
                new AllocateEmptyPrimaryAllocationCommand(index, shardId, node, true),
                new AllocateStalePrimaryAllocationCommand(index, shardId, node, true)
        );
    }

    public void testAllocateCommand() {
        AllocationService allocation = createAllocationService(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build());
        final String index = "test";

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        // shard routing is added as "from recovery" instead of "new index creation" so that we can test below that allocating an empty
        // primary with accept_data_loss flag set to false fails
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(index))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();
        final ShardId shardId = new ShardId(metaData.index(index).getIndex(), 0);

        logger.info("--> adding 3 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .put(newNode("node3"))
                .put(newNode("node4", singleton(DiscoveryNode.Role.MASTER)))
        ).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating to non-existent node, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(randomAllocateCommand(index, shardId.id(), "node42")), false, false);
            fail("expected IllegalArgumentException when allocating to non-existing node");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("failed to resolve [node42], no matching nodes"));
        }

        logger.info("--> allocating to non-data node, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(randomAllocateCommand(index, shardId.id(), "node4")), false, false);
            fail("expected IllegalArgumentException when allocating to non-data node");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("allocation can only be done on data nodes"));
        }

        logger.info("--> allocating non-existing shard, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(randomAllocateCommand("test", 1, "node2")), false, false);
            fail("expected ShardNotFoundException when allocating non-existing shard");
        } catch (ShardNotFoundException e) {
            assertThat(e.getMessage(), containsString("no such shard"));
        }

        logger.info("--> allocating non-existing index, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(randomAllocateCommand("test2", 0, "node2")), false, false);
            fail("expected ShardNotFoundException when allocating non-existing index");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), containsString("no such index"));
        }

        logger.info("--> allocating empty primary with acceptDataLoss flag set to false");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", false)), false, false);
            fail("expected IllegalArgumentException when allocating empty primary with acceptDataLoss flag set to false");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("allocating an empty primary for " + shardId + " can result in data loss. Please confirm by setting the accept_data_loss parameter to true"));
        }

        logger.info("--> allocating stale primary with acceptDataLoss flag set to false");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateStalePrimaryAllocationCommand(index, shardId.id(), "node1", false)), false, false);
            fail("expected IllegalArgumentException when allocating stale primary with acceptDataLoss flag set to false");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("allocating an empty primary for " + shardId + " can result in data loss. Please confirm by setting the accept_data_loss parameter to true"));
        }

        logger.info("--> allocating empty primary with acceptDataLoss flag set to true");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> start the primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> allocate the replica shard on the primary shard node, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node1")), false, false);
            fail("expected IllegalArgumentException when allocating replica shard on the primary shard node");
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));


        logger.info("--> start the replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> verify that we fail when there are no unassigned shards");
        try {
            allocation.reroute(clusterState, new AllocationCommands(randomAllocateCommand("test", 0, "node3")), false, false);
            fail("expected IllegalArgumentException when allocating shard while no unassigned shard available");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testCancelCommand() {
        AllocationService allocation = createAllocationService(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .put(newNode("node3"))
        ).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating empty primary shard with accept_data_loss flag set to true");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", false)), false, false);
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> start the primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", false)), false, false);
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the relocation allocation");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the primary being replicated, make sure it fails");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", false)), false, false);
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> start the replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> cancel allocation of the replica shard");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)), false, false);
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(rerouteResult.changed(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));
        logger.info("--> start the replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> move the replica shard");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")), false, false);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the move of the replica shard");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node3", false)), false, false);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> move the replica shard again");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")), false, false);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the source replica shard");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)), false, false);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).get(0).relocatingNodeId(), nullValue());

        logger.info("--> start the former target replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(STARTED).size(), equalTo(1));


        logger.info("--> cancel the primary allocation (with allow_primary set to true)");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", true)), false, false);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(rerouteResult.changed(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(STARTED).iterator().next().primary(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
    }

    public void testSerialization() throws Exception {
        AllocationCommands commands = new AllocationCommands(
                new AllocateEmptyPrimaryAllocationCommand("test", 1, "node1", true),
                new AllocateStalePrimaryAllocationCommand("test", 2, "node1", true),
                new AllocateReplicaAllocationCommand("test", 2, "node1"),
                new MoveAllocationCommand("test", 3, "node2", "node3"),
                new CancelAllocationCommand("test", 4, "node5", true)
        );
        BytesStreamOutput bytes = new BytesStreamOutput();
        AllocationCommands.writeTo(commands, bytes);
        StreamInput in = StreamInput.wrap(bytes.bytes());

        // Since the commands are named writeable we need to register them and wrap the input stream
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        new NetworkModule(null, Settings.EMPTY, true, namedWriteableRegistry);
        in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);

        // Now we can read them!
        AllocationCommands sCommands = AllocationCommands.readFrom(in);

        assertThat(sCommands.commands().size(), equalTo(5));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).shardId(), equalTo(1));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).index(), equalTo("test"));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).node(), equalTo("node1"));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).acceptDataLoss(), equalTo(true));

        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).shardId(), equalTo(2));
        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).index(), equalTo("test"));
        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).node(), equalTo("node1"));
        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).acceptDataLoss(), equalTo(true));

        assertThat(((AllocateReplicaAllocationCommand) (sCommands.commands().get(2))).shardId(), equalTo(2));
        assertThat(((AllocateReplicaAllocationCommand) (sCommands.commands().get(2))).index(), equalTo("test"));
        assertThat(((AllocateReplicaAllocationCommand) (sCommands.commands().get(2))).node(), equalTo("node1"));

        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).shardId(), equalTo(3));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).index(), equalTo("test"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).fromNode(), equalTo("node2"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).toNode(), equalTo("node3"));

        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).shardId(), equalTo(4));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).index(), equalTo("test"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).node(), equalTo("node5"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).allowPrimary(), equalTo(true));
    }

    public void testXContent() throws Exception {
        String commands = "{\n" +
                "    \"commands\" : [\n" +
                "        {\"allocate_empty_primary\" : {\"index\" : \"test\", \"shard\" : 1, \"node\" : \"node1\", \"accept_data_loss\" : true}}\n" +
                "       ,{\"allocate_stale_primary\" : {\"index\" : \"test\", \"shard\" : 2, \"node\" : \"node1\", \"accept_data_loss\" : true}}\n" +
                "       ,{\"allocate_replica\" : {\"index\" : \"test\", \"shard\" : 2, \"node\" : \"node1\"}}\n" +
                "       ,{\"move\" : {\"index\" : \"test\", \"shard\" : 3, \"from_node\" : \"node2\", \"to_node\" : \"node3\"}} \n" +
                "       ,{\"cancel\" : {\"index\" : \"test\", \"shard\" : 4, \"node\" : \"node5\", \"allow_primary\" : true}} \n" +
                "    ]\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(commands);
        // move two tokens, parser expected to be "on" `commands` field
        parser.nextToken();
        parser.nextToken();
        AllocationCommandRegistry registry = new NetworkModule(null, Settings.EMPTY, true, new NamedWriteableRegistry())
                .getAllocationCommandRegistry();
        AllocationCommands sCommands = AllocationCommands.fromXContent(parser, ParseFieldMatcher.STRICT, registry);

        assertThat(sCommands.commands().size(), equalTo(5));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).shardId(), equalTo(1));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).index(), equalTo("test"));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).node(), equalTo("node1"));
        assertThat(((AllocateEmptyPrimaryAllocationCommand) (sCommands.commands().get(0))).acceptDataLoss(), equalTo(true));

        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).shardId(), equalTo(2));
        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).index(), equalTo("test"));
        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).node(), equalTo("node1"));
        assertThat(((AllocateStalePrimaryAllocationCommand) (sCommands.commands().get(1))).acceptDataLoss(), equalTo(true));

        assertThat(((AllocateReplicaAllocationCommand) (sCommands.commands().get(2))).shardId(), equalTo(2));
        assertThat(((AllocateReplicaAllocationCommand) (sCommands.commands().get(2))).index(), equalTo("test"));
        assertThat(((AllocateReplicaAllocationCommand) (sCommands.commands().get(2))).node(), equalTo("node1"));

        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).shardId(), equalTo(3));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).index(), equalTo("test"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).fromNode(), equalTo("node2"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(3))).toNode(), equalTo("node3"));

        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).shardId(), equalTo(4));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).index(), equalTo("test"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).node(), equalTo("node5"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(4))).allowPrimary(), equalTo(true));
    }
}
