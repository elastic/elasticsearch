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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class AllocationCommandsTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(AllocationCommandsTests.class);

    @Test
    public void moveShardCommand() {
        AllocationService allocation = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
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
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new MoveAllocationCommand(new ShardId("test", 0), existingNodeId, toNodeId)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node(existingNodeId).get(0).state(), equalTo(ShardRoutingState.RELOCATING));
        assertThat(clusterState.getRoutingNodes().node(toNodeId).get(0).state(), equalTo(ShardRoutingState.INITIALIZING));

        logger.info("finish moving the shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.getRoutingNodes().node(existingNodeId).isEmpty(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node(toNodeId).get(0).state(), equalTo(ShardRoutingState.STARTED));
    }

    @Test
    public void allocateCommand() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE, "none")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .put(newNode("node3"))
                .put(newNode("node4", ImmutableMap.of("data", Boolean.FALSE.toString())))
        ).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating with primary flag set to false, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", false)));
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocating to non-data node, should fail");
        try {
            rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node4", true)));
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocating with primary flag set to true");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true)));
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
            allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", false)));
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
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
            allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node3", false)));
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void cancelCommand() {
        AllocationService allocation = createAllocationService(settingsBuilder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE, "none")
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .put(newNode("node3"))
        ).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating with primary flag set to true");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", false)));
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
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", false)));
            fail();
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the relocation allocation");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the primary being replicated, make sure it fails");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", false)));
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
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
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
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new MoveAllocationCommand(new ShardId("test", 0), "node2", "node3")));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the move of the replica shard");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node3", false)));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));


        logger.info("--> cancel the primary allocation (with allow_primary set to true)");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", true)));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(rerouteResult.changed(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).get(0).primary(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));
    }

    @Test
    public void serialization() throws Exception {
        AllocationCommands commands = new AllocationCommands(
                new AllocateAllocationCommand(new ShardId("test", 1), "node1", true),
                new MoveAllocationCommand(new ShardId("test", 3), "node2", "node3"),
                new CancelAllocationCommand(new ShardId("test", 4), "node5", true)
        );
        BytesStreamOutput bytes = new BytesStreamOutput();
        AllocationCommands.writeTo(commands, bytes);
        AllocationCommands sCommands = AllocationCommands.readFrom(StreamInput.wrap(bytes.bytes()));

        assertThat(sCommands.commands().size(), equalTo(3));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).shardId(), equalTo(new ShardId("test", 1)));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).node(), equalTo("node1"));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).allowPrimary(), equalTo(true));

        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).shardId(), equalTo(new ShardId("test", 3)));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).fromNode(), equalTo("node2"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).toNode(), equalTo("node3"));

        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).shardId(), equalTo(new ShardId("test", 4)));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).node(), equalTo("node5"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).allowPrimary(), equalTo(true));
    }

    @Test
    public void xContent() throws Exception {
        String commands = "{\n" +
                "    \"commands\" : [\n" +
                "        {\"allocate\" : {\"index\" : \"test\", \"shard\" : 1, \"node\" : \"node1\", \"allow_primary\" : true}}\n" +
                "       ,{\"move\" : {\"index\" : \"test\", \"shard\" : 3, \"from_node\" : \"node2\", \"to_node\" : \"node3\"}} \n" +
                "       ,{\"cancel\" : {\"index\" : \"test\", \"shard\" : 4, \"node\" : \"node5\", \"allow_primary\" : true}} \n" +
                "    ]\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(commands);
        // move two tokens, parser expected to be "on" `commands` field
        parser.nextToken();
        parser.nextToken();
        AllocationCommands sCommands = AllocationCommands.fromXContent(parser);

        assertThat(sCommands.commands().size(), equalTo(3));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).shardId(), equalTo(new ShardId("test", 1)));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).node(), equalTo("node1"));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).allowPrimary(), equalTo(true));

        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).shardId(), equalTo(new ShardId("test", 3)));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).fromNode(), equalTo("node2"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).toNode(), equalTo("node3"));

        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).shardId(), equalTo(new ShardId("test", 4)));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).node(), equalTo("node5"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).allowPrimary(), equalTo(true));
    }
}
