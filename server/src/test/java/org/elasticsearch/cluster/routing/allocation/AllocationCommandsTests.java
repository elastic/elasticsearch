/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AllocationCommandsTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(AllocationCommandsTests.class);

    public void testMoveShardCommand() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        logger.info("move the shard");
        String existingNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String toNodeId;
        if ("node1".equals(existingNodeId)) {
            toNodeId = "node2";
        } else {
            toNodeId = "node1";
        }
        ClusterState newState = allocation.reroute(clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, existingNodeId, toNodeId)),
            false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node(existingNodeId).iterator().next().state(), equalTo(ShardRoutingState.RELOCATING));
        assertThat(clusterState.getRoutingNodes().node(toNodeId).iterator().next().state(), equalTo(ShardRoutingState.INITIALIZING));

        logger.info("finish moving the shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

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
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Collections.singleton("asdf"))
                    .putInSyncAllocationIds(1, Collections.singleton("qwertz")))
                .build();
        // shard routing is added as "from recovery" instead of "new index creation" so that we can test below that allocating an empty
        // primary with accept_data_loss flag set to false fails
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metadata.index(index))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();
        final ShardId shardId = new ShardId(metadata.index(index).getIndex(), 0);

        logger.info("--> adding 3 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
                .add(newNode("node3"))
                .add(newNode("node4", singleton(DiscoveryNodeRole.MASTER_ROLE)))
        ).build();
        clusterState = allocation.reroute(clusterState, "reroute");
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
            assertThat(e.getMessage(), containsString("no such index [test2]"));
        }

        logger.info("--> allocating empty primary with acceptDataLoss flag set to false");
        try {
            allocation.reroute(clusterState, new AllocationCommands(
                new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", false)), false, false);
            fail("expected IllegalArgumentException when allocating empty primary with acceptDataLoss flag set to false");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("allocating an empty primary for " + shardId +
                " can result in data loss. Please confirm by setting the accept_data_loss parameter to true"));
        }

        logger.info("--> allocating stale primary with acceptDataLoss flag set to false");
        try {
            allocation.reroute(clusterState, new AllocationCommands(
                new AllocateStalePrimaryAllocationCommand(index, shardId.id(), "node1", false)), false, false);
            fail("expected IllegalArgumentException when allocating stale primary with acceptDataLoss flag set to false");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("allocating an empty primary for " + shardId +
                " can result in data loss. Please confirm by setting the accept_data_loss parameter to true"));
        }

        logger.info("--> allocating empty primary with acceptDataLoss flag set to true");
        ClusterState newState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)),
            false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> start the primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> allocate the replica shard on the primary shard node, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(
                new AllocateReplicaAllocationCommand("test", 0, "node1")), false, false);
            fail("expected IllegalArgumentException when allocating replica shard on the primary shard node");
        } catch (IllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on the second node");
        newState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));


        logger.info("--> start the replica shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
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

    public void testAllocateStalePrimaryCommand() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build());
        final String index = "test";

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)
                .putInSyncAllocationIds(0, Collections.singleton("asdf")).putInSyncAllocationIds(1, Collections.singleton("qwertz")))
            .build();
        // shard routing is added as "from recovery" instead of "new index creation" so that we can test below that allocating an empty
        // primary with accept_data_loss flag set to false fails
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRecovery(metadata.index(index))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).build();

        final String node1 = "node1";
        final String node2 = "node2";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode(node1))
            .add(newNode(node2))
        ).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        // mark all shards as stale
        final List<ShardRouting> shardRoutings = clusterState.getRoutingNodes().shardsWithState(UNASSIGNED);
        assertThat(shardRoutings, hasSize(2));

        logger.info("--> allocating empty primary with acceptDataLoss flag set to true");
        clusterState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateStalePrimaryAllocationCommand(index, 0, node1, true)), false, false).getClusterState();
        RoutingNode routingNode1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNode1.size(), equalTo(1));
        assertThat(routingNode1.shardsWithState(INITIALIZING).size(), equalTo(1));
        Set<String> inSyncAllocationIds = clusterState.metadata().index(index).inSyncAllocationIds(0);
        assertThat(inSyncAllocationIds, equalTo(Collections.singleton(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)));

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        routingNode1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNode1.size(), equalTo(1));
        assertThat(routingNode1.shardsWithState(STARTED).size(), equalTo(1));
        inSyncAllocationIds = clusterState.metadata().index(index).inSyncAllocationIds(0);
        assertThat(inSyncAllocationIds, hasSize(1));
        assertThat(inSyncAllocationIds, not(Collections.singleton(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)));
    }

    public void testCancelCommand() {
        AllocationService allocation = createAllocationService(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build());

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1"))
                .add(newNode("node2"))
                .add(newNode("node3"))
        ).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating empty primary shard with accept_data_loss flag set to true");
        ClusterState newState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
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
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
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
        newState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the relocation allocation");
        newState = allocation.reroute(clusterState,
            new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        newState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
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
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> cancel allocation of the replica shard");
        newState = allocation.reroute(clusterState,
            new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        newState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")), false, false).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));
        logger.info("--> start the replica shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> move the replica shard");
        clusterState = allocation.reroute(clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")), false, false).getClusterState();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));

        if (randomBoolean()) {
            logger.info("--> cancel the primary allocation (with allow_primary set to true)");
            newState = allocation.reroute(clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", true)), false, false).getClusterState();
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).iterator().next().primary(), equalTo(true));
            assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));
        } else {
            logger.info("--> cancel the move of the replica shard");
            clusterState = allocation.reroute(clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node3", false)), false, false).getClusterState();
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

            logger.info("--> move the replica shard again");
            clusterState = allocation.reroute(clusterState,
                new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")), false, false).getClusterState();
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(RELOCATING).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));

            logger.info("--> cancel the source replica shard");
            clusterState = allocation.reroute(clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)), false, false).getClusterState();
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).get(0).relocatingNodeId(), nullValue());

            logger.info("--> start the former target replica shard");
            clusterState = startInitializingShardsAndReroute(allocation, clusterState);
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(STARTED).size(), equalTo(1));

            logger.info("--> cancel the primary allocation (with allow_primary set to true)");
            newState = allocation.reroute(clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", true)), false, false).getClusterState();
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(STARTED).iterator().next().primary(), equalTo(true));
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        }
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
        StreamInput in = bytes.bytes().streamInput();

        // Since the commands are named writeable we need to register them and wrap the input stream
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
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
            "        {\"allocate_empty_primary\" : {\"index\" : \"test\", \"shard\" : 1," +
            "         \"node\" : \"node1\", \"accept_data_loss\" : true}}\n" +
            "       ,{\"allocate_stale_primary\" : {\"index\" : \"test\", \"shard\" : 2," +
            "         \"node\" : \"node1\", \"accept_data_loss\" : true}}\n" +
            "       ,{\"allocate_replica\" : {\"index\" : \"test\", \"shard\" : 2, \"node\" : \"node1\"}}\n" +
            "       ,{\"move\" : {\"index\" : \"test\", \"shard\" : 3, \"from_node\" : \"node2\", \"to_node\" : \"node3\"}} \n" +
            "       ,{\"cancel\" : {\"index\" : \"test\", \"shard\" : 4, \"node\" : \"node5\", \"allow_primary\" : true}} \n" +
            "    ]\n" +
            "}\n";
        XContentParser parser = createParser(JsonXContent.jsonXContent, commands);
        // move two tokens, parser expected to be "on" `commands` field
        parser.nextToken();
        parser.nextToken();
        AllocationCommands sCommands = AllocationCommands.fromXContent(parser);

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

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(NetworkModule.getNamedXContents());
    }

    public void testMoveShardToNonDataNode() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes");

        DiscoveryNode node1 = new DiscoveryNode("node1", "node1", "node1", "test1", "test1", buildNewFakeTransportAddress(), emptyMap(),
            MASTER_DATA_ROLES, Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", "node2", "node2", "test2", "test2", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INGEST_ROLE))), Version.CURRENT);

        clusterState = ClusterState.builder(clusterState).nodes(
            DiscoveryNodes.builder()
                .add(node1)
                .add(node2)).build();

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        Index index = clusterState.getMetadata().index("test").getIndex();
        MoveAllocationCommand command = new MoveAllocationCommand(index.getName(), 0, "node1", "node2");
        RoutingAllocation routingAllocation = new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, System.nanoTime());
        logger.info("--> executing move allocation command to non-data node");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> command.execute(routingAllocation, false));
        assertEquals("[move_allocation] can't move [test][0] from " + node1 + " to " +
            node2 + ": source [" + node2.getName() + "] is not a data node.", e.getMessage());
    }

    public void testMoveShardFromNonDataNode() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes");

        DiscoveryNode node1 = new DiscoveryNode("node1", "node1", "node1", "test1", "test1", buildNewFakeTransportAddress(), emptyMap(),
            MASTER_DATA_ROLES, Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", "node2", "node2", "test2", "test2", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INGEST_ROLE))), Version.CURRENT);

        clusterState = ClusterState.builder(clusterState).nodes(
            DiscoveryNodes.builder()
                .add(node1)
                .add(node2)).build();
        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        Index index = clusterState.getMetadata().index("test").getIndex();
        MoveAllocationCommand command = new MoveAllocationCommand(index.getName(), 0, "node2", "node1");
        RoutingAllocation routingAllocation = new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, System.nanoTime());
        logger.info("--> executing move allocation command from non-data node");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> command.execute(routingAllocation, false));
        assertEquals("[move_allocation] can't move [test][0] from " + node2 + " to " + node1 +
            ": source [" + node2.getName() + "] is not a data node.", e.getMessage());
    }

    public void testConflictingCommandsInSingleRequest() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build());

        final String index1 = "test1";
        final String index2 = "test2";
        final String index3 = "test3";
        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(index1).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)
                .putInSyncAllocationIds(0, Collections.singleton("randomAllocID"))
                .putInSyncAllocationIds(1, Collections.singleton("randomAllocID2")))
            .put(IndexMetadata.builder(index2).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)
                .putInSyncAllocationIds(0, Collections.singleton("randomAllocID"))
                .putInSyncAllocationIds(1, Collections.singleton("randomAllocID2")))
            .put(IndexMetadata.builder(index3).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)
                .putInSyncAllocationIds(0, Collections.singleton("randomAllocID"))
                .putInSyncAllocationIds(1, Collections.singleton("randomAllocID2")))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRecovery(metadata.index(index1))
            .addAsRecovery(metadata.index(index2))
            .addAsRecovery(metadata.index(index3))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).build();

        final String node1 = "node1";
        final String node2 = "node2";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode(node1))
            .add(newNode(node2))
        ).build();
        final ClusterState finalClusterState = allocation.reroute(clusterState, "reroute");

        logger.info("--> allocating same index primary in multiple commands should fail");
        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            allocation.reroute(finalClusterState,
                new AllocationCommands(
                    new AllocateStalePrimaryAllocationCommand(index1, 0, node1, true),
                    new AllocateStalePrimaryAllocationCommand(index1, 0, node2, true)
                ), false, false);
        }).getMessage(), containsString("primary [" + index1 + "][0] is already assigned"));

        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            allocation.reroute(finalClusterState,
                new AllocationCommands(
                    new AllocateEmptyPrimaryAllocationCommand(index2, 0, node1, true),
                    new AllocateEmptyPrimaryAllocationCommand(index2, 0, node2, true)
                ), false, false);
        }).getMessage(), containsString("primary [" + index2 + "][0] is already assigned"));


        clusterState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand(index3, 0, node1, true)), false, false).getClusterState();
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        final ClusterState updatedClusterState = clusterState;
        assertThat(updatedClusterState.getRoutingNodes().node(node1).shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> subsequent replica allocation fails as all configured replicas have been allocated");
        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            allocation.reroute(updatedClusterState,
                new AllocationCommands(
                    new AllocateReplicaAllocationCommand(index3, 0, node2),
                    new AllocateReplicaAllocationCommand(index3, 0, node2)
                ), false, false);
        }).getMessage(), containsString("all copies of [" + index3 + "][0] are already assigned. Use the move allocation command instead"));
    }
}
