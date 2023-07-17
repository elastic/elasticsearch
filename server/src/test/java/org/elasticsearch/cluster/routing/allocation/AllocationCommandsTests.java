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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AllocationCommandsTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(AllocationCommandsTests.class);

    public void testMoveShardCommand() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("creating an index with 1 shard, no replica");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

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
        ClusterState newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, existingNodeId, toNodeId)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
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
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build()
        );
        final String index = "test";

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(index)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Collections.singleton("asdf"))
                    .putInSyncAllocationIds(1, Collections.singleton("qwertz"))
            )
            .build();
        // shard routing is added as "from recovery" instead of "new index creation" so that we can test below that allocating an empty
        // primary with accept_data_loss flag set to false fails
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsRecovery(metadata.index(index))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        final ShardId shardId = new ShardId(metadata.index(index).getIndex(), 0);

        logger.info("--> adding 3 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1"))
                    .add(newNode("node2"))
                    .add(newNode("node3"))
                    .add(newNode("node4", singleton(DiscoveryNodeRole.MASTER_ROLE)))
            )
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating to non-existent node, should fail");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(randomAllocateCommand(index, shardId.id(), "node42")),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected IllegalArgumentException when allocating to non-existing node");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("failed to resolve [node42], no matching nodes"));
        }

        logger.info("--> allocating to non-data node, should fail");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(randomAllocateCommand(index, shardId.id(), "node4")),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected IllegalArgumentException when allocating to non-data node");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("allocation can only be done on data nodes"));
        }

        logger.info("--> allocating non-existing shard, should fail");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(randomAllocateCommand("test", 1, "node2")),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected ShardNotFoundException when allocating non-existing shard");
        } catch (ShardNotFoundException e) {
            assertThat(e.getMessage(), containsString("no such shard"));
        }

        logger.info("--> allocating non-existing index, should fail");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(randomAllocateCommand("test2", 0, "node2")),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected ShardNotFoundException when allocating non-existing index");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), containsString("no such index [test2]"));
        }

        logger.info("--> allocating empty primary with acceptDataLoss flag set to false");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", false)),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected IllegalArgumentException when allocating empty primary with acceptDataLoss flag set to false");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                containsString(
                    "allocating an empty primary for "
                        + shardId
                        + " can result in data loss. Please confirm by setting the accept_data_loss parameter to true"
                )
            );
        }

        logger.info("--> allocating stale primary with acceptDataLoss flag set to false");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(new AllocateStalePrimaryAllocationCommand(index, shardId.id(), "node1", false)),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected IllegalArgumentException when allocating stale primary with acceptDataLoss flag set to false");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                containsString(
                    "allocating an empty primary for "
                        + shardId
                        + " can result in data loss. Please confirm by setting the accept_data_loss parameter to true"
                )
            );
        }

        logger.info("--> allocating empty primary with acceptDataLoss flag set to true");
        ClusterState newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> start the primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> allocate the replica shard on the primary shard node, should fail");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node1")),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected IllegalArgumentException when allocating replica shard on the primary shard node");
        } catch (IllegalArgumentException e) {}

        logger.info("--> allocate the replica shard on the second node");
        newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(INITIALIZING), equalTo(1));

        logger.info("--> start the replica shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("--> verify that we fail when there are no unassigned shards");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(randomAllocateCommand("test", 0, "node3")),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail("expected IllegalArgumentException when allocating shard while no unassigned shard available");
        } catch (IllegalArgumentException e) {}
    }

    public void testAllocateStalePrimaryCommand() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build()
        );
        final String index = "test";

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(index)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Collections.singleton("asdf"))
                    .putInSyncAllocationIds(1, Collections.singleton("qwertz"))
            )
            .build();
        // shard routing is added as "from recovery" instead of "new index creation" so that we can test below that allocating an empty
        // primary with accept_data_loss flag set to false fails
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsRecovery(metadata.index(index))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        final String node1 = "node1";
        final String node2 = "node2";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode(node1)).add(newNode(node2))).build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        // mark all shards as stale
        final List<ShardRouting> shardRoutings = shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED);
        assertThat(shardRoutings, hasSize(2));

        logger.info("--> allocating empty primary with acceptDataLoss flag set to true");
        clusterState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateStalePrimaryAllocationCommand(index, 0, node1, true)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        RoutingNode routingNode1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNode1.size(), equalTo(1));
        assertThat(routingNode1.numberOfShardsWithState(INITIALIZING), equalTo(1));
        Set<String> inSyncAllocationIds = clusterState.metadata().index(index).inSyncAllocationIds(0);
        assertThat(inSyncAllocationIds, equalTo(Collections.singleton(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)));

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        routingNode1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNode1.size(), equalTo(1));
        assertThat(routingNode1.numberOfShardsWithState(STARTED), equalTo(1));
        inSyncAllocationIds = clusterState.metadata().index(index).inSyncAllocationIds(0);
        assertThat(inSyncAllocationIds, hasSize(1));
        assertThat(inSyncAllocationIds, not(Collections.singleton(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)));
    }

    public void testCancelCommand() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build()
        );

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating empty primary shard with accept_data_loss flag set to true");
        ClusterState newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", false)),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail();
        } catch (IllegalArgumentException e) {}

        logger.info("--> start the primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", false)),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail();
        } catch (IllegalArgumentException e) {}

        logger.info("--> allocate the replica shard on on the second node");
        newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(INITIALIZING), equalTo(1));

        logger.info("--> cancel the relocation allocation");
        newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(INITIALIZING), equalTo(1));

        logger.info("--> cancel the primary being replicated, make sure it fails");
        try {
            allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", false)),
                false,
                false,
                false,
                ActionListener.noop()
            );
            fail();
        } catch (IllegalArgumentException e) {}

        logger.info("--> start the replica shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("--> cancel allocation of the replica shard");
        newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        newState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand("test", 0, "node2")),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(INITIALIZING), equalTo(1));
        logger.info("--> start the replica shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("--> move the replica shard");
        clusterState = allocation.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(RELOCATING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));

        if (randomBoolean()) {
            logger.info("--> cancel the primary allocation (with allow_primary set to true)");
            newState = allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", true)),
                false,
                false,
                false,
                ActionListener.noop()
            ).clusterState();
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState(STARTED).findFirst().get().primary(), equalTo(true));
            assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));
        } else {
            logger.info("--> cancel the move of the replica shard");
            clusterState = allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node3", false)),
                false,
                false,
                false,
                ActionListener.noop()
            ).clusterState();
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(1));

            logger.info("--> move the replica shard again");
            clusterState = allocation.reroute(
                clusterState,
                new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")),
                false,
                false,
                false,
                ActionListener.noop()
            ).clusterState();
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(RELOCATING), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));

            logger.info("--> cancel the source replica shard");
            clusterState = allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node2", false)),
                false,
                false,
                false,
                ActionListener.noop()
            ).clusterState();
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
            assertThat(
                clusterState.getRoutingNodes().node("node3").shardsWithState(INITIALIZING).findFirst().get().relocatingNodeId(),
                nullValue()
            );

            logger.info("--> start the former target replica shard");
            clusterState = startInitializingShardsAndReroute(allocation, clusterState);
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node3").numberOfShardsWithState(STARTED), equalTo(1));

            logger.info("--> cancel the primary allocation (with allow_primary set to true)");
            newState = allocation.reroute(
                clusterState,
                new AllocationCommands(new CancelAllocationCommand("test", 0, "node1", true)),
                false,
                false,
                false,
                ActionListener.noop()
            ).clusterState();
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState(STARTED).findFirst().get().primary(), equalTo(true));
            assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        }
    }

    public void testCanceledShardIsInitializedRespectingAllocationDeciders() {

        var allocationId1 = AllocationId.newInitializing(UUIDs.randomBase64UUID());
        var allocationId2 = AllocationId.newInitializing(UUIDs.randomBase64UUID());

        var indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 1).put("index.routing.allocation.exclude._id", "node-0"))
            .putInSyncAllocationIds(0, Set.of(allocationId1.getId(), allocationId2.getId()))
            .build();
        var shardId = new ShardId(indexMetadata.getIndex(), 0);

        ShardRouting primary = newShardRouting(shardId, "node-0", null, true, STARTED, allocationId1);
        ShardRouting replica = newShardRouting(shardId, "node-1", null, false, STARTED, allocationId2);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node-0", Version.V_8_10_0))
                    .add(newNode("node-1", Version.V_8_9_0))
                    .add(newNode("node-2", Version.V_8_9_0))
            )
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(shardId.getIndex()).addShard(primary).addShard(replica)))
            .build();

        var allocation = createAllocationService();
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        if (allocation.shardsAllocator instanceof DesiredBalanceShardsAllocator dbsa) {
            // ShardAssignment still contains `node-0` even though `can_remain_decision=no` for it
            assertThat(dbsa.getDesiredBalance().getAssignment(shardId), equalTo(new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)));
        }

        clusterState = allocation.reroute(
            clusterState,
            new AllocationCommands(new CancelAllocationCommand(shardId.getIndexName(), 0, "node-0", true)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        assertThat(clusterState.getRoutingNodes().node("node-0").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node-1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node-2").numberOfShardsWithState(STARTED), equalTo(1));

        if (allocation.shardsAllocator instanceof DesiredBalanceShardsAllocator dbsa) {
            assertThat(dbsa.getDesiredBalance().getAssignment(shardId), equalTo(new ShardAssignment(Set.of("node-1", "node-2"), 2, 0, 0)));
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
        assertThat(AllocationCommands.readFrom(in), equalTo(commands));
    }

    public void testXContent() throws Exception {
        String commands = """
            {
               "commands": [
                 {
                   "allocate_empty_primary": {
                     "index": "test",
                     "shard": 1,
                     "node": "node1",
                     "accept_data_loss": true
                   }
                 },
                 {
                   "allocate_stale_primary": {
                     "index": "test",
                     "shard": 2,
                     "node": "node1",
                     "accept_data_loss": true
                   }
                 },
                 {
                   "allocate_replica": {
                     "index": "test",
                     "shard": 2,
                     "node": "node1"
                   }
                 },
                 {
                   "move": {
                     "index": "test",
                     "shard": 3,
                     "from_node": "node2",
                     "to_node": "node3"
                   }
                 },
                 {
                   "cancel": {
                     "index": "test",
                     "shard": 4,
                     "node": "node5",
                     "allow_primary": true
                   }
                 }
               ]
             }
            """;
        XContentParser parser = createParser(JsonXContent.jsonXContent, commands);
        // move two tokens, parser expected to be "on" `commands` field
        parser.nextToken();
        parser.nextToken();

        assertThat(
            AllocationCommands.fromXContent(parser),
            equalTo(
                new AllocationCommands(
                    new AllocateEmptyPrimaryAllocationCommand("test", 1, "node1", true),
                    new AllocateStalePrimaryAllocationCommand("test", 2, "node1", true),
                    new AllocateReplicaAllocationCommand("test", 2, "node1"),
                    new MoveAllocationCommand("test", 3, "node2", "node3"),
                    new CancelAllocationCommand("test", 4, "node5", true)
                )
            )
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(NetworkModule.getNamedXContents());
    }

    public void testMoveShardToNonDataNode() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("creating an index with 1 shard, no replica");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes");

        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            "node1",
            "node1",
            "test1",
            "test1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            MASTER_DATA_ROLES,
            null
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            "node2",
            "node2",
            "test2",
            "test2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            new HashSet<>(randomSubsetOf(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INGEST_ROLE))),
            null
        );

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node1).add(node2)).build();

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        Index index = clusterState.getMetadata().index("test").getIndex();
        MoveAllocationCommand command = new MoveAllocationCommand(index.getName(), 0, "node1", "node2");
        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        logger.info("--> executing move allocation command to non-data node");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> command.execute(routingAllocation, false));
        assertEquals(
            "[move_allocation] can't move [test][0] from "
                + node1
                + " to "
                + node2
                + ": source ["
                + node2.getName()
                + "] is not a data node.",
            e.getMessage()
        );
    }

    public void testMoveShardFromNonDataNode() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("creating an index with 1 shard, no replica");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes");

        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            "node1",
            "node1",
            "test1",
            "test1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            MASTER_DATA_ROLES,
            null
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            "node2",
            "node2",
            "test2",
            "test2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            new HashSet<>(randomSubsetOf(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INGEST_ROLE))),
            null
        );

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node1).add(node2)).build();
        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        Index index = clusterState.getMetadata().index("test").getIndex();
        MoveAllocationCommand command = new MoveAllocationCommand(index.getName(), 0, "node2", "node1");
        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        logger.info("--> executing move allocation command from non-data node");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> command.execute(routingAllocation, false));
        assertEquals(
            "[move_allocation] can't move [test][0] from "
                + node2
                + " to "
                + node1
                + ": source ["
                + node2.getName()
                + "] is not a data node.",
            e.getMessage()
        );
    }

    public void testConflictingCommandsInSingleRequest() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .build()
        );

        final String index1 = "test1";
        final String index2 = "test2";
        final String index3 = "test3";
        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(index1)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Collections.singleton("randomAllocID"))
                    .putInSyncAllocationIds(1, Collections.singleton("randomAllocID2"))
            )
            .put(
                IndexMetadata.builder(index2)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Collections.singleton("randomAllocID"))
                    .putInSyncAllocationIds(1, Collections.singleton("randomAllocID2"))
            )
            .put(
                IndexMetadata.builder(index3)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Collections.singleton("randomAllocID"))
                    .putInSyncAllocationIds(1, Collections.singleton("randomAllocID2"))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsRecovery(metadata.index(index1))
            .addAsRecovery(metadata.index(index2))
            .addAsRecovery(metadata.index(index3))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        final String node1 = "node1";
        final String node2 = "node2";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode(node1)).add(newNode(node2))).build();
        final ClusterState finalClusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("--> allocating same index primary in multiple commands should fail");
        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            allocation.reroute(
                finalClusterState,
                new AllocationCommands(
                    new AllocateStalePrimaryAllocationCommand(index1, 0, node1, true),
                    new AllocateStalePrimaryAllocationCommand(index1, 0, node2, true)
                ),
                false,
                false,
                false,
                ActionListener.noop()
            );
        }).getMessage(), containsString("primary [" + index1 + "][0] is already assigned"));

        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            allocation.reroute(
                finalClusterState,
                new AllocationCommands(
                    new AllocateEmptyPrimaryAllocationCommand(index2, 0, node1, true),
                    new AllocateEmptyPrimaryAllocationCommand(index2, 0, node2, true)
                ),
                false,
                false,
                false,
                ActionListener.noop()
            );
        }).getMessage(), containsString("primary [" + index2 + "][0] is already assigned"));

        clusterState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand(index3, 0, node1, true)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        final ClusterState updatedClusterState = clusterState;
        assertThat(updatedClusterState.getRoutingNodes().node(node1).numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("--> subsequent replica allocation fails as all configured replicas have been allocated");
        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            allocation.reroute(
                updatedClusterState,
                new AllocationCommands(
                    new AllocateReplicaAllocationCommand(index3, 0, node2),
                    new AllocateReplicaAllocationCommand(index3, 0, node2)
                ),
                false,
                false,
                false,
                ActionListener.noop()
            );
        }).getMessage(), containsString("all copies of [" + index3 + "][0] are already assigned. Use the move allocation command instead"));
    }
}
