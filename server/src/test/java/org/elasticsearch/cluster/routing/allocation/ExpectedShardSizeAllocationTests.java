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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ExpectedShardSizeAllocationTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ExpectedShardSizeAllocationTests.class);

    public void testInitializingHasExpectedSize() {
        final long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
        final ClusterInfo clusterInfo = createClusterInfoWith(new ShardId("test", "_na_", 0), byteSize);
        AllocationService strategy = createAllocationService(Settings.EMPTY, () -> clusterInfo);

        logger.info("Building initial routing table");
        var indexMetadata = IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        logger.info("Adding one node and performing rerouting");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertEquals(1, clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING));
        assertEquals(
            byteSize,
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).getExpectedShardSize()
        );
        logger.info("Start the primary shard");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertEquals(1, clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED));
        assertEquals(1, clusterState.getRoutingNodes().unassigned().size());

        logger.info("Add another one node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertEquals(1, clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING));
        assertEquals(
            byteSize,
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).getExpectedShardSize()
        );
    }

    public void testExpectedSizeOnMove() {
        final long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
        final ClusterInfo clusterInfo = createClusterInfoWith(new ShardId("test", "_na_", 0), byteSize);
        final AllocationService allocation = createAllocationService(Settings.EMPTY, () -> clusterInfo);
        logger.info("creating an index with 1 shard, no replica");
        var indexMetadata = IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        logger.info("move the shard");
        String existingNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String toNodeId = "node1".equals(existingNodeId) ? "node2" : "node1";

        AllocationService.CommandsResult commandsResult = allocation.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, existingNodeId, toNodeId)),
            false,
            false,
            false,
            ActionListener.noop()
        );
        assertThat(commandsResult.clusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.clusterState();
        assertEquals(clusterState.getRoutingNodes().node(existingNodeId).iterator().next().state(), ShardRoutingState.RELOCATING);
        assertEquals(clusterState.getRoutingNodes().node(toNodeId).iterator().next().state(), ShardRoutingState.INITIALIZING);

        assertEquals(clusterState.getRoutingNodes().node(existingNodeId).iterator().next().getExpectedShardSize(), byteSize);
        assertEquals(clusterState.getRoutingNodes().node(toNodeId).iterator().next().getExpectedShardSize(), byteSize);

        logger.info("finish moving the shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        assertThat(clusterState.getRoutingNodes().node(existingNodeId).isEmpty(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node(toNodeId).iterator().next().state(), equalTo(ShardRoutingState.STARTED));
        assertEquals(clusterState.getRoutingNodes().node(toNodeId).iterator().next().getExpectedShardSize(), -1);
    }

    private static ClusterInfo createClusterInfoWith(ShardId shardId, long size) {
        return new ClusterInfo(
            Map.of(),
            Map.of(),
            Map.ofEntries(
                Map.entry(ClusterInfo.shardIdentifierFromRouting(shardId, true), size),
                Map.entry(ClusterInfo.shardIdentifierFromRouting(shardId, false), size)
            ),
            Map.of(),
            Map.of(),
            Map.of()
        );
    }
}
