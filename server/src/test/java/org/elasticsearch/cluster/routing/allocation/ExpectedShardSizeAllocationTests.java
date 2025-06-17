/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ExpectedShardSizeAllocationTests extends ESAllocationTestCase {

    public void testAllocateToCorrectNodeUsingShardSizeFromClusterInfo() {

        var indexMetadata = IndexMetadata.builder("test").settings(indexSettings(IndexVersion.current(), 1, 0)).build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .build();
        var dataNodeIds = clusterState.nodes().getDataNodes().keySet();

        long shardSize = ByteSizeValue.ofGb(1).getBytes();
        long diskSize = ByteSizeValue.ofGb(5).getBytes();
        long headRoom = diskSize / 10;
        var expectedNodeId = randomFrom(dataNodeIds);
        var clusterInfo = createClusterInfo(
            createDiskUsage(
                dataNodeIds,
                nodeId -> createDiskUsage(nodeId, diskSize, headRoom + shardSize + (Objects.equals(nodeId, expectedNodeId) ? +1 : -1))
            ),
            Map.of(ClusterInfo.shardIdentifierFromRouting(new ShardId(indexMetadata.getIndex(), 0), true), shardSize)
        );

        AllocationService service = createAllocationService(Settings.EMPTY, () -> clusterInfo);
        clusterState = service.reroute(clusterState, "reroute", ActionListener.noop());

        assertThatShard(
            clusterState.routingTable().index(indexMetadata.getIndex()).shard(0).primaryShard(),
            INITIALIZING,
            expectedNodeId,
            shardSize
        );
    }

    public void testAllocateToCorrectNodeAccordingToSnapshotShardInfo() {

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");
        var restoreId = "restore-id";

        var indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putInSyncAllocationIds(0, Set.of(randomUUID()))
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsRestore(
                        indexMetadata,
                        new RecoverySource.SnapshotRecoverySource(restoreId, snapshot, IndexVersion.current(), indexId)
                    )
            )
            .customs(
                Map.of(
                    RestoreInProgress.TYPE,
                    new RestoreInProgress.Builder().add(
                        new RestoreInProgress.Entry(
                            restoreId,
                            snapshot,
                            RestoreInProgress.State.STARTED,
                            false,
                            List.of(indexMetadata.getIndex().getName()),
                            Map.of(new ShardId(indexMetadata.getIndex(), 0), new RestoreInProgress.ShardRestoreStatus(randomIdentifier()))
                        )
                    ).build()
                )
            )
            .build();
        var dataNodeIds = clusterState.nodes().getDataNodes().keySet();

        long shardSize = ByteSizeValue.ofGb(1).getBytes();
        long diskSize = ByteSizeValue.ofGb(5).getBytes();
        long headRoom = diskSize / 10;
        var expectedNodeId = randomFrom(dataNodeIds);
        var clusterInfo = createClusterInfo(
            createDiskUsage(
                dataNodeIds,
                nodeId -> createDiskUsage(nodeId, diskSize, headRoom + shardSize + (Objects.equals(nodeId, expectedNodeId) ? +1 : -1))
            ),
            Map.of()
        );
        var snapshotShardSizeInfo = new SnapshotShardSizeInfo(
            Map.of(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, new ShardId(indexMetadata.getIndex(), 0)), shardSize)
        );

        AllocationService service = createAllocationService(
            Settings.EMPTY,
            new TestGatewayAllocator(),
            () -> clusterInfo,
            () -> snapshotShardSizeInfo
        );
        clusterState = service.reroute(clusterState, "reroute", ActionListener.noop());

        assertThatShard(
            clusterState.routingTable().index(indexMetadata.getIndex()).shard(0).primaryShard(),
            INITIALIZING,
            expectedNodeId,
            shardSize
        );
    }

    private static void assertThatShard(ShardRouting shard, ShardRoutingState state, String nodeId, long expectedShardSize) {
        assertThat(shard.state(), equalTo(state));
        assertThat(shard.currentNodeId(), equalTo(nodeId));
        assertThat(shard.getExpectedShardSize(), equalTo(expectedShardSize));
    }

    private static Map<String, DiskUsage> createDiskUsage(Collection<String> nodeIds, Function<String, DiskUsage> diskUsageCreator) {
        return nodeIds.stream().collect(toMap(Function.identity(), diskUsageCreator));
    }

    private static DiskUsage createDiskUsage(String nodeId, long totalBytes, long freeBytes) {
        return new DiskUsage(nodeId, nodeId, "/data", totalBytes, freeBytes);
    }

    public void testInitializingHasExpectedSize() {
        final long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
        final ClusterInfo clusterInfo = createClusterInfoWith(new ShardId("test", "_na_", 0), byteSize);
        AllocationService strategy = createAllocationService(Settings.EMPTY, () -> clusterInfo);

        logger.info("Building initial routing table");
        var indexMetadata = IndexMetadata.builder("test").settings(indexSettings(IndexVersion.current(), 1, 1)).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        logger.info("Adding one node and performing rerouting");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertEquals(1, clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(INITIALIZING));
        assertEquals(byteSize, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(0).getExpectedShardSize());
        logger.info("Start the primary shard");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertEquals(1, clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED));
        assertEquals(1, clusterState.getRoutingNodes().unassigned().size());

        logger.info("Add another one node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertEquals(1, clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(INITIALIZING));
        assertEquals(byteSize, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(0).getExpectedShardSize());
    }

    public void testExpectedSizeOnMove() {
        final long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
        final ClusterInfo clusterInfo = createClusterInfoWith(new ShardId("test", "_na_", 0), byteSize);
        final AllocationService allocation = createAllocationService(Settings.EMPTY, () -> clusterInfo);
        logger.info("creating an index with 1 shard, no replica");
        var indexMetadata = IndexMetadata.builder("test").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
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
        assertEquals(clusterState.getRoutingNodes().node(toNodeId).iterator().next().state(), INITIALIZING);

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
            Map.of(),
            Map.of()
        );
    }

    private static ClusterInfo createClusterInfo(Map<String, DiskUsage> diskUsage, Map<String, Long> shardSizes) {
        return new ClusterInfo(diskUsage, diskUsage, shardSizes, Map.of(), Map.of(), Map.of(), Map.of());
    }
}
