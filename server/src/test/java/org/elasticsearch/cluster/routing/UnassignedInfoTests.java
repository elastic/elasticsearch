/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.REMOVE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.REPLACE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UnassignedInfoTests extends ESAllocationTestCase {

    public void testReasonOrdinalOrder() {
        UnassignedInfo.Reason[] order = new UnassignedInfo.Reason[] {
            UnassignedInfo.Reason.INDEX_CREATED,
            UnassignedInfo.Reason.CLUSTER_RECOVERED,
            UnassignedInfo.Reason.INDEX_REOPENED,
            UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED,
            UnassignedInfo.Reason.NEW_INDEX_RESTORED,
            UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
            UnassignedInfo.Reason.REPLICA_ADDED,
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            UnassignedInfo.Reason.NODE_LEFT,
            UnassignedInfo.Reason.REROUTE_CANCELLED,
            UnassignedInfo.Reason.REINITIALIZED,
            UnassignedInfo.Reason.REALLOCATED_REPLICA,
            UnassignedInfo.Reason.PRIMARY_FAILED,
            UnassignedInfo.Reason.FORCED_EMPTY_PRIMARY,
            UnassignedInfo.Reason.MANUAL_ALLOCATION,
            UnassignedInfo.Reason.INDEX_CLOSED,
            UnassignedInfo.Reason.NODE_RESTARTING,
            UnassignedInfo.Reason.UNPROMOTABLE_REPLICA,
            UnassignedInfo.Reason.RESHARD_ADDED };
        for (int i = 0; i < order.length; i++) {
            assertThat(order[i].ordinal(), equalTo(i));
        }
        assertThat(UnassignedInfo.Reason.values().length, equalTo(order.length));
    }

    public void testSerialization() throws Exception {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(random(), UnassignedInfo.Reason.values());
        int failedAllocations = randomIntBetween(1, 100);
        Set<String> failedNodes = IntStream.range(0, between(0, failedAllocations))
            .mapToObj(n -> "failed-node-" + n)
            .collect(Collectors.toSet());

        UnassignedInfo meta;
        if (reason == UnassignedInfo.Reason.ALLOCATION_FAILED) {
            meta = new UnassignedInfo(
                reason,
                randomBoolean() ? randomAlphaOfLength(4) : null,
                null,
                failedAllocations,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                AllocationStatus.NO_ATTEMPT,
                failedNodes,
                null
            );
        } else if (reason == UnassignedInfo.Reason.NODE_LEFT || reason == UnassignedInfo.Reason.NODE_RESTARTING) {
            String lastAssignedNodeId = randomAlphaOfLength(10);
            if (reason == UnassignedInfo.Reason.NODE_LEFT && randomBoolean()) {
                // If the reason is `NODE_LEFT`, sometimes we'll have an empty lastAllocatedNodeId due to BWC
                lastAssignedNodeId = null;
            }
            meta = new UnassignedInfo(
                reason,
                randomBoolean() ? randomAlphaOfLength(4) : null,
                null,
                0,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                AllocationStatus.NO_ATTEMPT,
                Set.of(),
                lastAssignedNodeId
            );
        } else {
            meta = new UnassignedInfo(reason, randomBoolean() ? randomAlphaOfLength(4) : null);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        meta.writeTo(out);
        out.close();

        UnassignedInfo read = UnassignedInfo.fromStreamInput(out.bytes().streamInput());
        assertThat(read.reason(), equalTo(meta.reason()));
        assertThat(read.unassignedTimeMillis(), equalTo(meta.unassignedTimeMillis()));
        assertThat(read.message(), equalTo(meta.message()));
        assertThat(read.details(), equalTo(meta.details()));
        assertThat(read.failedAllocations(), equalTo(meta.failedAllocations()));
        assertThat(read.failedNodeIds(), equalTo(meta.failedNodeIds()));
        assertThat(read.lastAllocatedNodeId(), equalTo(meta.lastAllocatedNodeId()));
    }

    public void testIndexCreated() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(metadata.getProject().index("test")).build()
            )
            .build();
        for (ShardRouting shard : shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED)) {
            assertThat(shard.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.INDEX_CREATED));
        }
    }

    public void testClusterRecovered() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsRecovery(metadata.getProject().index("test"))
                    .build()
            )
            .build();
        for (ShardRouting shard : shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED)) {
            assertThat(shard.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.CLUSTER_RECOVERED));
        }
    }

    public void testIndexClosedAndReopened() {
        final var allocationService = createAllocationService();

        // cluster state 0: index fully assigned and ready to close
        final var metadata0 = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();

        assertThat(metadata0.projects(), aMapWithSize(1));
        var entry = metadata0.projects().entrySet().iterator().next();
        final ProjectMetadata projectMetadata0 = entry.getValue();
        final ProjectId projectId0 = entry.getKey();

        final var clusterState0 = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node-1"))
                        .add(newNode("node-2"))
                        .add(newNode("node-3"))
                        .add(newNode("node-4"))
                        .add(newNode("node-5"))
                )
                .metadata(metadata0)
                .routingTable(
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(projectMetadata0.index("test")).build()
                )
                .build(),
            allocationService
        );
        assertTrue(clusterState0.routingTable().index("test").allShardsActive());

        // cluster state 1: perhaps start one of the shards relocating
        final var clusterState1 = randomBoolean()
            ? clusterState0
            : allocationService.executeWithRoutingAllocation(clusterState0, "test", routingAllocation -> {
                final var indexShardRoutingTable = routingAllocation.routingTable(projectId0).index("test").shard(0);
                for (DiscoveryNode node : routingAllocation.nodes()) {
                    if (routingAllocation.routingNodes().node(node.getId()).getByShardId(indexShardRoutingTable.shardId()) == null) {
                        routingAllocation.routingNodes()
                            .relocateShard(indexShardRoutingTable.shard(0), node.getId(), 0L, "test", routingAllocation.changes());
                        return;
                    }
                }
                throw new AssertionError("no suitable target found");
            });

        // cluster state 2: index closed and fully unassigned
        final var metadata1 = Metadata.builder(metadata0)
            .put(IndexMetadata.builder(projectMetadata0.index("test")).state(IndexMetadata.State.CLOSE))
            .build();
        final ProjectMetadata projectMetadata1 = metadata1.getProject();
        final var clusterState2 = ClusterState.builder(clusterState1)
            .metadata(metadata1)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState1.routingTable(projectMetadata1.id()))
                    .addAsFromOpenToClose(projectMetadata1.index("test"))
            )
            .build();

        assertLastAllocatedNodeIdsAssigned(
            UnassignedInfo.Reason.INDEX_CLOSED,
            clusterState1.routingTable().index("test"),
            clusterState2.routingTable().index("test")
        );

        // cluster state 3: closed index has been fully assigned
        final var clusterState3 = applyStartedShardsUntilNoChange(clusterState2, allocationService);
        assertTrue(clusterState3.routingTable().index("test").allShardsActive());

        // cluster state 4: index reopened, fully unassigned again
        final var metadata4 = Metadata.builder(metadata0)
            .put(IndexMetadata.builder(projectMetadata1.index("test")).state(IndexMetadata.State.OPEN))
            .build();
        final var clusterState4 = ClusterState.builder(clusterState3)
            .metadata(metadata4)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState3.routingTable())
                    .addAsFromCloseToOpen(metadata4.getProject().index("test"))
            )
            .build();

        assertLastAllocatedNodeIdsAssigned(
            UnassignedInfo.Reason.INDEX_REOPENED,
            clusterState3.routingTable().index("test"),
            clusterState4.routingTable().index("test")
        );
    }

    private void assertLastAllocatedNodeIdsAssigned(
        UnassignedInfo.Reason expectedUnassignedReason,
        IndexRoutingTable originalRoutingTable,
        IndexRoutingTable finalRoutingTable
    ) {
        final var shardCountChanged = originalRoutingTable.size() != finalRoutingTable.size()
            || originalRoutingTable.shard(0).size() != finalRoutingTable.shard(0).size();
        if (shardCountChanged) {
            assertThat(expectedUnassignedReason, equalTo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED));
        }

        boolean foundAnyNodeIds = false;
        for (int shardId = 0; shardId < finalRoutingTable.size(); shardId++) {
            final var previousShardRoutingTable = originalRoutingTable.shard(shardId);
            final var previousNodes = previousShardRoutingTable == null
                ? Set.<String>of()
                : IntStream.range(0, previousShardRoutingTable.size()).mapToObj(previousShardRoutingTable::shard).map(shard -> {
                    assertTrue(shard.started() || shard.relocating());
                    return shard.currentNodeId();
                }).collect(Collectors.toSet());
            final var shardRoutingTable = finalRoutingTable.shard(shardId);
            for (int shardCopy = 0; shardCopy < shardRoutingTable.size(); shardCopy++) {
                final var shard = shardRoutingTable.shard(shardCopy);
                assertTrue(shard.unassigned());
                assertThat(shard.unassignedInfo().reason(), equalTo(expectedUnassignedReason));
                final var lastAllocatedNodeId = shard.unassignedInfo().lastAllocatedNodeId();
                if (lastAllocatedNodeId == null) {
                    // restoring an index may change the number of shards/replicas so no guarantee that lastAllocatedNodeId is populated
                    assertTrue(shardCountChanged);
                } else {
                    foundAnyNodeIds = true;
                    assertThat(previousNodes, hasItem(lastAllocatedNodeId));
                }
            }
            if (shardCountChanged == false) {
                assertNotNull(previousShardRoutingTable);
                assertThat(
                    shardRoutingTable.primaryShard().unassignedInfo().lastAllocatedNodeId(),
                    equalTo(previousShardRoutingTable.primaryShard().currentNodeId())
                );
            }
        }

        // both original and restored index must have at least one shard tho
        assertTrue(foundAnyNodeIds);
    }

    public void testIndexReopened() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsFromCloseToOpen(metadata.getProject().index("test"))
                    .build()
            )
            .build();
        for (ShardRouting shard : shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED)) {
            assertThat(shard.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.INDEX_REOPENED));
        }
    }

    public void testNewIndexRestored() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsNewRestore(
                        metadata.getProject().index("test"),
                        new SnapshotRecoverySource(
                            UUIDs.randomBase64UUID(),
                            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
                            IndexVersion.current(),
                            new IndexId("test", UUIDs.randomBase64UUID(random()))
                        ),
                        new HashSet<>()
                    )
                    .build()
            )
            .build();
        for (ShardRouting shard : shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED)) {
            assertThat(shard.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.NEW_INDEX_RESTORED));
        }
    }

    public void testExistingIndexRestored() {
        final var allocationService = createAllocationService();

        // cluster state 0: index fully assigned and ready to close
        final var metadata0 = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        final var clusterState0 = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node-1"))
                        .add(newNode("node-2"))
                        .add(newNode("node-3"))
                        .add(newNode("node-4"))
                        .add(newNode("node-5"))
                )
                .metadata(metadata0)
                .routingTable(
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                        .addAsNew(metadata0.getProject().index("test"))
                        .build()
                )
                .build(),
            allocationService
        );
        assertTrue(clusterState0.routingTable().index("test").allShardsActive());

        // cluster state 1: index closed and reassigned
        final var metadata1 = Metadata.builder(metadata0)
            .put(IndexMetadata.builder(metadata0.getProject().index("test")).state(IndexMetadata.State.CLOSE))
            .build();
        final var clusterState1 = ClusterState.builder(clusterState0)
            .metadata(metadata1)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState0.routingTable())
                    .addAsFromOpenToClose(metadata1.getProject().index("test"))
            )
            .build();

        assertLastAllocatedNodeIdsAssigned(
            UnassignedInfo.Reason.INDEX_CLOSED,
            clusterState0.routingTable().index("test"),
            clusterState1.routingTable().index("test")
        );

        // cluster state 2: closed index has been fully assigned
        final var clusterState2 = applyStartedShardsUntilNoChange(clusterState1, allocationService);
        assertTrue(clusterState2.routingTable().index("test").allShardsActive());

        // cluster state 3: restore started, fully unassigned again (NB may have different number of shards/replicas)
        final var metadata3 = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        final var clusterState3 = ClusterState.builder(clusterState2)
            .metadata(metadata3)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState2.routingTable())
                    .addAsRestore(
                        metadata3.getProject().index("test"),
                        new SnapshotRecoverySource(
                            UUIDs.randomBase64UUID(),
                            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
                            IndexVersion.current(),
                            new IndexId("test", UUIDs.randomBase64UUID(random()))
                        )
                    )
            )
            .build();

        assertLastAllocatedNodeIdsAssigned(
            UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
            clusterState2.routingTable().index("test"),
            clusterState3.routingTable().index("test")
        );
    }

    public void testDanglingIndexImported() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(randomIntBetween(1, 3))
                    .numberOfReplicas(randomIntBetween(0, 3))
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsFromDangling(metadata.getProject().index("test"))
                    .build()
            )
            .build();
        for (ShardRouting shard : shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED)) {
            assertThat(shard.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED));
        }
    }

    public void testReplicaAdded() {
        AllocationService allocation = createAllocationService();
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            .build();
        final Index index = metadata.getProject().index("test").getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(metadata.getProject().index(index)).build()
            )
            .build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            builder.addIndexShard(new IndexShardRoutingTable.Builder(indexRoutingTable.shard(i)));
        }
        builder.addReplica(ShardRouting.Role.DEFAULT);
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.builder(clusterState.routingTable()).add(builder).build())
            .build();
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().reason(),
            equalTo(UnassignedInfo.Reason.REPLICA_ADDED)
        );
    }

    /**
     * The unassigned meta is kept when a shard goes to INITIALIZING, but cleared when it moves to STARTED.
     */
    public void testStateTransitionMetaHandling() {
        ShardRouting shard = shardRoutingBuilder("test", 1, null, true, ShardRoutingState.UNASSIGNED).withUnassignedInfo(
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        ).build();
        assertThat(shard.unassignedInfo(), notNullValue());
        shard = shard.initialize("test_node", null, -1);
        assertThat(shard.state(), equalTo(ShardRoutingState.INITIALIZING));
        assertThat(shard.unassignedInfo(), notNullValue());
        shard = shard.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shard.unassignedInfo(), nullValue());
    }

    /**
     * Tests that during reroute when a node is detected as leaving the cluster, the right unassigned meta is set
     */
    public void testNodeLeave() {
        AllocationService allocation = createAllocationService();
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(metadata.getProject().index("test")).build()
            )
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        // verify that NODE_LEAVE is the reason for meta
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(true));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().reason(),
            equalTo(UnassignedInfo.Reason.NODE_LEFT)
        );
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().unassignedTimeMillis(),
            greaterThan(0L)
        );
    }

    /**
     * Verifies that when a shard fails, reason is properly set and details are preserved.
     */
    public void testFailedShard() {
        AllocationService allocation = createAllocationService();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata project = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putRoutingTable(
                projectId,
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(project.index("test")).build()
            )
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // fail shard
        ShardRouting shardToFail = shardsWithState(clusterState.getRoutingNodes(), STARTED).get(0);
        clusterState = allocation.applyFailedShards(
            clusterState,
            List.of(new FailedShard(shardToFail, "test fail", null, randomBoolean())),
            List.of()
        );
        // verify the reason and details
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(true));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().reason(),
            equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED)
        );
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().message(),
            equalTo("failed shard on node [" + shardToFail.currentNodeId() + "]: test fail")
        );
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().details(),
            equalTo("failed shard on node [" + shardToFail.currentNodeId() + "]: test fail")
        );
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0).unassignedInfo().unassignedTimeMillis(),
            greaterThan(0L)
        );
    }

    /**
     * Verifies that delayed allocation calculation are correct when there are no registered node shutdowns.
     */
    public void testRemainingDelayCalculationWithNoShutdowns() {
        checkRemainingDelayCalculation(
            "bogusNodeId",
            TimeValue.timeValueNanos(10),
            NodesShutdownMetadata.EMPTY,
            TimeValue.timeValueNanos(10),
            false
        );
    }

    /**
     * Verifies that delayed allocation calculations are correct when there are registered node shutdowns for nodes which are not relevant
     * to the shard currently being evaluated.
     */
    public void testRemainingDelayCalculationsWithUnrelatedShutdowns() {
        String lastNodeId = "bogusNodeId";
        NodesShutdownMetadata shutdowns = NodesShutdownMetadata.EMPTY;
        int numberOfShutdowns = randomIntBetween(1, 15);
        for (int i = 0; i <= numberOfShutdowns; i++) {
            final SingleNodeShutdownMetadata.Type type = randomFrom(EnumSet.allOf(SingleNodeShutdownMetadata.Type.class));
            final String targetNodeName = type == REPLACE ? randomAlphaOfLengthBetween(10, 20) : null;
            shutdowns = shutdowns.putSingleNodeMetadata(
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(randomValueOtherThan(lastNodeId, () -> randomAlphaOfLengthBetween(5, 10)))
                    .setNodeEphemeralId(randomValueOtherThan(lastNodeId, () -> randomAlphaOfLengthBetween(5, 10)))
                    .setReason(this.getTestName())
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setType(type)
                    .setTargetNodeName(targetNodeName)
                    .setGracePeriod(type == SIGTERM ? randomTimeValue() : null)
                    .build()
            );
        }
        checkRemainingDelayCalculation(lastNodeId, TimeValue.timeValueNanos(10), shutdowns, TimeValue.timeValueNanos(10), false);
    }

    /**
     * Verifies that delay calculation is not impacted when the node the shard was last assigned to was registered for removal.
     */
    public void testRemainingDelayCalculationWhenNodeIsShuttingDownForRemoval() {
        for (SingleNodeShutdownMetadata.Type type : List.of(REMOVE, SIGTERM)) {
            String lastNodeId = "bogusNodeId";
            NodesShutdownMetadata shutdowns = NodesShutdownMetadata.EMPTY.putSingleNodeMetadata(
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(lastNodeId)
                    .setNodeEphemeralId(lastNodeId)
                    .setReason(this.getTestName())
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setType(type)
                    .setGracePeriod(type == SIGTERM ? randomTimeValue() : null)
                    .build()
            );

            checkRemainingDelayCalculation(lastNodeId, TimeValue.timeValueNanos(10), shutdowns, TimeValue.timeValueNanos(10), false);
        }
    }

    /**
     * Verifies that the delay calculation uses the configured delay value for nodes known to be restarting, because they are registered for
     * a `RESTART`-type shutdown, rather than the default global delay.
     */
    public void testRemainingDelayCalculationWhenNodeIsKnownToBeRestartingWithCustomDelay() {
        String lastNodeId = "bogusNodeId";
        NodesShutdownMetadata shutdowns = NodesShutdownMetadata.EMPTY.putSingleNodeMetadata(
            SingleNodeShutdownMetadata.builder()
                .setNodeId(lastNodeId)
                .setNodeEphemeralId(lastNodeId)
                .setReason(this.getTestName())
                .setStartedAtMillis(randomNonNegativeLong())
                .setType(SingleNodeShutdownMetadata.Type.RESTART)
                .setAllocationDelay(TimeValue.timeValueMinutes(1))
                .build()
        );

        // Use a different index-level delay so this test will fail if that one gets used instead of the one from the shutdown metadata
        checkRemainingDelayCalculation(lastNodeId, TimeValue.timeValueNanos(10), shutdowns, TimeValue.timeValueMinutes(1), true);
    }

    /**
     * Verifies that the delay calculation uses the default delay value for nodes known to be restarting, because they are registered for
     * a `RESTART`-type shutdown, rather than the default global delay.
     */
    public void testRemainingDelayCalculationWhenNodeIsKnownToBeRestartingWithDefaultDelay() {
        String lastNodeId = "bogusNodeId";

        // Note that we do not explicitly configure the reallocation delay here.
        NodesShutdownMetadata shutdowns = NodesShutdownMetadata.EMPTY.putSingleNodeMetadata(
            SingleNodeShutdownMetadata.builder()
                .setNodeId(lastNodeId)
                .setNodeEphemeralId(lastNodeId)
                .setReason(this.getTestName())
                .setStartedAtMillis(randomNonNegativeLong())
                .setType(SingleNodeShutdownMetadata.Type.RESTART)
                .build()
        );

        // Use a different index-level delay so this test will fail if that one gets used instead of the one from the shutdown metadata
        checkRemainingDelayCalculation(
            lastNodeId,
            TimeValue.timeValueNanos(10),
            shutdowns,
            SingleNodeShutdownMetadata.DEFAULT_RESTART_SHARD_ALLOCATION_DELAY,
            true
        );
    }

    public void testRemainingDelayUsesIndexLevelDelayIfNodeWasNotRestartingWhenShardBecameUnassigned() {
        String lastNodeId = "bogusNodeId";

        // Generate a random time value - but don't use nanos as extremely small values of nanos can break assertion calculations
        final TimeValue shutdownDelay = randomTimeValue(
            100,
            1000,
            randomValueOtherThan(TimeUnit.NANOSECONDS, () -> randomFrom(TimeUnit.values()))
        );
        NodesShutdownMetadata shutdowns = NodesShutdownMetadata.EMPTY.putSingleNodeMetadata(
            SingleNodeShutdownMetadata.builder()
                .setNodeId(lastNodeId)
                .setNodeEphemeralId(lastNodeId)
                .setReason(this.getTestName())
                .setStartedAtMillis(randomNonNegativeLong())
                .setType(SingleNodeShutdownMetadata.Type.RESTART)
                .setAllocationDelay(shutdownDelay)
                .build()
        );

        // We want an index level delay that's less than the shutdown delay to avoid picking the index-level delay because it's larger
        final TimeValue indexLevelDelay = randomValueOtherThanMany(
            tv -> shutdownDelay.compareTo(tv) < 0,
            () -> randomTimeValue(1, 1000, randomValueOtherThan(TimeUnit.NANOSECONDS, () -> randomFrom(TimeUnit.values())))
        );

        logger.info("index level delay: {}, shutdown delay: {}", indexLevelDelay, shutdownDelay);
        checkRemainingDelayCalculation(lastNodeId, indexLevelDelay, shutdowns, indexLevelDelay, false);
    }

    private void checkRemainingDelayCalculation(
        String lastNodeId,
        TimeValue indexLevelTimeoutSetting,
        NodesShutdownMetadata nodeShutdowns,
        TimeValue expectedTotalDelay,
        boolean nodeRestarting
    ) {
        final long baseTime = System.nanoTime();
        UnassignedInfo unassignedInfo = new UnassignedInfo(
            nodeRestarting ? UnassignedInfo.Reason.NODE_RESTARTING : UnassignedInfo.Reason.NODE_LEFT,
            "test",
            null,
            0,
            baseTime,
            System.currentTimeMillis(),
            randomBoolean(),
            AllocationStatus.NO_ATTEMPT,
            Set.of(),
            lastNodeId
        );
        final long totalDelayNanos = expectedTotalDelay.nanos();
        final Settings indexSettings = Settings.builder()
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), indexLevelTimeoutSetting)
            .build();
        long delay = unassignedInfo.remainingDelay(baseTime, indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(totalDelayNanos));
        long delta1 = randomLongBetween(1, (totalDelayNanos - 1));
        delay = unassignedInfo.remainingDelay(baseTime + delta1, indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(totalDelayNanos - delta1));
        delay = unassignedInfo.remainingDelay(baseTime + totalDelayNanos, indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(0L));
        delay = unassignedInfo.remainingDelay(baseTime + totalDelayNanos + randomIntBetween(1, 20), indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(0L));
    }

    public void testNumberOfDelayedUnassigned() throws Exception {
        MockAllocationService allocation = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsNew(metadata.getProject().index("test1"))
                    .addAsNew(metadata.getProject().index("test2"))
                    .build()
            )
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(0));
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        // make sure both replicas are marked as delayed (i.e. not reallocated)
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        assertThat(clusterState.toString(), UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(2));
    }

    public void testFindNextDelayedAllocation() {
        MockAllocationService allocation = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        final TimeValue delayTest1 = TimeValue.timeValueMillis(randomIntBetween(1, 200));
        final TimeValue delayTest2 = TimeValue.timeValueMillis(randomIntBetween(1, 200));
        final long expectMinDelaySettingsNanos = Math.min(delayTest1.nanos(), delayTest2.nanos());

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(IndexVersion.current()).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayTest1)
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .put(
                IndexMetadata.builder("test2")
                    .settings(
                        settings(IndexVersion.current()).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayTest2)
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsNew(metadata.getProject().index("test1"))
                    .addAsNew(metadata.getProject().index("test2"))
                    .build()
            )
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(0));
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        final long baseTime = System.nanoTime();
        allocation.setNanoTimeOverride(baseTime);
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        final long delta = randomBoolean() ? 0 : randomInt((int) expectMinDelaySettingsNanos - 1);

        if (delta > 0) {
            allocation.setNanoTimeOverride(baseTime + delta);
            clusterState = allocation.reroute(clusterState, "time moved", ActionListener.noop());
        }

        assertThat(UnassignedInfo.findNextDelayedAllocation(baseTime + delta, clusterState), equalTo(expectMinDelaySettingsNanos - delta));
    }

    public void testAllocationStatusSerialization() throws IOException {
        for (AllocationStatus allocationStatus : AllocationStatus.values()) {
            BytesStreamOutput out = new BytesStreamOutput();
            allocationStatus.writeTo(out);
            ByteBufferStreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(out.bytes().toBytesRef().bytes));
            AllocationStatus readStatus = AllocationStatus.readFrom(in);
            assertThat(readStatus, equalTo(allocationStatus));
        }
    }

    public static UnassignedInfo randomUnassignedInfo(String message) {
        return randomUnassignedInfo(message, null);
    }

    /**
     * Randomly generates an UnassignedInfo.
     * @param message The message to be used.
     * @param delayed Used for the `delayed` flag if provided.
     * @return A randomly-generated UnassignedInfo with the given message and delayed value (if any)
     */
    public static UnassignedInfo randomUnassignedInfo(String message, @Nullable Boolean delayed) {
        UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
        String lastAllocatedNodeId = null;
        boolean delayedFlag = delayed == null ? false : delayed;
        if (reason == UnassignedInfo.Reason.NODE_LEFT || reason == UnassignedInfo.Reason.NODE_RESTARTING) {
            if (randomBoolean() && delayed == null) {
                delayedFlag = true;
            }
            lastAllocatedNodeId = randomIdentifier();
        }
        return new UnassignedInfo(
            reason,
            message,
            null,
            reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0,
            System.nanoTime(),
            System.currentTimeMillis(),
            delayedFlag,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? Set.of(randomIdentifier()) : Set.of(),
            lastAllocatedNodeId
        );
    }

    public void testSummaryContainsImportantFields() {
        var info = randomUnassignedInfo(randomBoolean() ? randomIdentifier() : null);
        var summary = info.shortSummary();

        assertThat("reason", summary, containsString("[reason=" + info.reason() + ']'));
        assertThat(
            "delay",
            summary,
            containsString("at[" + UnassignedInfo.DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(info.unassignedTimeMillis())) + ']')
        );
        if (info.failedAllocations() > 0) {
            assertThat("failed_allocations", summary, containsString("failed_attempts[" + info.failedAllocations() + ']'));
        }
        if (info.failedNodeIds().isEmpty() == false) {
            assertThat("failed_nodes", summary, containsString("failed_nodes[" + info.failedNodeIds() + ']'));
        }
        assertThat("delayed", summary, containsString("delayed=" + info.delayed()));
        if (info.lastAllocatedNodeId() != null) {
            assertThat("last_node", summary, containsString("last_node[" + info.lastAllocatedNodeId() + ']'));
        }
        if (info.message() != null) {
            assertThat("details", summary, containsString("details[" + info.message() + ']'));
        }
        assertThat("allocation_status", summary, containsString("allocation_status[" + info.lastAllocationStatus().value() + ']'));
    }
}
