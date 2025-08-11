/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.NodeReplacementAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeShutdownAllocationDecider;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.mockito.internal.util.collections.Sets;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TransportGetShutdownStatusActionTests extends ESTestCase {
    public static final String SHUTTING_DOWN_NODE_ID = "node1";
    public static final String LIVE_NODE_ID = "node2";
    public static final String OTHER_LIVE_NODE_ID = "node3";

    private final AtomicReference<TestDecider> canAllocate = new AtomicReference<>();
    private final AtomicReference<TestDecider> canRemain = new AtomicReference<>();

    private ClusterInfoService clusterInfoService;
    private AllocationDeciders allocationDeciders;
    private AllocationService allocationService;
    private SnapshotsInfoService snapshotsInfoService;

    @FunctionalInterface
    private interface TestDecider {
        Decision test(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation);
    }

    @Before
    private void setup() {
        canAllocate.set((r, n, a) -> { throw new UnsupportedOperationException("canAllocate not initiated in this test"); });
        canRemain.set((r, n, a) -> { throw new UnsupportedOperationException("canRemain not initiated in this test"); });

        clusterInfoService = EmptyClusterInfoService.INSTANCE;
        allocationDeciders = new AllocationDeciders(
            org.elasticsearch.core.List.of(
                new NodeShutdownAllocationDecider(),
                new NodeReplacementAllocationDecider(),
                new AllocationDecider() {
                    @Override
                    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                        return canAllocate.get().test(shardRouting, node, allocation);
                    }

                    @Override
                    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                        // No behavior should change based on rebalance decisions
                        return Decision.NO;
                    }

                    @Override
                    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                        return canRemain.get().test(shardRouting, node, allocation);
                    }

                    @Override
                    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                        // No behavior relevant to these tests should change based on auto expansion decisions
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Decision canRebalance(RoutingAllocation allocation) {
                        // No behavior should change based on rebalance decisions
                        return Decision.NO;
                    }
                }
            )
        );
        snapshotsInfoService = () -> new SnapshotShardSizeInfo(
            new ImmutableOpenMap.Builder<InternalSnapshotsInfoService.SnapshotShard, Long>().build()
        );
        allocationService = new AllocationService(
            allocationDeciders,
            new BalancedShardsAllocator(Settings.EMPTY),
            clusterInfoService,
            snapshotsInfoService
        );
        allocationService.setExistingShardsAllocators(
            Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, new TestGatewayAllocator())
        );
    }

    /**
     * Ensures we have sane behavior if there are no indices anywhere.
     */
    public void testEmptyCluster() {
        RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        ClusterState state = createTestClusterState(routingTable, Collections.emptyList(), SingleNodeShutdownMetadata.Type.REMOVE);

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            false,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(status, SingleNodeShutdownMetadata.Status.COMPLETE, 0, nullValue());
    }

    /**
     * Restart-type shutdowns don't migrate shards, so we should always report the migration status as complete
     */
    public void testRestartAlwaysComplete() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 2), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.STARTED))
            .build();

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.RESTART
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.RESTART,
            randomBoolean(), // Whether the node has been seen doesn't matter, restart-type shutdowns should always say COMPLETE here.
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(
            status,
            SingleNodeShutdownMetadata.Status.COMPLETE,
            0,
            is("no shard relocation is necessary for a node restart")
        );
    }

    /**
     * Only slightly more complex than the previous case, tests that things are correctly reported as COMPLETE if there are no shards left
     * on the node.
     */
    public void testComplete() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 2), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .build();

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(status, SingleNodeShutdownMetadata.Status.COMPLETE, 0, nullValue());
    }

    /**
     * Ensures that we properly detect "in progress" migrations while there are shards relocating off the node that's shutting down.
     */
    public void testInProgressWithRelocatingShards() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(
                TestShardRouting.newShardRouting(
                    new ShardId(index, 1),
                    SHUTTING_DOWN_NODE_ID,
                    LIVE_NODE_ID,
                    true,
                    ShardRoutingState.RELOCATING
                )
            )
            .addShard(
                TestShardRouting.newShardRouting(
                    new ShardId(index, 2),
                    SHUTTING_DOWN_NODE_ID,
                    true,
                    randomFrom(ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING) // Should have 2 shards remaining either way
                )
            )
            .build();

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(status, SingleNodeShutdownMetadata.Status.IN_PROGRESS, 2, nullValue());
    }

    /**
     * Ensures that we correctly report "in progress", not "stalled", when there are no shards relocating off the node that's shutting down,
     * but there is nothing preventing shards from moving other than that there are already too many ongoing recoveries.
     */
    public void testInProgressWithShardsMovingBetweenOtherNodes() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(
                TestShardRouting.newShardRouting(
                    new ShardId(index, 1),
                    OTHER_LIVE_NODE_ID,
                    LIVE_NODE_ID,
                    true,
                    ShardRoutingState.RELOCATING
                )
            )
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 2), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.STARTED))
            .build();

        // This canAllocate decider simulates the ThrottlingAllocationDecider if LIVE_NODE has a maximum incoming recoveries of 1
        canAllocate.set((r, n, a) -> {
            if (n.nodeId().equals(LIVE_NODE_ID)) {
                return Decision.THROTTLE;
            } else if (n.nodeId().equals(SHUTTING_DOWN_NODE_ID)) {
                return Decision.NO;
            } else {
                return Decision.YES;
            }
        });
        // And the remain decider simulates NodeShutdownAllocationDecider
        canRemain.set((r, n, a) -> n.nodeId().equals(SHUTTING_DOWN_NODE_ID) ? Decision.NO : Decision.YES);

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(status, SingleNodeShutdownMetadata.Status.IN_PROGRESS, 1, nullValue());
    }

    /**
     * Ensure we can detect stalled migrations, defined as the inability to move remaining shards off a shutting-down node due to allocation
     * deciders.
     */
    public void testStalled() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 2), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.STARTED))
            .build();

        // Force a decision of NO for all moves and new allocations, simulating a decider that's stuck
        canAllocate.set((r, n, a) -> Decision.NO);
        // And the remain decider simulates NodeShutdownAllocationDecider
        canRemain.set((r, n, a) -> n.nodeId().equals(SHUTTING_DOWN_NODE_ID) ? Decision.NO : Decision.YES);

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(
            status,
            SingleNodeShutdownMetadata.Status.STALLED,
            1,
            allOf(containsString(index.getName()), containsString("[2] [primary]"))
        );
    }

    /**
     * Ensure we can detect stalled migrations when we have unassigned shards that had the shutting down node as their last known
     * node id
     */
    public void testStalledUnassigned() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = spy(generateIndexMetadata(index, 3, 0));
        // make sure the TestGatewayAllocator stays in sync always, avoid flaky tests
        doAnswer(i -> {
            if ((Integer) i.getArgument(0) < 2) {
                return Sets.newSet(LIVE_NODE_ID);
            }
            return Sets.newSet(SHUTTING_DOWN_NODE_ID);
        }).when(imd).inSyncAllocationIds(anyInt());

        ShardRouting shard0 = TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED);
        ShardRouting shard1 = TestShardRouting.newShardRouting(new ShardId(index, 1), LIVE_NODE_ID, true, ShardRoutingState.STARTED);

        // we should stall the node if we find an unassigned shard with lastAllocatedNodeId matching the shutting down node
        ShardRouting unassigned = makeUnassignedShard(index, 2, SHUTTING_DOWN_NODE_ID, true);

        assertShardMigration(
            getUnassignedShutdownStatus(index, imd, shard0, shard1, unassigned),
            SingleNodeShutdownMetadata.Status.STALLED,
            1,
            allOf(containsString(index.getName()), containsString("[2] [primary]"))
        );

        // if the shard is unassigned, but it's not a primary on this node, we shouldn't stall
        ShardRouting shard2 = TestShardRouting.newShardRouting(new ShardId(index, 2), LIVE_NODE_ID, true, ShardRoutingState.STARTED);
        ShardRouting unassignedReplica = makeUnassignedShard(index, 2, SHUTTING_DOWN_NODE_ID, false);

        ShutdownShardMigrationStatus s = getUnassignedShutdownStatus(index, imd, shard0, shard1, shard2, unassignedReplica);
        assertShardMigration(s, SingleNodeShutdownMetadata.Status.COMPLETE, 0, nullValue());

        // check if we correctly count all of the unassigned shards
        ShardRouting unassigned3 = makeUnassignedShard(index, 3, SHUTTING_DOWN_NODE_ID, true);

        assertShardMigration(
            getUnassignedShutdownStatus(index, imd, shard0, shard1, unassigned3, unassigned),
            SingleNodeShutdownMetadata.Status.STALLED,
            2,
            allOf(containsString(index.getName()), containsString("[primary]"), containsString("is unassigned"))
        );

        // check if we correctly walk all of the unassigned shards, shard 2 replica, shard 3 primary
        assertShardMigration(
            getUnassignedShutdownStatus(index, imd, shard0, shard1, shard2, unassignedReplica, unassigned3),
            SingleNodeShutdownMetadata.Status.STALLED,
            1,
            allOf(containsString(index.getName()), containsString("[3] [primary]"))
        );
    }

    public void testNotStalledIfAllShardsHaveACopyOnAnotherNode() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, false, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.STARTED))
            .build();

        // Force a decision of NO for all moves and new allocations, simulating a decider that's stuck
        canAllocate.set((r, n, a) -> Decision.NO);
        // And the remain decider simulates NodeShutdownAllocationDecider
        canRemain.set((r, n, a) -> n.nodeId().equals(SHUTTING_DOWN_NODE_ID) ? Decision.NO : Decision.YES);

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(
            status,
            SingleNodeShutdownMetadata.Status.COMPLETE,
            0,
            containsString("[1] shards cannot be moved away from this node but have at least one copy on another node in the cluster")
        );
    }

    public void testOnlyInitializingShardsRemaining() {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = generateIndexMetadata(index, 3, 0);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.INITIALIZING))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 2), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.INITIALIZING))
            .build();

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(
            status,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            2,
            equalTo("all remaining shards are currently INITIALIZING and must finish before they can be moved off this node")
        );
    }

    public void testNodeNotInCluster() {
        String bogusNodeId = randomAlphaOfLength(10);
        ImmutableOpenMap.Builder<String, IndexMetadata> indicesTable = ImmutableOpenMap.<String, IndexMetadata>builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .indices(indicesTable.build())
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                bogusNodeId,
                                SingleNodeShutdownMetadata.builder()
                                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setReason(this.getTestName())
                                    .setNodeId(bogusNodeId)
                                    .build()
                            )
                        )
                    )
            )
            .nodes(
                DiscoveryNodes.builder()
                    .add(
                        DiscoveryNode.createLocal(
                            Settings.builder()
                                .put(Settings.builder().build())
                                .put(Node.NODE_NAME_SETTING.getKey(), SHUTTING_DOWN_NODE_ID)
                                .build(),
                            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                            SHUTTING_DOWN_NODE_ID
                        )
                    )
                    .add(
                        DiscoveryNode.createLocal(
                            Settings.builder().put(Settings.builder().build()).put(Node.NODE_NAME_SETTING.getKey(), LIVE_NODE_ID).build(),
                            new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                            LIVE_NODE_ID
                        )
                    )
                    .add(
                        DiscoveryNode.createLocal(
                            Settings.builder()
                                .put(Settings.builder().build())
                                .put(Node.NODE_NAME_SETTING.getKey(), OTHER_LIVE_NODE_ID)
                                .build(),
                            new TransportAddress(TransportAddress.META_ADDRESS, 9202),
                            OTHER_LIVE_NODE_ID
                        )
                    )
            )
            .routingTable(routingTable.build())
            .build();

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            bogusNodeId,
            SingleNodeShutdownMetadata.Type.REMOVE,
            false,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(status, SingleNodeShutdownMetadata.Status.NOT_STARTED, 0, is("node is not currently part of the cluster"));
    }

    public void testIlmShrinkingIndexAvoidsStall() {
        LifecycleExecutionState executionState = LifecycleExecutionState.builder()
            .setAction(ShrinkAction.NAME)
            .setStep(ShrinkStep.NAME)
            .setPhase(randomFrom("hot", "warm"))
            .build();
        checkStalledShardWithIlmState(executionState, OperationMode.RUNNING, SingleNodeShutdownMetadata.Status.IN_PROGRESS);
    }

    public void testIlmShrinkingWithIlmStoppingIndexAvoidsStall() {
        LifecycleExecutionState executionState = LifecycleExecutionState.builder()
            .setAction(ShrinkAction.NAME)
            .setStep(ShrinkStep.NAME)
            .build();
        checkStalledShardWithIlmState(executionState, OperationMode.STOPPING, SingleNodeShutdownMetadata.Status.IN_PROGRESS);
    }

    public void testIlmShrinkingButIlmStoppedDoesNotAvoidStall() {
        LifecycleExecutionState executionState = LifecycleExecutionState.builder()
            .setAction(ShrinkAction.NAME)
            .setStep(ShrinkStep.NAME)
            .build();
        checkStalledShardWithIlmState(executionState, OperationMode.STOPPED, SingleNodeShutdownMetadata.Status.STALLED);
    }

    public void testIlmShrinkingButErroredDoesNotAvoidStall() {
        LifecycleExecutionState executionState = LifecycleExecutionState.builder()
            .setAction(ShrinkAction.NAME)
            .setStep(ErrorStep.NAME)
            .build();
        checkStalledShardWithIlmState(
            executionState,
            randomFrom(OperationMode.STOPPING, OperationMode.RUNNING),
            SingleNodeShutdownMetadata.Status.STALLED
        );
    }

    public void testIlmInAnyOtherActionDoesNotAvoidStall() {
        Set<String> allOtherIlmActions = new HashSet<>();
        allOtherIlmActions.addAll(TimeseriesLifecycleType.ORDERED_VALID_HOT_ACTIONS);
        allOtherIlmActions.addAll(TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS);
        allOtherIlmActions.addAll(TimeseriesLifecycleType.ORDERED_VALID_COLD_ACTIONS);
        allOtherIlmActions.addAll(TimeseriesLifecycleType.ORDERED_VALID_FROZEN_ACTIONS);
        allOtherIlmActions.addAll(TimeseriesLifecycleType.ORDERED_VALID_DELETE_ACTIONS);
        allOtherIlmActions.remove(ShrinkAction.NAME);
        for (String action : allOtherIlmActions) {
            LifecycleExecutionState executionState = LifecycleExecutionState.builder().setAction(action).build();
            checkStalledShardWithIlmState(
                executionState,
                randomFrom(OperationMode.STOPPING, OperationMode.RUNNING),
                SingleNodeShutdownMetadata.Status.STALLED
            );
        }
    }

    private void checkStalledShardWithIlmState(
        LifecycleExecutionState executionState,
        OperationMode operationMode,
        SingleNodeShutdownMetadata.Status expectedStatus
    ) {
        Index index = new Index(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata imd = IndexMetadata.builder(generateIndexMetadata(index, 3, 0))
            .putCustom(ILM_CUSTOM_METADATA_KEY, executionState.asMap())
            .build();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), LIVE_NODE_ID, true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 2), SHUTTING_DOWN_NODE_ID, true, ShardRoutingState.STARTED))
            .build();

        // Force a decision of NO for all moves and new allocations, simulating a decider that's stuck
        canAllocate.set((r, n, a) -> Decision.NO);
        // And the remain decider simulates NodeShutdownAllocationDecider
        canRemain.set((r, n, a) -> n.nodeId().equals(SHUTTING_DOWN_NODE_ID) ? Decision.NO : Decision.YES);

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            org.elasticsearch.core.List.of(imd),
            SingleNodeShutdownMetadata.Type.REMOVE
        );
        state = setIlmOperationMode(state, operationMode);

        ShutdownShardMigrationStatus status = TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );

        assertShardMigration(
            status,
            expectedStatus,
            1,
            expectedStatus == SingleNodeShutdownMetadata.Status.STALLED
                ? allOf(containsString(index.getName()), containsString("[2] [primary]"))
                : nullValue()
        );
    }

    private IndexMetadata generateIndexMetadata(Index index, int numberOfShards, int numberOfReplicas) {
        return IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            )
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();
    }

    private void assertShardMigration(
        ShutdownShardMigrationStatus status,
        SingleNodeShutdownMetadata.Status expectedStatus,
        long expectedShardsRemaining,
        Matcher<? super String> explanationMatcher
    ) {
        assertThat(status.getStatus(), equalTo(expectedStatus));
        assertThat(status.getShardsRemaining(), equalTo(expectedShardsRemaining));
        assertThat(status.getExplanation(), explanationMatcher);
    }

    private ClusterState createTestClusterState(
        RoutingTable indexRoutingTable,
        List<IndexMetadata> indices,
        SingleNodeShutdownMetadata.Type shutdownType
    ) {
        return createTestClusterState(indexRoutingTable, indices, shutdownType, false);
    }

    private ClusterState createTestClusterState(
        RoutingTable indexRoutingTable,
        List<IndexMetadata> indices,
        SingleNodeShutdownMetadata.Type shutdownType,
        boolean shuttingDownNodeAlreadyLeft
    ) {
        ImmutableOpenMap.Builder<String, IndexMetadata> indicesTable = ImmutableOpenMap.<String, IndexMetadata>builder();
        indices.forEach(imd -> { indicesTable.fPut(imd.getIndex().getName(), imd); });

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder()
            .add(
                DiscoveryNode.createLocal(
                    Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), LIVE_NODE_ID).build(),
                    new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                    LIVE_NODE_ID
                )
            )
            .add(
                DiscoveryNode.createLocal(
                    Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), OTHER_LIVE_NODE_ID).build(),
                    new TransportAddress(TransportAddress.META_ADDRESS, 9202),
                    OTHER_LIVE_NODE_ID
                )
            );
        if (shuttingDownNodeAlreadyLeft == false) {
            discoveryNodesBuilder.add(
                DiscoveryNode.createLocal(
                    Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), SHUTTING_DOWN_NODE_ID).build(),
                    new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                    SHUTTING_DOWN_NODE_ID
                )
            );
        }

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .indices(indicesTable.build())
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                SHUTTING_DOWN_NODE_ID,
                                SingleNodeShutdownMetadata.builder()
                                    .setType(shutdownType)
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setReason(this.getTestName())
                                    .setNodeId(SHUTTING_DOWN_NODE_ID)
                                    .build()
                            )
                        )
                    )
            )
            .nodes(discoveryNodesBuilder)
            .routingTable(indexRoutingTable)
            .build();
    }

    private ClusterState setIlmOperationMode(ClusterState state, OperationMode operationMode) {
        IndexLifecycleMetadata ilmState = state.metadata().custom(IndexLifecycleMetadata.TYPE);

        return ClusterState.builder(state)
            .metadata(
                Metadata.builder(state.metadata())
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(ilmState != null ? ilmState.getPolicyMetadatas() : Collections.emptyMap(), operationMode)
                    )
            )
            .build();
    }

    private static class TestSnapshotInfoService implements SnapshotsInfoService {
        @Override
        public SnapshotShardSizeInfo snapshotShardSizes() {
            return new SnapshotShardSizeInfo(new ImmutableOpenMap.Builder<InternalSnapshotsInfoService.SnapshotShard, Long>().build());
        }
    }

    private UnassignedInfo makeUnassignedInfo(String nodeId) {
        return new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            "testing",
            null,
            1,
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            nodeId
        );
    }

    private ShardRouting makeUnassignedShard(Index index, int shardId, String nodeId, boolean primary) {
        UnassignedInfo unsignedInfo = makeUnassignedInfo(nodeId);

        return TestShardRouting.newShardRouting(
            new ShardId(index, shardId),
            null,
            null,
            primary,
            ShardRoutingState.UNASSIGNED,
            unsignedInfo
        );
    }

    private ShutdownShardMigrationStatus getUnassignedShutdownStatus(Index index, IndexMetadata imd, ShardRouting... shards) {
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);

        for (ShardRouting routing : shards) {
            indexRoutingTableBuilder.addShard(routing);
        }

        IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();

        // Force a decision of NO for all moves and new allocations, simulating a decider that's stuck
        canAllocate.set((r, n, a) -> Decision.NO);
        // And the remain decider simulates NodeShutdownAllocationDecider
        canRemain.set((r, n, a) -> n.nodeId().equals(SHUTTING_DOWN_NODE_ID) ? Decision.NO : Decision.YES);

        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.add(indexRoutingTable);
        ClusterState state = createTestClusterState(
            routingTable.build(),
            Arrays.asList(imd),
            SingleNodeShutdownMetadata.Type.REMOVE,
            randomBoolean()
        );

        return TransportGetShutdownStatusAction.shardMigrationStatus(
            state,
            SHUTTING_DOWN_NODE_ID,
            SingleNodeShutdownMetadata.Type.REMOVE,
            true,
            clusterInfoService,
            snapshotsInfoService,
            allocationService,
            allocationDeciders
        );
    }
}
