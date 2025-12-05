/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfo.NodeAndPath;
import org.elasticsearch.cluster.ClusterInfo.NodeAndShard;
import org.elasticsearch.cluster.ClusterInfo.ReservedSpace;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.time.TimeProviderUtils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService.SnapshotShard;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DesiredBalanceComputerTests extends ESAllocationTestCase {

    static final String TEST_INDEX = "test-index";

    public void testComputeBalance() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testStopsComputingWhenStale() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        // if the isFresh flag is false then we only do one iteration, allocating the primaries but not the replicas
        var desiredBalance0 = DesiredBalance.BECOME_MASTER_INITIAL;
        var desiredBalance1 = desiredBalanceComputer.compute(desiredBalance0, createInput(clusterState), queue(), input -> false);
        assertDesiredAssignments(
            desiredBalance1,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0"), 2, 1, 1),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0"), 2, 1, 1)
            )
        );

        // the next iteration allocates the replicas whether stale or fresh
        var desiredBalance2 = desiredBalanceComputer.compute(desiredBalance1, createInput(clusterState), queue(), input -> randomBoolean());
        assertDesiredAssignments(
            desiredBalance2,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testNoInfiniteLoopBetweenHotspotMitigationAndBalancing() {
        // This test demonstrates that the computer does not get stuck in an infinite loop when moveShards and balancer moving against
        // each other. This is done by configuring two shards each on its own node.
        // - Shard 0 with no write load on node-0 with node load 0.92 and queue latency 15s
        // - Shard 1 with some write load on node-1 with no node load nor queue latency
        // 1. MoveShard will want to move shard 0 off node-0 to node-1 for hot-spot mitigation.
        // 2. Balance will want to move shard 0 back to node-0 to spread the index. Balancer always picks shard 0 because it has
        // no write load thus write load decider says YES
        // The computation should stop after one round of moveShards + balance because hot-spot is considered mitigated after
        // a single shard movement and breaks the loop.
        final var initialState = createInitialClusterState(2, 2, 0);
        final var index = initialState.metadata().getProject(ProjectId.DEFAULT).index(TEST_INDEX).getIndex();
        final RoutingNodes routingNodes = initialState.getRoutingNodes().mutableCopy();
        final var changes = mock(RoutingChangesObserver.class);
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            routingNodes.startShard(
                iterator.initialize(shardRouting.shardId().id() == 0 ? "node-0" : "node-1", null, 0L, changes),
                changes,
                0L
            );
        }
        final var clusterState = rebuildRoutingTable(initialState, routingNodes);

        final var clusterInfo = ClusterInfo.builder()
            .shardWriteLoads(Map.of(new ShardId(index, 1), 0.004379))
            .nodeUsageStatsForThreadPools(
                Map.of(
                    "node-0",
                    new NodeUsageStatsForThreadPools(
                        "node-0",
                        Map.of("write", new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(4, 0.92f, 15732))

                    ),
                    "node-1",
                    new NodeUsageStatsForThreadPools(
                        "node-1",
                        Map.of("write", new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(4, 0.0f, 0))
                    )
                )
            )
            .build();

        final var settings = Settings.builder().put(WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(), "enabled").build();
        final var routingAllocation = routingAllocationWithDecidersOf(clusterState, clusterInfo, settings);
        final var input = new DesiredBalanceInput(42, routingAllocation, List.of());
        final var computer = createDesiredBalanceComputer(new BalancedShardsAllocator(settings));
        var balance = computer.compute(DesiredBalance.BECOME_MASTER_INITIAL, input, queue(), ignored -> true);
        // This is an unusual edge case that will result in both shards being moved to the non-hot-spotting node
        assertThat(balance.getAssignment(new ShardId(index, 0)).nodeIds(), equalTo(Set.of("node-1")));
        assertThat(balance.getAssignment(new ShardId(index, 1)).nodeIds(), equalTo(Set.of("node-1")));
    }

    public void testIgnoresOutOfScopePrimaries() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = mutateAllocationStatuses(createInitialClusterState(3));
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();
        var primaryShard = mutateAllocationStatus(clusterState.routingTable().index(TEST_INDEX).shard(0).primaryShard());

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState, primaryShard),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(
                    Set.of(),
                    2,
                    2,
                    clusterState.routingTable()
                        .index(TEST_INDEX)
                        .shard(0)
                        .replicaShards()
                        .get(0)
                        .unassignedInfo()
                        .lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO ? 1 : 2
                ),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testIgnoresOutOfScopeReplicas() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = mutateAllocationStatuses(createInitialClusterState(3));

        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();
        var originalReplicaShard = clusterState.routingTable().index(TEST_INDEX).shard(0).replicaShards().get(0);
        var replicaShard = mutateAllocationStatus(originalReplicaShard);

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState, replicaShard),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(
                    Set.of("node-0"),
                    2,
                    1,
                    originalReplicaShard.unassignedInfo().lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO ? 0 : 1
                ),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testAssignShardsToTheirPreviousLocationIfAvailable() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                iterator.updateUnassigned(
                    new UnassignedInfo(
                        UnassignedInfo.Reason.NODE_LEFT,
                        null,
                        null,
                        0,
                        0,
                        0,
                        false,
                        UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                        Set.of(),
                        "node-2"
                    ),
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    changes
                );
            }
        }
        clusterState = rebuildRoutingTable(clusterState, routingNodes);

        var ignored = randomBoolean()
            ? new ShardRouting[0]
            : new ShardRouting[] { clusterState.routingTable().index(TEST_INDEX).shard(0).primaryShard() };

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState, ignored),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-2", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testRespectsAssignmentOfUnknownPrimaries() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-2", null, 0L, changes);
                    case 2 -> routingNodes.startShard(iterator.initialize("node-2", null, 0L, changes), changes, 0L);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(iterator.initialize("node-1", null, 0L, changes), changes, 0L),
                        "node-2",
                        0L,
                        "test",
                        changes
                    );
                }
                break;
            }
        }
        clusterState = rebuildRoutingTable(clusterState, routingNodes);

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-2", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testRespectsAssignmentOfUnknownReplicas() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                routingNodes.startShard(iterator.initialize("node-2", null, 0L, changes), changes, 0L);
                break;
            }
        }
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0) {
                assert shardRouting.primary() == false;
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-0", null, 0L, changes);
                    case 2 -> routingNodes.startShard(iterator.initialize("node-0", null, 0L, changes), changes, 0L);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(iterator.initialize("node-1", null, 0L, changes), changes, 0L),
                        "node-0",
                        0L,
                        "test",
                        changes
                    );
                }
                break;
            }
        }
        clusterState = rebuildRoutingTable(clusterState, routingNodes);

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-2", "node-0"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testRespectsAssignmentByGatewayAllocators() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
        for (var iterator = routingAllocation.routingNodes().unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                routingAllocation.routingNodes()
                    .startShard(iterator.initialize("node-2", null, 0L, routingAllocation.changes()), routingAllocation.changes(), 0L);
                break;
            }
        }

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            DesiredBalanceInput.create(randomNonNegativeLong(), routingAllocation),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-2", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );

    }

    public void testSimulatesAchievingDesiredBalanceBeforeDelegating() {

        var allocateCalled = new AtomicBoolean();
        var desiredBalanceComputer = createDesiredBalanceComputer(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                assertTrue(allocateCalled.compareAndSet(false, true));
                // whatever the allocation in the current cluster state, the desired balance service should start by moving all the
                // known shards to their desired locations before delegating to the inner allocator
                for (var routingNode : allocation.routingNodes()) {
                    assertThat(
                        allocation.routingNodes().toString(),
                        routingNode.numberOfOwningShards(),
                        equalTo(routingNode.nodeId().equals("node-2") ? 0 : 2)
                    );
                    for (var shardRouting : routingNode) {
                        assertTrue(shardRouting.toString(), shardRouting.started());
                    }
                }
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        // first, manually assign the shards to their expected locations to pre-populate the desired balance
        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var desiredRoutingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = desiredRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            desiredRoutingNodes.startShard(
                iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes),
                changes,
                0L
            );
        }
        clusterState = rebuildRoutingTable(clusterState, desiredRoutingNodes);

        var desiredBalance1 = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState),
            queue(),
            input -> true
        );
        assertDesiredAssignments(
            desiredBalance1,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );

        // now create a cluster state with the routing table in a random state
        var randomRoutingNodes = clusterState.mutableRoutingNodes();
        for (int shard = 0; shard < 2; shard++) {
            var primaryRoutingState = randomFrom(ShardRoutingState.values());
            var replicaRoutingState = switch (primaryRoutingState) {
                case UNASSIGNED, INITIALIZING -> UNASSIGNED;
                case STARTED -> randomFrom(ShardRoutingState.values());
                case RELOCATING -> randomValueOtherThan(RELOCATING, () -> randomFrom(ShardRoutingState.values()));
            };
            var nodes = new ArrayList<>(List.of("node-0", "node-1", "node-2"));
            Randomness.shuffle(nodes);

            if (primaryRoutingState == UNASSIGNED) {
                continue;
            }
            for (var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary()) {
                    switch (primaryRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(iterator.initialize(nodes.remove(0), null, 0L, changes), changes, 0L);
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(iterator.initialize(nodes.remove(0), null, 0L, changes), changes, 0L),
                            nodes.remove(0),
                            0L,
                            "test",
                            changes
                        );
                    }
                    break;
                }
            }

            if (replicaRoutingState == UNASSIGNED) {
                continue;
            }
            for (var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary() == false) {
                    switch (replicaRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(iterator.initialize(nodes.remove(0), null, 0L, changes), changes, 0L);
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(iterator.initialize(nodes.remove(0), null, 0L, changes), changes, 0L),
                            nodes.remove(0),
                            0L,
                            "test",
                            changes
                        );
                    }
                    break;
                }
            }
        }
        clusterState = rebuildRoutingTable(clusterState, randomRoutingNodes);

        allocateCalled.set(false);

        var desiredBalance2 = desiredBalanceComputer.compute(desiredBalance1, createInput(clusterState), queue(), input -> true);
        assertDesiredAssignments(
            desiredBalance2,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
        assertTrue(allocateCalled.get());
    }

    public void testNoDataNodes() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(0);

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState),
            queue(),
            input -> true
        );

        assertDesiredAssignments(desiredBalance, Map.of());
    }

    public void testAppliesMoveCommands() {
        var desiredBalanceComputer = createDesiredBalanceComputer(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                // This runs after the move commands have been applied, we assert that the relocating shards caused by the move
                // commands are all started by the simulation.
                assertThat(
                    "unexpected relocating shards: " + allocation.routingNodes(),
                    allocation.routingNodes().getRelocatingShardCount(),
                    equalTo(0)
                );
                assertThat(Iterators.toList(allocation.routingNodes().node("node-2").started().iterator()), hasSize(2));
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            routingNodes.startShard(iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes), changes, 0L);
        }
        clusterState = rebuildRoutingTable(clusterState, routingNodes);

        final var dataNodeIds = clusterState.nodes().getDataNodes().keySet();
        for (var nodeId : List.of("node-0", "node-1")) {
            final var desiredBalanceInput = DesiredBalanceInput.create(
                randomInt(),
                new RoutingAllocation(new AllocationDeciders(List.of(new AllocationDecider() {
                    @Override
                    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                        // Move command works every decision except NO
                        return randomFrom(Decision.YES, Decision.THROTTLE, Decision.NOT_PREFERRED);
                    }
                })), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, 0L)
            );
            var desiredBalance = desiredBalanceComputer.compute(
                DesiredBalance.BECOME_MASTER_INITIAL,
                desiredBalanceInput,
                queue(
                    new MoveAllocationCommand(index.getName(), 0, nodeId, "node-2"),
                    new MoveAllocationCommand(index.getName(), 1, nodeId, "node-2")
                ),
                input -> true
            );

            final Set<String> expectedNodeIds = Sets.difference(dataNodeIds, Set.of(nodeId));
            assertDesiredAssignments(
                desiredBalance,
                Map.of(
                    new ShardId(index, 0),
                    new ShardAssignment(expectedNodeIds, 2, 0, 0),
                    new ShardId(index, 1),
                    new ShardAssignment(expectedNodeIds, 2, 0, 0)
                )
            );
        }
    }

    public void testCannotApplyMoveCommand() {
        var desiredBalanceComputer = createDesiredBalanceComputer(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                // This runs after the move commands have been executed and failed, we assert that no movement should be seen
                // in the routing nodes.
                assertThat(
                    "unexpected relocating shards: " + allocation.routingNodes(),
                    allocation.routingNodes().getRelocatingShardCount(),
                    equalTo(0)
                );
                assertThat(allocation.routingNodes().node("node-2").isEmpty(), equalTo(true));
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().getProject().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            routingNodes.startShard(iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes), changes, 0L);
        }
        clusterState = rebuildRoutingTable(clusterState, routingNodes);

        final var desiredBalanceInput = DesiredBalanceInput.create(
            randomInt(),
            new RoutingAllocation(new AllocationDeciders(List.of(new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    // Always return NO so that AllocationCommands will silently fail.
                    return Decision.NO;
                }
            })), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, 0L)
        );
        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            desiredBalanceInput,
            queue(
                new MoveAllocationCommand(index.getName(), 0, randomFrom("node-0", "node-1"), "node-2"),
                new MoveAllocationCommand(index.getName(), 1, randomFrom("node-0", "node-1"), "node-2")
            ),
            input -> true
        );

        final Set<String> expectedNodeIds = Set.of("node-0", "node-1");
        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(expectedNodeIds, 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(expectedNodeIds, 2, 0, 0)
            )
        );
    }

    public void testDesiredBalanceShouldConvergeInABigCluster() {
        var nodes = randomIntBetween(3, 7);
        var nodeIds = new ArrayList<String>(nodes);
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        var usedDiskSpace = Maps.<String, Long>newMapWithExpectedSize(nodes);
        for (int node = 0; node < nodes; node++) {
            var nodeId = "node-" + node;
            nodeIds.add(nodeId);
            discoveryNodesBuilder.add(newNode(nodeId));
            usedDiskSpace.put(nodeId, 0L);
        }

        var indices = scaledRandomIntBetween(1, 500);
        var totalShards = 0;
        var totalShardsSize = 0L;

        var shardSizes = new HashMap<String, Long>();
        var dataPath = new HashMap<NodeAndShard, String>();

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < indices; i++) {
            var indexName = "index-" + i;
            var shards = randomIntBetween(1, 10);
            var replicas = scaledRandomIntBetween(1, nodes - 1);
            totalShards += shards * (replicas + 1);
            var inSyncIds = randomList(shards * (replicas + 1), shards * (replicas + 1), () -> UUIDs.randomBase64UUID(random()));
            var shardSize = randomLongBetween(10_000_000L, 10_000_000_000L);

            var indexMetadataBuilder = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), shards, replicas));
            if (randomBoolean()) {
                indexMetadataBuilder.shardSizeInBytesForecast(smallShardSizeDeviation(shardSize));
            }

            for (int shard = 0; shard < shards; shard++) {
                indexMetadataBuilder.putInSyncAllocationIds(
                    shard,
                    Set.copyOf(inSyncIds.subList(shard * (replicas + 1), (shard + 1) * (replicas + 1)))
                );
            }
            metadataBuilder.put(indexMetadataBuilder);

            var indexId = metadataBuilder.get(indexName).getIndex();
            var indexRoutingTableBuilder = IndexRoutingTable.builder(indexId);

            for (int shard = 0; shard < shards; shard++) {
                var remainingNodeIds = new ArrayList<>(nodeIds);
                var shardId = new ShardId(indexId, shard);
                var thisShardSize = smallShardSizeDeviation(shardSize);

                var primaryNodeId = pickAndRemoveRandomValueFrom(remainingNodeIds);
                shardSizes.put(shardIdentifierFromRouting(shardId, true), thisShardSize);
                totalShardsSize += thisShardSize;
                dataPath.put(new NodeAndShard(primaryNodeId, shardId), "/data");
                usedDiskSpace.compute(primaryNodeId, (k, v) -> v + thisShardSize);
                var primaryState = randomIntBetween(0, 9) == 0 ? INITIALIZING : STARTED;
                indexRoutingTableBuilder.addShard(
                    shardRoutingBuilder(shardId, primaryNodeId, true, primaryState).withAllocationId(
                        AllocationId.newInitializing(inSyncIds.get(shard * (replicas + 1)))
                    ).build()
                );

                remainingNodeIds.add(null);// to simulate unassigned shard
                for (int replica = 0; replica < replicas; replica++) {
                    var replicaNodeId = pickAndRemoveRandomValueFrom(remainingNodeIds);
                    shardSizes.put(shardIdentifierFromRouting(shardId, false), thisShardSize);
                    totalShardsSize += thisShardSize;
                    if (replicaNodeId != null) {
                        dataPath.put(new NodeAndShard(replicaNodeId, shardId), "/data");
                        usedDiskSpace.compute(replicaNodeId, (k, v) -> v + thisShardSize);
                    }
                    var replicaState = randomIntBetween(0, 9) == 0 ? INITIALIZING : STARTED;
                    if (primaryState == INITIALIZING || replicaNodeId == null) {
                        replicaState = UNASSIGNED;
                        replicaNodeId = null;
                    }
                    indexRoutingTableBuilder.addShard(
                        shardRoutingBuilder(shardId, replicaNodeId, false, replicaState).withAllocationId(
                            AllocationId.newInitializing(inSyncIds.get(shard * (replicas + 1) + 1 + replica))
                        ).build()
                    );
                }

            }
            routingTableBuilder.add(indexRoutingTableBuilder);
        }

        logger.info("Simulating cluster with [{}] nodes and [{}] shards", nodes, totalShards);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var iteration = new AtomicInteger(0);

        long diskSize = Math.max(totalShardsSize / nodes, usedDiskSpace.values().stream().max(Long::compare).get()) * 120 / 100;
        assertTrue("Should have enough space for all shards", diskSize * nodes > totalShardsSize);

        var diskUsage = usedDiskSpace.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, it -> new DiskUsage(it.getKey(), it.getKey(), "/data", diskSize, diskSize - it.getValue())));

        var clusterInfo = ClusterInfo.builder()
            .leastAvailableSpaceUsage(diskUsage)
            .mostAvailableSpaceUsage(diskUsage)
            .shardSizes(shardSizes)
            .dataPath(dataPath)
            .build();

        var settings = Settings.EMPTY;

        var input = new DesiredBalanceInput(randomInt(), routingAllocationWithDecidersOf(clusterState, clusterInfo, settings), List.of());
        var desiredBalance = createDesiredBalanceComputer(new BalancedShardsAllocator(settings)).compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            input,
            queue(),
            ignored -> iteration.incrementAndGet() < 2000
        );

        var desiredDiskUsage = Maps.<String, Long>newMapWithExpectedSize(nodes);
        for (var assignment : desiredBalance.assignments().entrySet()) {
            var shardSize = Math.min(
                clusterInfo.getShardSize(assignment.getKey(), true),
                clusterInfo.getShardSize(assignment.getKey(), false)
            );
            for (String nodeId : assignment.getValue().nodeIds()) {
                desiredDiskUsage.compute(nodeId, (key, value) -> (value != null ? value : 0) + shardSize);
            }
        }

        assertThat(
            "Balance should converge, but exited by the iteration limit",
            desiredBalance.lastConvergedIndex(),
            equalTo(input.index())
        );
        logger.info("Balance converged after [{}] iterations", iteration.get());

        assertThat(
            "All desired disk usages " + desiredDiskUsage + " should be smaller then actual disk sizes: " + diskSize,
            desiredDiskUsage.values(),
            everyItem(lessThanOrEqualTo(diskSize))
        );
    }

    private static long smallShardSizeDeviation(long originalSize) {
        var deviation = randomIntBetween(-5, 5);
        return originalSize * (100 + deviation) / 100;
    }

    private String pickAndRemoveRandomValueFrom(List<String> values) {
        var value = randomFrom(values);
        values.remove(value);
        return value;
    }

    public void testComputeConsideringShardSizes() {

        var discoveryNodesBuilder = DiscoveryNodes.builder().add(newNode("node-0")).add(newNode("node-1")).add(newNode("node-2"));

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        ShardRouting index0PrimaryShard;
        ShardRouting index0ReplicaShard;
        {
            var indexName = "index-0";

            metadataBuilder.put(
                IndexMetadata.builder(indexName)
                    .settings(indexSettings(IndexVersion.current(), 1, 1).put("index.routing.allocation.exclude._id", "node-2"))
            );

            var indexId = metadataBuilder.get(indexName).getIndex();
            var shardId = new ShardId(indexId, 0);

            index0PrimaryShard = newShardRouting(shardId, "node-1", null, true, STARTED);
            index0ReplicaShard = switch (randomIntBetween(0, 6)) {
                // shard is started on the desired node
                case 0 -> newShardRouting(shardId, "node-0", null, false, STARTED);
                // shard is initializing on the desired node
                case 1 -> newShardRouting(shardId, "node-0", null, false, INITIALIZING);
                // shard is initializing on the undesired node
                case 2 -> newShardRouting(shardId, "node-2", null, false, INITIALIZING);
                // shard started on undesired node, assumed to be relocated to the desired node in the future
                case 3 -> newShardRouting(shardId, "node-2", null, false, STARTED);
                // shard is already relocating to the desired node
                case 4 -> newShardRouting(shardId, "node-2", "node-0", false, RELOCATING);
                // shard is relocating to the undesired node
                case 5 -> newShardRouting(shardId, "node-0", "node-2", false, RELOCATING);
                // shard is unassigned
                case 6 -> newShardRouting(shardId, null, null, false, UNASSIGNED);
                default -> throw new IllegalStateException();
            };

            routingTableBuilder.add(IndexRoutingTable.builder(indexId).addShard(index0PrimaryShard).addShard(index0ReplicaShard));
        }

        for (int i = 1; i < 10; i++) {
            var indexName = "index-" + i;

            metadataBuilder.put(
                IndexMetadata.builder(indexName)
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.routing.allocation.exclude._id", "node-2"))
            );

            var indexId = metadataBuilder.get(indexName).getIndex();
            var shardId = new ShardId(indexId, 0);

            routingTableBuilder.add(
                IndexRoutingTable.builder(indexId).addShard(newShardRouting(shardId, i == 1 ? "node-0" : "node-1", null, true, STARTED))
            );
        }

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var node0RemainingBytes = (index0ReplicaShard.started() || index0ReplicaShard.relocating())
            && Objects.equals(index0ReplicaShard.currentNodeId(), "node-0") ? 100 : 600;

        var clusterInfo = new ClusterInfoTestBuilder().withNode("node-0", 1000, node0RemainingBytes)
            .withNode("node-1", 1000, 100)
            .withNode("node-2", 1000, 1000)
            // node-0 & node-1
            .withShard(findShard(clusterState, "index-0", true), 500)
            .withShard(findShard(clusterState, "index-0", false), 500)
            // node-0
            .withShard(findShard(clusterState, "index-1", true), 400)
            // node-1
            .withShard(findShard(clusterState, "index-2", true), 50)
            .withShard(findShard(clusterState, "index-3", true), 50)
            .withShard(findShard(clusterState, "index-4", true), 50)
            .withShard(findShard(clusterState, "index-5", true), 50)
            .withShard(findShard(clusterState, "index-6", true), 50)
            .withShard(findShard(clusterState, "index-7", true), 50)
            .withShard(findShard(clusterState, "index-8", true), 50)
            .withShard(findShard(clusterState, "index-9", true), 50)
            .build();

        var settings = Settings.builder()
            // force as many iterations as possible to accumulate the diff
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), "1")
            // have a small gap to keep allocating the shards
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "97%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "98%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "99%")
            .build();

        var initial = new DesiredBalance(
            1,
            Map.ofEntries(
                Map.entry(findShardId(clusterState, "index-0"), new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)),
                Map.entry(findShardId(clusterState, "index-1"), new ShardAssignment(Set.of("node-0"), 1, 0, 0))
            )
        );

        var desiredBalance = createDesiredBalanceComputer(new BalancedShardsAllocator(settings)).compute(
            initial,
            new DesiredBalanceInput(randomInt(), routingAllocationWithDecidersOf(clusterState, clusterInfo, settings), List.of()),
            queue(),
            input -> true
        );

        var resultDiskUsage = new HashMap<String, Long>();
        for (var assignment : desiredBalance.assignments().entrySet()) {
            for (String nodeId : assignment.getValue().nodeIds()) {
                var size = Objects.requireNonNull(clusterInfo.getShardSize(assignment.getKey(), true));
                resultDiskUsage.compute(nodeId, (k, v) -> v == null ? size : v + size);
            }
        }

        assertThat(resultDiskUsage, allOf(aMapWithSize(2), hasEntry("node-0", 950L), hasEntry("node-1", 850L)));
    }

    public void testAccountForSizeOfMisplacedShardsDuringNewComputation() {

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot", randomUUID()));

        var clusterInfoBuilder = new ClusterInfoTestBuilder().withNode(
            "node-1",
            ByteSizeValue.ofGb(10).getBytes(),
            ByteSizeValue.ofGb(2).getBytes()
        ).withNode("node-2", ByteSizeValue.ofGb(10).getBytes(), ByteSizeValue.ofGb(2).getBytes());
        var snapshotShardSizes = Maps.<InternalSnapshotsInfoService.SnapshotShard, Long>newHashMapWithExpectedSize(5);

        var routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        // index-1 is allocated according to the desired balance
        var indexMetadata1 = IndexMetadata.builder("index-1").settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        routingTableBuilder.add(
            IndexRoutingTable.builder(indexMetadata1.getIndex())
                .addShard(newShardRouting(shardIdFrom(indexMetadata1, 0), "node-1", true, STARTED))
                .addShard(newShardRouting(shardIdFrom(indexMetadata1, 1), "node-2", true, STARTED))
        );
        clusterInfoBuilder.withShard(shardIdFrom(indexMetadata1, 0), true, ByteSizeValue.ofGb(8).getBytes())
            .withShard(shardIdFrom(indexMetadata1, 1), true, ByteSizeValue.ofGb(8).getBytes());

        // index-2 is restored earlier but is not started on the desired node yet
        var indexMetadata2 = IndexMetadata.builder("index-2").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        snapshotShardSizes.put(
            new SnapshotShard(snapshot, indexIdFrom(indexMetadata2), shardIdFrom(indexMetadata2, 0)),
            ByteSizeValue.ofGb(1).getBytes()
        );
        var index2SnapshotRecoverySource = new RecoverySource.SnapshotRecoverySource(
            "restore",
            snapshot,
            IndexVersion.current(),
            indexIdFrom(indexMetadata2)
        );
        switch (randomInt(3)) {
            // index is still unassigned
            case 0 -> routingTableBuilder.addAsNewRestore(indexMetadata2, index2SnapshotRecoverySource, Set.of());
            // index is initializing on desired node
            case 1 -> {
                ShardId index2ShardId = shardIdFrom(indexMetadata2, 0);
                routingTableBuilder.add(
                    IndexRoutingTable.builder(indexMetadata2.getIndex())
                        .addShard(
                            shardRoutingBuilder(index2ShardId, "node-1", true, INITIALIZING).withRecoverySource(
                                index2SnapshotRecoverySource
                            ).build()
                        )
                );
                if (randomBoolean()) {
                    // Shard is 75% downloaded
                    clusterInfoBuilder //
                        .withNodeUsedSpace("node-1", ByteSizeValue.ofMb(768).getBytes())
                        .withReservedSpace("node-1", ByteSizeValue.ofMb(256).getBytes(), index2ShardId);
                }
            }
            // index is initializing on undesired node
            case 2 -> {
                ShardId index2ShardId = shardIdFrom(indexMetadata2, 0);
                routingTableBuilder.add(
                    IndexRoutingTable.builder(indexMetadata2.getIndex())
                        .addShard(
                            shardRoutingBuilder(index2ShardId, "node-2", true, INITIALIZING).withRecoverySource(
                                index2SnapshotRecoverySource
                            ).build()
                        )
                );
                if (randomBoolean()) {
                    // Shard is 75% downloaded
                    clusterInfoBuilder //
                        .withNodeUsedSpace("node-2", ByteSizeValue.ofMb(768).getBytes())
                        .withReservedSpace("node-2", ByteSizeValue.ofMb(256).getBytes(), index2ShardId);
                }
            }
            // index is started on undesired node
            case 3 -> {
                routingTableBuilder.add(
                    IndexRoutingTable.builder(indexMetadata2.getIndex())
                        .addShard(newShardRouting(shardIdFrom(indexMetadata2, 0), "node-2", true, STARTED))
                );
                clusterInfoBuilder.withNodeUsedSpace("node-2", ByteSizeValue.ofGb(1).getBytes())
                    .withShard(shardIdFrom(indexMetadata2, 0), true, ByteSizeValue.ofGb(1).getBytes());
            }
            default -> throw new AssertionError("unexpected randomization");
        }

        // index-3 is restored as new from snapshot
        var indexMetadata3 = IndexMetadata.builder("index-3").settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        routingTableBuilder.addAsNewRestore(
            indexMetadata3,
            new RecoverySource.SnapshotRecoverySource("restore", snapshot, IndexVersion.current(), indexIdFrom(indexMetadata3)),
            Set.of()
        );
        snapshotShardSizes.put(
            new SnapshotShard(snapshot, indexIdFrom(indexMetadata3), shardIdFrom(indexMetadata3, 0)),
            ByteSizeValue.ofMb(512).getBytes()
        );
        snapshotShardSizes.put(
            new SnapshotShard(snapshot, indexIdFrom(indexMetadata3), shardIdFrom(indexMetadata3, 1)),
            ByteSizeValue.ofMb(512).getBytes()
        );

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false).put(indexMetadata3, false).build())
            .routingTable(routingTableBuilder)
            .customs(
                Map.of(
                    RestoreInProgress.TYPE,
                    new RestoreInProgress.Builder().add(
                        new RestoreInProgress.Entry(
                            "restore",
                            snapshot,
                            RestoreInProgress.State.STARTED,
                            randomBoolean(),
                            List.of(indexMetadata2.getIndex().getName(), indexMetadata3.getIndex().getName()),
                            Map.ofEntries(
                                Map.entry(shardIdFrom(indexMetadata2, 0), new RestoreInProgress.ShardRestoreStatus(randomUUID())),
                                Map.entry(shardIdFrom(indexMetadata3, 0), new RestoreInProgress.ShardRestoreStatus(randomUUID())),
                                Map.entry(shardIdFrom(indexMetadata3, 1), new RestoreInProgress.ShardRestoreStatus(randomUUID()))
                            )
                        )
                    ).build()
                )
            )
            .build();

        var settings = Settings.EMPTY;
        var allocation = new RoutingAllocation(
            randomAllocationDeciders(settings, createBuiltInClusterSettings(settings)),
            clusterState,
            clusterInfoBuilder.build(),
            new SnapshotShardSizeInfo(snapshotShardSizes),
            0L
        );
        var initialDesiredBalance = new DesiredBalance(
            1,
            Map.ofEntries(
                Map.entry(shardIdFrom(indexMetadata1, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                Map.entry(shardIdFrom(indexMetadata1, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                Map.entry(shardIdFrom(indexMetadata2, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0))
            )
        );
        var nextDesiredBalance = createDesiredBalanceComputer(new BalancedShardsAllocator()).compute(
            initialDesiredBalance,
            new DesiredBalanceInput(2, allocation, List.of()),
            queue(),
            input -> true
        );

        // both node-1 and node-2 has enough space to allocate either only [index-2] shard or both [index-3] shards
        assertThat(
            nextDesiredBalance.assignments(),
            anyOf(
                equalTo(
                    Map.ofEntries(
                        Map.entry(shardIdFrom(indexMetadata1, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata1, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata2, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 0), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0))
                    )
                ),
                equalTo(
                    Map.ofEntries(
                        Map.entry(shardIdFrom(indexMetadata1, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata1, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata2, 0), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 1), new ShardAssignment(Set.of("node-1"), 1, 0, 0))
                    )
                )
            )
        );
    }

    public void testAccountForSizeOfAllInitializingShardsDuringAllocation() {

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot", randomUUID()));

        var clusterInfoBuilder = new ClusterInfoTestBuilder().withNode(
            "node-1",
            ByteSizeValue.ofGb(10).getBytes(),
            ByteSizeValue.ofGb(2).getBytes()
        ).withNode("node-2", ByteSizeValue.ofGb(10).getBytes(), ByteSizeValue.ofGb(2).getBytes());
        var snapshotShardSizes = Maps.<SnapshotShard, Long>newHashMapWithExpectedSize(5);

        var routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        // index-1 is allocated according to the desired balance
        var indexMetadata1 = IndexMetadata.builder("index-1").settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        routingTableBuilder.add(
            IndexRoutingTable.builder(indexMetadata1.getIndex())
                .addShard(newShardRouting(shardIdFrom(indexMetadata1, 0), "node-1", true, STARTED))
                .addShard(newShardRouting(shardIdFrom(indexMetadata1, 1), "node-2", true, STARTED))
        );
        clusterInfoBuilder.withShard(shardIdFrom(indexMetadata1, 0), true, ByteSizeValue.ofGb(8).getBytes())
            .withShard(shardIdFrom(indexMetadata1, 1), true, ByteSizeValue.ofGb(8).getBytes());

        // index-2 & index-3 are restored as new from snapshot
        var indexMetadata2 = IndexMetadata.builder("index-2")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), 2))
            .build();
        routingTableBuilder.addAsNewRestore(
            indexMetadata2,
            new RecoverySource.SnapshotRecoverySource("restore", snapshot, IndexVersion.current(), indexIdFrom(indexMetadata2)),
            Set.of()
        );
        snapshotShardSizes.put(
            new SnapshotShard(snapshot, indexIdFrom(indexMetadata2), shardIdFrom(indexMetadata2, 0)),
            ByteSizeValue.ofGb(1).getBytes()
        );

        var indexMetadata3 = IndexMetadata.builder("index-3")
            .settings(indexSettings(IndexVersion.current(), 2, 0).put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), 1))
            .build();
        routingTableBuilder.addAsNewRestore(
            indexMetadata3,
            new RecoverySource.SnapshotRecoverySource("restore", snapshot, IndexVersion.current(), indexIdFrom(indexMetadata3)),
            Set.of()
        );
        snapshotShardSizes.put(
            new SnapshotShard(snapshot, indexIdFrom(indexMetadata3), shardIdFrom(indexMetadata3, 0)),
            ByteSizeValue.ofMb(512).getBytes()
        );
        snapshotShardSizes.put(
            new SnapshotShard(snapshot, indexIdFrom(indexMetadata3), shardIdFrom(indexMetadata3, 1)),
            ByteSizeValue.ofMb(512).getBytes()
        );

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false).put(indexMetadata3, false).build())
            .routingTable(routingTableBuilder)
            .customs(
                Map.of(
                    RestoreInProgress.TYPE,
                    new RestoreInProgress.Builder().add(
                        new RestoreInProgress.Entry(
                            "restore",
                            snapshot,
                            RestoreInProgress.State.STARTED,
                            randomBoolean(),
                            List.of(indexMetadata2.getIndex().getName(), indexMetadata3.getIndex().getName()),
                            Map.ofEntries(
                                Map.entry(shardIdFrom(indexMetadata2, 0), new RestoreInProgress.ShardRestoreStatus(randomUUID())),
                                Map.entry(shardIdFrom(indexMetadata3, 0), new RestoreInProgress.ShardRestoreStatus(randomUUID())),
                                Map.entry(shardIdFrom(indexMetadata3, 1), new RestoreInProgress.ShardRestoreStatus(randomUUID()))
                            )
                        )
                    ).build()
                )
            )
            .build();

        var settings = Settings.EMPTY;
        var allocation = new RoutingAllocation(
            randomAllocationDeciders(settings, createBuiltInClusterSettings(settings)),
            clusterState,
            clusterInfoBuilder.build(),
            new SnapshotShardSizeInfo(snapshotShardSizes),
            0L
        );
        var initialDesiredBalance = new DesiredBalance(
            1,
            Map.ofEntries(
                Map.entry(shardIdFrom(indexMetadata1, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                Map.entry(shardIdFrom(indexMetadata1, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0))
            )
        );
        var nextDesiredBalance = createDesiredBalanceComputer(new BalancedShardsAllocator()).compute(
            initialDesiredBalance,
            new DesiredBalanceInput(2, allocation, List.of()),
            queue(),
            input -> true
        );

        // both node-1 and node-2 has enough space to allocate either only [index-2] shard or both [index-3] shards
        assertThat(
            nextDesiredBalance.assignments(),
            anyOf(
                equalTo(
                    Map.ofEntries(
                        Map.entry(shardIdFrom(indexMetadata1, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata1, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata2, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 0), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0))
                    )
                ),
                equalTo(
                    Map.ofEntries(
                        Map.entry(shardIdFrom(indexMetadata1, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata1, 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata2, 0), new ShardAssignment(Set.of("node-2"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                        Map.entry(shardIdFrom(indexMetadata3, 1), new ShardAssignment(Set.of("node-1"), 1, 0, 0))
                    )
                )
            )
        );
    }

    private static ClusterState rebuildRoutingTable(ClusterState clusterState, RoutingNodes routingNodes) {
        GlobalRoutingTable routingTable = clusterState.globalRoutingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable.rebuild(routingNodes, clusterState.metadata())).build();
        return clusterState;
    }

    private static class ClusterInfoTestBuilder {

        private final Map<String, DiskUsage> diskUsage = new HashMap<>();
        private final Map<String, Long> shardSizes = new HashMap<>();
        private final Map<NodeAndPath, ReservedSpace> reservedSpace = new HashMap<>();
        private final Map<NodeAndShard, String> dataPath = new HashMap<>();

        public ClusterInfoTestBuilder withNode(String nodeId, long totalBytes, long freeBytes) {
            diskUsage.put(nodeId, new DiskUsage(nodeId, nodeId, "/path", totalBytes, freeBytes));
            return this;
        }

        public ClusterInfoTestBuilder withNodeUsedSpace(String nodeId, long usedBytes) {
            diskUsage.compute(nodeId, (key, usage) -> {
                assertThat(usage, notNullValue());
                return new DiskUsage(usage.nodeId(), usage.nodeName(), usage.path(), usage.totalBytes(), usage.freeBytes() - usedBytes);
            });
            return this;
        }

        public ClusterInfoTestBuilder withShard(ShardId shardId, boolean primary, long size) {
            shardSizes.put(shardIdentifierFromRouting(shardId, primary), size);
            return this;
        }

        public ClusterInfoTestBuilder withShard(ShardRouting shard, long size) {
            shardSizes.put(shardIdentifierFromRouting(shard), size);
            if (shard.unassigned() == false) {
                dataPath.put(NodeAndShard.from(shard), "/data/path");
            }
            return this;
        }

        public ClusterInfoTestBuilder withReservedSpace(String nodeId, long size, ShardId... shardIds) {
            reservedSpace.put(new NodeAndPath(nodeId, "/path"), new ReservedSpace(size, Set.of(shardIds)));
            return this;
        }

        public ClusterInfo build() {
            return ClusterInfo.builder()
                .leastAvailableSpaceUsage(diskUsage)
                .mostAvailableSpaceUsage(diskUsage)
                .shardSizes(shardSizes)
                .reservedSpace(reservedSpace)
                .dataPath(dataPath)
                .build();
        }
    }

    private static IndexId indexIdFrom(IndexMetadata indexMetadata) {
        return new IndexId(indexMetadata.getIndex().getName(), indexMetadata.getIndex().getUUID());
    }

    private static ShardId shardIdFrom(IndexMetadata indexMetadata, int shardId) {
        return new ShardId(indexMetadata.getIndex(), shardId);
    }

    public void testShouldLogComputationIteration() {
        checkIterationLogging(
            999,
            10L,
            new MockLog.UnseenEventExpectation(
                "Should not report long computation too early",
                DesiredBalanceComputer.class.getCanonicalName(),
                Level.INFO,
                "Desired balance computation for [*] is still not converged after [*] and [*] iterations"
            )
        );

        checkIterationLogging(
            1001,
            10L,
            new MockLog.SeenEventExpectation(
                "Should report long computation based on iteration count",
                DesiredBalanceComputer.class.getCanonicalName(),
                Level.INFO,
                "Desired balance computation for [*] is still not converged after [10s] and [1000] iterations"
            )
        );

        checkIterationLogging(
            61,
            1000L,
            new MockLog.SeenEventExpectation(
                "Should report long computation based on time",
                DesiredBalanceComputer.class.getCanonicalName(),
                Level.INFO,
                "Desired balance computation for [*] is still not converged after [1m] and [59] iterations"
            )
        );
    }

    private void checkIterationLogging(int iterations, long eachIterationDuration, MockLog.AbstractEventExpectation expectation) {
        var currentTime = new AtomicLong(0L);
        TimeProvider timeProvider = TimeProviderUtils.create(() -> currentTime.addAndGet(eachIterationDuration));

        // Some runs of this test try to simulate a long desired balance computation. Setting a high value on the following setting
        // prevents interrupting a long computation.
        var clusterSettings = createBuiltInClusterSettings(
            Settings.builder().put(DesiredBalanceComputer.MAX_BALANCE_COMPUTATION_TIME_DURING_INDEX_CREATION_SETTING.getKey(), "2m").build()
        );
        var desiredBalanceComputer = new DesiredBalanceComputer(clusterSettings, timeProvider, new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else {
                        unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                    }
                }

                // move shard on each iteration
                for (var shard : allocation.routingNodes().node("node-0").shardsWithState(STARTED).toList()) {
                    allocation.routingNodes().relocateShard(shard, "node-1", 0L, "test", allocation.changes());
                }
                for (var shard : allocation.routingNodes().node("node-1").shardsWithState(STARTED).toList()) {
                    allocation.routingNodes().relocateShard(shard, "node-0", 0L, "test", allocation.changes());
                }
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        }, TEST_ONLY_EXPLAINER);

        assertLoggerExpectationsFor(() -> {
            var iteration = new AtomicInteger(0);
            desiredBalanceComputer.compute(
                DesiredBalance.BECOME_MASTER_INITIAL,
                createInput(createInitialClusterState(3)),
                queue(),
                input -> iteration.incrementAndGet() < iterations
            );
        }, expectation);
    }

    private void assertLoggerExpectationsFor(Runnable action, MockLog.LoggingExpectation... expectations) {
        assertThatLogger(action, DesiredBalanceComputer.class, expectations);
    }

    public void testLoggingOfComputeCallsAndIterationsSinceConvergence() {
        final var clusterSettings = new ClusterSettings(
            Settings.builder().put(DesiredBalanceComputer.PROGRESS_LOG_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(5L)).build(),
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
        final var timeInMillis = new AtomicLong(-1L);
        final var iterationCounter = new AtomicInteger(0);
        final var requiredIterations = new AtomicInteger(2);
        final var desiredBalance = new AtomicReference<DesiredBalance>(DesiredBalance.BECOME_MASTER_INITIAL);
        final var indexSequence = new AtomicLong(0);
        final var clusterState = createInitialClusterState(1, 1, 0);

        final var computer = new DesiredBalanceComputer(
            clusterSettings,
            TimeProviderUtils.create(timeInMillis::incrementAndGet),
            new BalancedShardsAllocator(Settings.EMPTY),
            TEST_ONLY_EXPLAINER
        ) {
            @Override
            boolean hasEnoughIterations(int currentIteration) {
                iterationCounter.incrementAndGet();
                return currentIteration >= requiredIterations.get();
            }
        };
        computer.setConvergenceLogMsgLevel(Level.INFO);

        record ExpectedLastConvergenceInfo(int numComputeCalls, int numTotalIterations, long timestampMillis) {}

        Consumer<ExpectedLastConvergenceInfo> assertLastConvergenceInfo = data -> {
            assertEquals(data.numComputeCalls(), computer.getNumComputeCallsSinceLastConverged());
            assertEquals(data.numTotalIterations(), computer.getNumIterationsSinceLastConverged());
            assertEquals(data.timestampMillis(), computer.getLastConvergedTimeMillis());
        };

        final Function<Predicate<DesiredBalanceInput>, Runnable> getComputeRunnableForIsFreshPredicate = isFreshFunc -> {
            final var input = new DesiredBalanceInput(indexSequence.incrementAndGet(), routingAllocationOf(clusterState), List.of());
            return () -> desiredBalance.set(computer.compute(desiredBalance.get(), input, queue(), isFreshFunc));
        };

        record LogExpectationData(
            boolean isConverged,
            String timeSinceConverged,
            int totalIterations,
            int totalComputeCalls,
            int currentIterations,
            String currentDuration
        ) {
            LogExpectationData(boolean isConverged, String timeSinceConverged, int totalIterations) {
                this(isConverged, timeSinceConverged, totalIterations, 0, 0, "");
            }
        }

        Function<LogExpectationData, MockLog.SeenEventExpectation> getLogExpectation = data -> {
            final var singleComputeCallMsg = "Desired balance computation for [%d] "
                + (data.isConverged ? "" : "is still not ")
                + "converged after [%s] and [%d] iterations";
            return new MockLog.SeenEventExpectation(
                "expected a " + (data.isConverged ? "converged" : "not converged") + " log message",
                DesiredBalanceComputer.class.getCanonicalName(),
                Level.INFO,
                (data.totalComputeCalls > 1
                    ? Strings.format(
                        singleComputeCallMsg + ", resumed computation [%d] times with [%d] iterations since the last resumption [%s] ago",
                        indexSequence.get(),
                        data.timeSinceConverged,
                        data.totalIterations,
                        data.totalComputeCalls,
                        data.currentIterations,
                        data.currentDuration
                    )
                    : Strings.format(singleComputeCallMsg, indexSequence.get(), data.timeSinceConverged, data.totalIterations))
            );
        };

        final Consumer<DesiredBalance.ComputationFinishReason> assertFinishReason = reason -> {
            assertEquals(reason, desiredBalance.get().finishReason());
            if (DesiredBalance.ComputationFinishReason.CONVERGED == reason) {
                // Verify the number of compute() calls and total iterations have been reset after converging.
                assertLastConvergenceInfo.accept(new ExpectedLastConvergenceInfo(0, 0, timeInMillis.get()));
            }
        };

        // No compute() calls yet, last convergence timestamp is the startup time.
        assertLastConvergenceInfo.accept(new ExpectedLastConvergenceInfo(0, 0, timeInMillis.get()));

        // Converges right away, verify the debug level convergence message.
        assertLoggerExpectationsFor(
            getComputeRunnableForIsFreshPredicate.apply(ignored -> true),
            getLogExpectation.apply(new LogExpectationData(true, "3ms", 2))
        );
        assertFinishReason.accept(DesiredBalance.ComputationFinishReason.CONVERGED);
        final var lastConvergenceTimestampMillis = computer.getLastConvergedTimeMillis();

        // Test a series of compute() calls that don't converge.
        iterationCounter.set(0);
        requiredIterations.set(10);
        // This INFO is triggered from the interval since last convergence timestamp.
        assertLoggerExpectationsFor(
            getComputeRunnableForIsFreshPredicate.apply(ignored -> iterationCounter.get() < 6),
            getLogExpectation.apply(new LogExpectationData(false, "5ms", 4))
        );
        assertFinishReason.accept(DesiredBalance.ComputationFinishReason.YIELD_TO_NEW_INPUT);
        assertLastConvergenceInfo.accept(new ExpectedLastConvergenceInfo(1, 6, lastConvergenceTimestampMillis));

        iterationCounter.set(0);
        // The next INFO is triggered from the interval since last INFO message logged, and then another after the interval period.
        assertLoggerExpectationsFor(
            getComputeRunnableForIsFreshPredicate.apply(ignored -> iterationCounter.get() < 8),
            getLogExpectation.apply(new LogExpectationData(false, "10ms", 8, 2, 2, "2ms")),
            getLogExpectation.apply(new LogExpectationData(false, "15ms", 13, 2, 7, "7ms"))
        );
        assertFinishReason.accept(DesiredBalance.ComputationFinishReason.YIELD_TO_NEW_INPUT);
        assertLastConvergenceInfo.accept(new ExpectedLastConvergenceInfo(2, 14, lastConvergenceTimestampMillis));

        assertLoggerExpectationsFor(
            getComputeRunnableForIsFreshPredicate.apply(ignored -> true),
            getLogExpectation.apply(new LogExpectationData(false, "20ms", 17, 3, 3, "3ms")),
            getLogExpectation.apply(new LogExpectationData(false, "25ms", 22, 3, 8, "8ms")),
            getLogExpectation.apply(new LogExpectationData(true, "27ms", 24, 3, 10, "10ms"))
        );
        assertFinishReason.accept(DesiredBalance.ComputationFinishReason.CONVERGED);

        // First INFO is triggered from interval since last converged, second is triggered from the inverval since the last INFO log.
        assertLoggerExpectationsFor(
            getComputeRunnableForIsFreshPredicate.apply(ignored -> true),
            getLogExpectation.apply(new LogExpectationData(false, "5ms", 4)),
            getLogExpectation.apply(new LogExpectationData(false, "10ms", 9)),
            getLogExpectation.apply(new LogExpectationData(true, "11ms", 10))
        );
        assertFinishReason.accept(DesiredBalance.ComputationFinishReason.CONVERGED);

        // Verify the final assignment mappings after converging.
        final var index = clusterState.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(TEST_INDEX).getIndex();
        final var expectedAssignmentsMap = Map.of(new ShardId(index, 0), new ShardAssignment(Set.of("node-0"), 1, 0, 0));
        assertDesiredAssignments(desiredBalance.get(), expectedAssignmentsMap);
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceComputer.allocation_explain:DEBUG",
        reason = "test logging for allocation explain"
    )
    public void testLogAllocationExplainForUnassigned() {
        final ClusterSettings clusterSettings = createBuiltInClusterSettings();
        final var computer = new DesiredBalanceComputer(
            clusterSettings,
            TimeProviderUtils.create(() -> 0L),
            new BalancedShardsAllocator(Settings.EMPTY),
            createAllocationService()::explainShardAllocation
        );
        final String loggerName = DesiredBalanceComputer.class.getCanonicalName() + ".allocation_explain";

        // No logging since no unassigned shard
        {
            final var allocation = new RoutingAllocation(
                randomAllocationDeciders(Settings.EMPTY, clusterSettings),
                createInitialClusterState(1, 1, 0),
                ClusterInfo.EMPTY,
                SnapshotShardSizeInfo.EMPTY,
                0L
            );
            try (var mockLog = MockLog.capture(loggerName)) {
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "Should NOT log allocation explain since all shards are assigned",
                        loggerName,
                        Level.DEBUG,
                        "*"
                    )
                );
                computer.compute(DesiredBalance.BECOME_MASTER_INITIAL, DesiredBalanceInput.create(1, allocation), queue(), ignore -> true);
                mockLog.assertAllExpectationsMatched();
            }
        }

        var initialState = createInitialClusterState(1, 1, 1);
        // Logging for unassigned primary shard (preferred over unassigned replica)
        {
            final DiscoveryNode dataNode = initialState.nodes().getDataNodes().values().iterator().next();
            final var clusterState = initialState.copyAndUpdateMetadata(
                b -> b.putCustom(
                    NodesShutdownMetadata.TYPE,
                    new NodesShutdownMetadata(
                        Map.of(
                            dataNode.getId(),
                            SingleNodeShutdownMetadata.builder()
                                .setNodeId(dataNode.getId())
                                .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                .setReason("test")
                                .setStartedAtMillis(0L)
                                .build()
                        )
                    )
                )
            );
            final var allocation = new RoutingAllocation(
                randomAllocationDeciders(Settings.EMPTY, clusterSettings),
                clusterState,
                ClusterInfo.EMPTY,
                SnapshotShardSizeInfo.EMPTY,
                0L
            );
            final DesiredBalance newDesiredBalance;
            try (var mockLog = MockLog.capture(loggerName)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "Should log allocation explain for unassigned primary shard",
                        loggerName,
                        Level.DEBUG,
                        "*unassigned shard [[test-index][0], node[null], [P], * due to allocation decision *"
                            + "\"decider\":\"node_shutdown\",\"decision\":\"NO\"*"
                    )
                );
                newDesiredBalance = computer.compute(
                    DesiredBalance.BECOME_MASTER_INITIAL,
                    DesiredBalanceInput.create(1, allocation),
                    queue(),
                    ignore -> true
                );
                mockLog.assertAllExpectationsMatched();
            }

            // No logging since the same tracked primary is still unassigned
            try (var mockLog = MockLog.capture(loggerName)) {
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "Should NOT log allocation explain again for existing tracked unassigned shard",
                        loggerName,
                        Level.DEBUG,
                        "*"
                    )
                );
                computer.compute(newDesiredBalance, DesiredBalanceInput.create(2, allocation), queue(), ignore -> true);
                mockLog.assertAllExpectationsMatched();
            }
        }

        // Logging for unassigned replica shard (since primary is now assigned)
        {
            final var allocation = new RoutingAllocation(
                randomAllocationDeciders(Settings.EMPTY, clusterSettings),
                initialState,
                ClusterInfo.EMPTY,
                SnapshotShardSizeInfo.EMPTY,
                0L
            );
            try (var mockLog = MockLog.capture(loggerName)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "Should log for previously unassigned shard becomes assigned",
                        loggerName,
                        Level.DEBUG,
                        "*assigned previously tracked unassigned shard [[test-index][0], node[null], [P],*"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "Should log allocation explain for unassigned replica shard",
                        loggerName,
                        Level.DEBUG,
                        "*unassigned shard [[test-index][0], node[null], [R], * due to allocation decision *"
                            + "\"decider\":\"same_shard\",\"decision\":\"NO\"*"
                    )
                );
                computer.compute(DesiredBalance.BECOME_MASTER_INITIAL, DesiredBalanceInput.create(1, allocation), queue(), ignore -> true);
                mockLog.assertAllExpectationsMatched();
            }
        }

        // No logging if master is shutting down
        {
            var clusterState = createInitialClusterState(1, 1, 1).copyAndUpdateMetadata(
                b -> b.putCustom(
                    NodesShutdownMetadata.TYPE,
                    new NodesShutdownMetadata(
                        Map.of(
                            "master",
                            SingleNodeShutdownMetadata.builder()
                                .setNodeId("master")
                                .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                .setReason("test")
                                .setStartedAtMillis(0L)
                                .setGracePeriod(TimeValue.THIRTY_SECONDS)
                                .build()
                        )
                    )
                )
            );
            final var allocation = new RoutingAllocation(
                randomAllocationDeciders(Settings.EMPTY, clusterSettings),
                clusterState,
                ClusterInfo.EMPTY,
                SnapshotShardSizeInfo.EMPTY,
                0L
            );
            try (var mockLog = MockLog.capture(loggerName)) {
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "Should NOT log allocation explain since all shards are assigned",
                        loggerName,
                        Level.DEBUG,
                        "*"
                    )
                );
                computer.compute(DesiredBalance.BECOME_MASTER_INITIAL, DesiredBalanceInput.create(1, allocation), queue(), ignore -> true);
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    public void testMaybeSimulateAlreadyStartedShards() {
        final var clusterInfoSimulator = mock(ClusterInfoSimulator.class);
        final var routingChangesObserver = mock(RoutingChangesObserver.class);

        final ClusterState initialState = createInitialClusterState(3, 3, 1);
        final RoutingNodes routingNodes = initialState.mutableRoutingNodes();

        final IndexRoutingTable indexRoutingTable = initialState.routingTable(ProjectId.DEFAULT).index(TEST_INDEX);

        final List<ShardRouting> existingStartedShards = new ArrayList<>();

        // Shard 0 - primary started
        final String shard0PrimaryNodeId = randomFrom(initialState.nodes().getDataNodes().values()).getId();
        existingStartedShards.add(
            routingNodes.startShard(
                routingNodes.initializeShard(
                    indexRoutingTable.shard(0).primaryShard(),
                    shard0PrimaryNodeId,
                    null,
                    randomLongBetween(100, 999),
                    routingChangesObserver
                ),
                routingChangesObserver,
                randomLongBetween(100, 999)
            )
        );
        if (randomBoolean()) {
            existingStartedShards.add(
                routingNodes.startShard(
                    routingNodes.initializeShard(
                        indexRoutingTable.shard(0).replicaShards().getFirst(),
                        randomValueOtherThan(shard0PrimaryNodeId, () -> randomFrom(initialState.nodes().getDataNodes().values()).getId()),
                        null,
                        randomLongBetween(100, 999),
                        routingChangesObserver
                    ),
                    routingChangesObserver,
                    randomLongBetween(100, 999)
                )
            );
        }

        // Shard 1 - initializing primary or replica
        final String shard1PrimaryNodeId = randomFrom(initialState.nodes().getDataNodes().values()).getId();
        ShardRouting initializingShard = routingNodes.initializeShard(
            indexRoutingTable.shard(1).primaryShard(),
            shard1PrimaryNodeId,
            null,
            randomLongBetween(100, 999),
            routingChangesObserver
        );
        if (randomBoolean()) {
            existingStartedShards.add(routingNodes.startShard(initializingShard, routingChangesObserver, randomLongBetween(100, 999)));
            initializingShard = routingNodes.initializeShard(
                indexRoutingTable.shard(1).replicaShards().getFirst(),
                randomValueOtherThan(shard1PrimaryNodeId, () -> randomFrom(initialState.nodes().getDataNodes().values()).getId()),
                null,
                randomLongBetween(100, 999),
                routingChangesObserver
            );
        }

        // Shard 2 - Relocating primary
        final String shard2PrimaryNodeId = randomFrom(initialState.nodes().getDataNodes().values()).getId();
        final Tuple<ShardRouting, ShardRouting> relocationTuple = routingNodes.relocateShard(
            routingNodes.startShard(
                routingNodes.initializeShard(
                    indexRoutingTable.shard(2).primaryShard(),
                    shard2PrimaryNodeId,
                    null,
                    randomLongBetween(100, 999),
                    routingChangesObserver
                ),
                routingChangesObserver,
                randomLongBetween(100, 999)
            ),
            randomValueOtherThan(shard2PrimaryNodeId, () -> randomFrom(initialState.nodes().getDataNodes().values()).getId()),
            randomLongBetween(100, 999),
            "test",
            routingChangesObserver
        );
        existingStartedShards.add(relocationTuple.v1());

        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .dataPath(
                existingStartedShards.stream()
                    .collect(Collectors.toUnmodifiableMap(NodeAndShard::from, ignore -> "/data/" + randomIdentifier()))
            )
            .build();

        // No extra simulation calls since there is no new shard or relocated shard that are not in ClusterInfo
        {
            DesiredBalanceComputer.maybeSimulateAlreadyStartedShards(clusterInfo, routingNodes, clusterInfoSimulator);
            verifyNoInteractions(clusterInfoSimulator);
        }

        // Start the initializing shard and it should be identified and simulated
        final var startedShard = routingNodes.startShard(initializingShard, routingChangesObserver, randomLongBetween(100, 999));
        {
            DesiredBalanceComputer.maybeSimulateAlreadyStartedShards(clusterInfo, routingNodes, clusterInfoSimulator);
            verify(clusterInfoSimulator).simulateAlreadyStartedShard(startedShard, null);
            verifyNoMoreInteractions(clusterInfoSimulator);
        }

        // Also start the relocating shard and both should be identified and simulated
        {
            Mockito.clearInvocations(clusterInfoSimulator);
            final var startedRelocatingShard = routingNodes.startShard(
                relocationTuple.v2(),
                routingChangesObserver,
                randomLongBetween(100, 999)
            );
            DesiredBalanceComputer.maybeSimulateAlreadyStartedShards(clusterInfo, routingNodes, clusterInfoSimulator);
            verify(clusterInfoSimulator).simulateAlreadyStartedShard(startedShard, null);
            verify(clusterInfoSimulator).simulateAlreadyStartedShard(startedRelocatingShard, relocationTuple.v1().currentNodeId());
            verifyNoMoreInteractions(clusterInfoSimulator);
        }
    }

    private static ShardRouting findShard(ClusterState clusterState, String name, boolean primary) {
        final var indexShardRoutingTable = clusterState.getRoutingTable().index(name).shard(0);
        return primary ? indexShardRoutingTable.primaryShard() : indexShardRoutingTable.replicaShards().getFirst();
    }

    private static ShardId findShardId(ClusterState clusterState, String name) {
        return clusterState.getRoutingTable().index(name).shard(0).shardId();
    }

    static ClusterState createInitialClusterState(int dataNodesCount) {
        return createInitialClusterState(dataNodesCount, 2, 1);
    }

    static ClusterState createInitialClusterState(int dataNodesCount, int numShards, int numReplicas) {
        var discoveryNodes = DiscoveryNodes.builder().add(newNode("master", Set.of(DiscoveryNodeRole.MASTER_ROLE)));
        for (int i = 0; i < dataNodesCount; i++) {
            discoveryNodes.add(newNode("node-" + i, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        }

        var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(indexSettings(IndexVersion.current(), numShards, numReplicas))
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes.masterNodeId("master").localNodeId("master"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .build();
    }

    static ClusterState mutateAllocationStatuses(ClusterState clusterState) {
        final var routingTableBuilder = RoutingTable.builder();
        for (final var indexRoutingTable : clusterState.routingTable()) {
            final var indexRoutingTableBuilder = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final var shardRoutingTable = indexRoutingTable.shard(shardId);
                final var shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardRoutingTable.shardId());
                for (int shardCopy = 0; shardCopy < shardRoutingTable.size(); shardCopy++) {
                    shardRoutingTableBuilder.addShard(mutateAllocationStatus(shardRoutingTable.shard(shardCopy)));
                }
                indexRoutingTableBuilder.addIndexShard(shardRoutingTableBuilder);
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
        }
        return ClusterState.builder(clusterState).routingTable(routingTableBuilder).build();
    }

    private static ShardRouting mutateAllocationStatus(ShardRouting shardRouting) {
        if (shardRouting.unassigned()) {
            var unassignedInfo = shardRouting.unassignedInfo();
            return shardRouting.updateUnassigned(
                new UnassignedInfo(
                    unassignedInfo.reason(),
                    unassignedInfo.message(),
                    unassignedInfo.failure(),
                    unassignedInfo.failedAllocations(),
                    unassignedInfo.unassignedTimeNanos(),
                    unassignedInfo.unassignedTimeMillis(),
                    unassignedInfo.delayed(),
                    randomFrom(
                        UnassignedInfo.AllocationStatus.DECIDERS_NO,
                        UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                        UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED
                    ),
                    unassignedInfo.failedNodeIds(),
                    unassignedInfo.lastAllocatedNodeId()
                ),
                shardRouting.recoverySource()
            );
        } else {
            return shardRouting;
        }
    }

    /**
     * @return a {@link DesiredBalanceComputer} which allocates unassigned primaries to node-0 and unassigned replicas to node-1
     */
    private static DesiredBalanceComputer createDesiredBalanceComputer() {
        return createDesiredBalanceComputer(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (isCorrespondingPrimaryStarted(shardRouting, allocation)) {
                        unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                    } else {
                        unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                    }
                }
            }

            private static boolean isCorrespondingPrimaryStarted(ShardRouting shardRouting, RoutingAllocation allocation) {
                return allocation.routingNodes().assignedShards(shardRouting.shardId()).stream().anyMatch(r -> r.primary() && r.started());
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
    }

    private static DesiredBalanceComputer createDesiredBalanceComputer(ShardsAllocator allocator) {
        return new DesiredBalanceComputer(
            createBuiltInClusterSettings(),
            TimeProviderUtils.create(() -> 0L),
            allocator,
            TEST_ONLY_EXPLAINER
        );
    }

    private static void assertDesiredAssignments(DesiredBalance desiredBalance, Map<ShardId, ShardAssignment> expected) {
        assertThat(desiredBalance.assignments(), equalTo(expected));
    }

    private static DesiredBalanceInput createInput(ClusterState clusterState, ShardRouting... ignored) {
        return new DesiredBalanceInput(randomInt(), routingAllocationOf(clusterState), List.of(ignored));
    }

    private static RoutingAllocation routingAllocationOf(ClusterState clusterState) {
        return new RoutingAllocation(new AllocationDeciders(List.of()), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, 0L);
    }

    private static RoutingAllocation routingAllocationWithDecidersOf(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Settings settings
    ) {
        return new RoutingAllocation(
            randomAllocationDeciders(settings, createBuiltInClusterSettings(settings)),
            clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
    }

    private static Queue<List<MoveAllocationCommand>> queue(MoveAllocationCommand... commands) {
        return new LinkedList<>(List.of(List.of(commands)));
    }
}
