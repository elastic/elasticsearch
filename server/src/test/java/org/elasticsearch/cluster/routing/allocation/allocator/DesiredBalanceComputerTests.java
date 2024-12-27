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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.time.TimeProviderUtils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService.SnapshotShard;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.MockLog;

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

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class DesiredBalanceComputerTests extends ESAllocationTestCase {

    static final String TEST_INDEX = "test-index";

    public void testComputeBalance() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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

    public void testIgnoresOutOfScopePrimaries() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = mutateAllocationStatuses(createInitialClusterState(3));
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();
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

        var index = clusterState.metadata().index(TEST_INDEX).getIndex();
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
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.of(routingNodes)).build();

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
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.of(routingNodes)).build();

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
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.of(routingNodes)).build();

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
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

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
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.of(desiredRoutingNodes)).build();

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
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.of(randomRoutingNodes)).build();

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
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState(3);
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            routingNodes.startShard(iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes), changes, 0L);
        }
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.of(routingNodes)).build();

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            createInput(clusterState),
            queue(
                new MoveAllocationCommand(index.getName(), 0, "node-1", "node-2"),
                new MoveAllocationCommand(index.getName(), 1, "node-1", "node-2")
            ),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)
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

        var clusterInfo = new ClusterInfo(diskUsage, diskUsage, shardSizes, Map.of(), dataPath, Map.of());

        var settings = Settings.EMPTY;

        var input = new DesiredBalanceInput(randomInt(), routingAllocationWithDecidersOf(clusterState, clusterInfo, settings), List.of());
        var desiredBalance = createDesiredBalanceComputer(new BalancedShardsAllocator(settings)).compute(
            DesiredBalance.BECOME_MASTER_INITIAL,
            input,
            queue(),
            ignored -> iteration.incrementAndGet() < 1000
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
            .withShard(findShardId(clusterState, "index-0"), true, 500)
            .withShard(findShardId(clusterState, "index-0"), false, 500)
            // node-0
            .withShard(findShardId(clusterState, "index-1"), true, 400)
            // node-1
            .withShard(findShardId(clusterState, "index-2"), true, 50)
            .withShard(findShardId(clusterState, "index-3"), true, 50)
            .withShard(findShardId(clusterState, "index-4"), true, 50)
            .withShard(findShardId(clusterState, "index-5"), true, 50)
            .withShard(findShardId(clusterState, "index-6"), true, 50)
            .withShard(findShardId(clusterState, "index-7"), true, 50)
            .withShard(findShardId(clusterState, "index-8"), true, 50)
            .withShard(findShardId(clusterState, "index-9"), true, 50)
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

    private static class ClusterInfoTestBuilder {

        private final Map<String, DiskUsage> diskUsage = new HashMap<>();
        private final Map<String, Long> shardSizes = new HashMap<>();
        private final Map<NodeAndPath, ReservedSpace> reservedSpace = new HashMap<>();

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

        public ClusterInfoTestBuilder withReservedSpace(String nodeId, long size, ShardId... shardIds) {
            reservedSpace.put(new NodeAndPath(nodeId, "/path"), new ReservedSpace(size, Set.of(shardIds)));
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(diskUsage, diskUsage, shardSizes, Map.of(), Map.of(), reservedSpace);
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
                "Desired balance computation for [*] is still not converged after [1m] and [60] iterations"
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
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        assertThatLogger(() -> {
            var iteration = new AtomicInteger(0);
            desiredBalanceComputer.compute(
                DesiredBalance.BECOME_MASTER_INITIAL,
                createInput(createInitialClusterState(3)),
                queue(),
                input -> iteration.incrementAndGet() < iterations
            );
        }, DesiredBalanceComputer.class, expectation);
    }

    private static ShardId findShardId(ClusterState clusterState, String name) {
        return clusterState.getRoutingTable().index(name).shard(0).shardId();
    }

    static ClusterState createInitialClusterState(int dataNodesCount) {
        var discoveryNodes = DiscoveryNodes.builder().add(newNode("master", Set.of(DiscoveryNodeRole.MASTER_ROLE)));
        for (int i = 0; i < dataNodesCount; i++) {
            discoveryNodes.add(newNode("node-" + i, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        }

        var indexMetadata = IndexMetadata.builder(TEST_INDEX).settings(indexSettings(IndexVersion.current(), 2, 1)).build();

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
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
    }

    private static DesiredBalanceComputer createDesiredBalanceComputer(ShardsAllocator allocator) {
        return new DesiredBalanceComputer(createBuiltInClusterSettings(), TimeProviderUtils.create(() -> 0L), allocator);
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
