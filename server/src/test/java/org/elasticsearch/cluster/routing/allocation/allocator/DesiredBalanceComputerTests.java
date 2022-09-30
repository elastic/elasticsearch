/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceComputerTests extends ESTestCase {

    static final String TEST_INDEX = "test-index";

    public void testSimple() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        var desiredBalance = desiredBalanceComputer.compute(DesiredBalance.INITIAL, createInput(clusterState), queue(), input -> true);

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
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        // if the isFresh flag is false then we only do one iteration, allocating the primaries but not the replicas
        var desiredBalance0 = DesiredBalance.INITIAL;
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
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        var primaryShard = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.INITIAL,
            createInput(clusterState, primaryShard),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of(), 2, 2, 2),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testIgnoresOutOfScopeReplicas() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState();

        var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        var replicaShard = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.INITIAL,
            createInput(clusterState, replicaShard),
            queue(),
            input -> true
        );

        assertDesiredAssignments(
            desiredBalance,
            Map.of(
                new ShardId(index, 0),
                new ShardAssignment(Set.of("node-0"), 2, 1, 1),
                new ShardId(index, 1),
                new ShardAssignment(Set.of("node-0", "node-1"), 2, 0, 0)
            )
        );
    }

    public void testRespectsAssignmentOfUnknownPrimaries() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-2", null, 0L, changes);
                    case 2 -> routingNodes.startShard(logger, iterator.initialize("node-2", null, 0L, changes), changes, 0L);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(logger, iterator.initialize("node-1", null, 0L, changes), changes, 0L),
                        "node-2",
                        0L,
                        changes
                    );
                }
                break;
            }
        }
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.of(clusterState.routingTable().version(), routingNodes))
            .build();

        var desiredBalance = desiredBalanceComputer.compute(DesiredBalance.INITIAL, createInput(clusterState), queue(), input -> true);

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
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                routingNodes.startShard(logger, iterator.initialize("node-2", null, 0L, changes), changes, 0L);
                break;
            }
        }
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0) {
                assert shardRouting.primary() == false;
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-0", null, 0L, changes);
                    case 2 -> routingNodes.startShard(logger, iterator.initialize("node-0", null, 0L, changes), changes, 0L);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(logger, iterator.initialize("node-1", null, 0L, changes), changes, 0L),
                        "node-0",
                        0L,
                        changes
                    );
                }
                break;
            }
        }
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.of(clusterState.routingTable().version(), routingNodes))
            .build();

        var desiredBalance = desiredBalanceComputer.compute(DesiredBalance.INITIAL, createInput(clusterState), queue(), input -> true);

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

    public void testSimulatesAchievingDesiredBalanceBeforeDelegating() {

        var allocateCalled = new AtomicBoolean();
        var desiredBalanceComputer = new DesiredBalanceComputer(new ShardsAllocator() {
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
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        // first, manually assign the shards to their expected locations to pre-populate the desired balance
        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var desiredRoutingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = desiredRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            desiredRoutingNodes.startShard(
                logger,
                iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes),
                changes,
                0L
            );
        }
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.of(clusterState.routingTable().version(), desiredRoutingNodes))
            .build();

        var desiredBalance1 = desiredBalanceComputer.compute(DesiredBalance.INITIAL, createInput(clusterState), queue(), input -> true);
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
                case UNASSIGNED, INITIALIZING -> ShardRoutingState.UNASSIGNED;
                case STARTED -> randomFrom(ShardRoutingState.values());
                case RELOCATING -> randomValueOtherThan(ShardRoutingState.RELOCATING, () -> randomFrom(ShardRoutingState.values()));
            };
            var nodes = new ArrayList<>(List.of("node-0", "node-1", "node-2"));
            Randomness.shuffle(nodes);

            if (primaryRoutingState == ShardRoutingState.UNASSIGNED) {
                continue;
            }
            for (var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary()) {
                    switch (primaryRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(
                            logger,
                            iterator.initialize(nodes.remove(0), null, 0L, changes),
                            changes,
                            0L
                        );
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(logger, iterator.initialize(nodes.remove(0), null, 0L, changes), changes, 0L),
                            nodes.remove(0),
                            0L,
                            changes
                        );
                    }
                    break;
                }
            }

            if (replicaRoutingState == ShardRoutingState.UNASSIGNED) {
                continue;
            }
            for (var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary() == false) {
                    switch (replicaRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(
                            logger,
                            iterator.initialize(nodes.remove(0), null, 0L, changes),
                            changes,
                            0L
                        );
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(logger, iterator.initialize(nodes.remove(0), null, 0L, changes), changes, 0L),
                            nodes.remove(0),
                            0L,
                            changes
                        );
                    }
                    break;
                }
            }
        }
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.of(clusterState.routingTable().version(), randomRoutingNodes))
            .build();

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

        var desiredBalance = desiredBalanceComputer.compute(DesiredBalance.INITIAL, createInput(clusterState), queue(), input -> true);

        assertDesiredAssignments(desiredBalance, Map.of());
    }

    public void testAppliesMoveCommands() {
        var desiredBalanceComputer = createDesiredBalanceComputer();
        var clusterState = createInitialClusterState();
        var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        var routingNodes = clusterState.mutableRoutingNodes();
        for (var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            var shardRouting = iterator.next();
            routingNodes.startShard(
                logger,
                iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes),
                changes,
                0L
            );
        }
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.of(clusterState.routingTable().version(), routingNodes))
            .build();

        var desiredBalance = desiredBalanceComputer.compute(
            DesiredBalance.INITIAL,
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
        for (int node = 0; node < nodes; node++) {
            var nodeId = "node-" + node;
            nodeIds.add(nodeId);
            discoveryNodesBuilder.add(
                new DiscoveryNode(
                    nodeId,
                    nodeId,
                    UUIDs.randomBase64UUID(random()),
                    buildNewFakeTransportAddress(),
                    Map.of(),
                    DiscoveryNodeRole.roles(),
                    Version.CURRENT
                )
            );
        }

        var indices = scaledRandomIntBetween(1, 1000);
        var totalShards = 0;
        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < indices; i++) {
            var indexName = "index-" + i;
            var shards = randomIntBetween(1, 10);
            var replicas = randomIntBetween(1, nodes - 1);
            totalShards += shards * (replicas + 1);
            var inSyncIds = randomList(shards * (replicas + 1), shards * (replicas + 1), () -> UUIDs.randomBase64UUID(random()));

            var indexMetadataBuilder = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas)
                        .put("index.version.created", Version.CURRENT)
                        .build()
                );
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
                remainingNodeIds.add(null);// disconnected node
                var shardId = new ShardId(indexId, shard);
                var primaryNodeId = pickAndRemoveRandomValueFrom(remainingNodeIds);
                indexRoutingTableBuilder.addShard(
                    TestShardRouting.newShardRouting(
                        shardId,
                        primaryNodeId,
                        null,
                        true,
                        primaryNodeId == null ? ShardRoutingState.UNASSIGNED : ShardRoutingState.STARTED,
                        AllocationId.newInitializing(inSyncIds.get(shard * (replicas + 1)))
                    )
                );
                for (int replica = 0; replica < replicas; replica++) {
                    var replicaNodeId = pickAndRemoveRandomValueFrom(remainingNodeIds);
                    indexRoutingTableBuilder.addShard(
                        TestShardRouting.newShardRouting(
                            shardId,
                            replicaNodeId,
                            null,
                            false,
                            replicaNodeId == null ? ShardRoutingState.UNASSIGNED : ShardRoutingState.STARTED,
                            AllocationId.newInitializing(inSyncIds.get(shard * (replicas + 1) + 1 + replica))
                        )
                    );
                }
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
        }

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var iteration = new AtomicInteger(0);

        var input = new DesiredBalanceInput(randomInt(), routingAllocationWithDecidersOf(clusterState), Set.of());
        var desiredBalance = new DesiredBalanceComputer(new BalancedShardsAllocator(Settings.EMPTY)).compute(
            DesiredBalance.INITIAL,
            input,
            queue(),
            ignored -> iteration.incrementAndGet() < 1000
        );

        try {
            assertThat(
                "Balance should converge, but exited by the iteration limit",
                desiredBalance.lastConvergedIndex(),
                equalTo(input.index())
            );
            logger.info(
                "Balance converged after [{}] iterations for [{}] nodes and [{}] total shards",
                iteration.get(),
                nodes,
                totalShards
            );
        } catch (AssertionError e) {
            logger.error(
                "Failed to converge desired balance for [{}] nodes and [{}] total shards:\n {}",
                nodes,
                totalShards,
                clusterState.getRoutingNodes()
            );
            throw e;
        }
    }

    private String pickAndRemoveRandomValueFrom(List<String> values) {
        var value = randomFrom(values);
        values.remove(value);
        return value;
    }

    static ClusterState createInitialClusterState() {
        return createInitialClusterState(3);
    }

    static ClusterState createInitialClusterState(int dataNodesCount) {
        var discoveryNodes = DiscoveryNodes.builder().add(createDiscoveryNode("master", Set.of(DiscoveryNodeRole.MASTER_ROLE)));
        for (int i = 0; i < dataNodesCount; i++) {
            discoveryNodes.add(createDiscoveryNode("node-" + i, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        }

        var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes.masterNodeId("master").localNodeId("master"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();
    }

    private static DiscoveryNode createDiscoveryNode(String id, Set<DiscoveryNodeRole> roles) {
        var transportAddress = buildNewFakeTransportAddress();
        return new DiscoveryNode(
            id,
            id,
            UUIDs.randomBase64UUID(random()),
            transportAddress.address().getHostString(),
            transportAddress.getAddress(),
            transportAddress,
            Map.of(),
            roles,
            Version.CURRENT
        );
    }

    /**
     * @return a {@link DesiredBalanceComputer} which allocates unassigned primaries to node-0 and unassigned replicas to node-1
     */
    private static DesiredBalanceComputer createDesiredBalanceComputer() {
        return new DesiredBalanceComputer(new ShardsAllocator() {
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

    private static void assertDesiredAssignments(DesiredBalance desiredBalance, Map<ShardId, ShardAssignment> expected) {
        assertThat(desiredBalance.assignments(), equalTo(expected));
    }

    private static DesiredBalanceInput createInput(ClusterState clusterState, ShardRouting... ignored) {
        return new DesiredBalanceInput(randomInt(), routingAllocationOf(clusterState), Set.of(ignored));
    }

    private static RoutingAllocation routingAllocationOf(ClusterState clusterState) {
        return new RoutingAllocation(new AllocationDeciders(List.of()), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, 0L);
    }

    private static RoutingAllocation routingAllocationWithDecidersOf(ClusterState clusterState) {
        return new RoutingAllocation(
            new AllocationDeciders(
                ClusterModule.createAllocationDeciders(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    List.of()
                )
            ),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
    }

    private static Queue<List<MoveAllocationCommand>> queue(MoveAllocationCommand... commands) {
        return new LinkedList<>(List.of(List.of(commands)));
    }
}
