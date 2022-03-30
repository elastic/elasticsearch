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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceServiceTests extends ESTestCase {

    static final String TEST_INDEX = "test-index";

    public void testSimple() {
        final var desiredBalanceService = getAllocatingDesiredBalanceService();
        final var clusterState = getInitialClusterState();
        assertDesiredAssignments(desiredBalanceService, Map.of());
        assertTrue(executeAndReroute(desiredBalanceService, clusterState));

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );
        assertFalse(executeAndReroute(desiredBalanceService, clusterState));
    }

    public void testStopsComputingWhenStale() {
        final var desiredBalanceService = getAllocatingDesiredBalanceService();
        final var clusterState = getInitialClusterState();

        // if the isFresh flag is false then we only do one iteration, allocating the primaries but not the replicas
        assertTrue(executeAndRerouteStale(desiredBalanceService, clusterState));
        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0"), new ShardId(index, 1), Set.of("node-0"))
        );

        // the next iteration allocates the replicas whether stale or fresh
        assertTrue(
            randomBoolean()
                ? executeAndRerouteStale(desiredBalanceService, clusterState)
                : executeAndReroute(desiredBalanceService, clusterState)
        );
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

    }

    public void testIgnoresOutOfScopePrimaries() {
        final var desiredBalanceService = getAllocatingDesiredBalanceService();
        final var clusterState = getInitialClusterState();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        final var primaryShard = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        assertTrue(executeAndRerouteWithIgnoredShards(desiredBalanceService, clusterState, List.of(primaryShard)));
        assertDesiredAssignments(desiredBalanceService, Map.of(new ShardId(index, 1), Set.of("node-0", "node-1")));
    }

    public void testIgnoresOutOfScopeReplicas() {
        final var desiredBalanceService = getAllocatingDesiredBalanceService();
        final var clusterState = getInitialClusterState();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        final var replicaShard = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        assertTrue(executeAndRerouteWithIgnoredShards(desiredBalanceService, clusterState, List.of(replicaShard)));
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );
    }

    public void testRespectsAssignmentOfUnknownPrimaries() {
        final var desiredBalanceService = getAllocatingDesiredBalanceService();
        final var clusterState = getInitialClusterState();

        final var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        final var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-2", null, 0L, changes);
                    case 2 -> routingNodes.startShard(logger, iterator.initialize("node-2", null, 0L, changes), changes);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(logger, iterator.initialize("node-1", null, 0L, changes), changes),
                        "node-2",
                        0L,
                        changes
                    );
                }
                break;
            }
        }

        assertTrue(
            executeAndReroute(
                desiredBalanceService,
                ClusterState.builder(clusterState)
                    .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), routingNodes))
                    .build()
            )
        );

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-2", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );
    }

    public void testRespectsAssignmentOfUnknownReplicas() {
        final var desiredBalanceService = getAllocatingDesiredBalanceService();
        final var clusterState = getInitialClusterState();

        final var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        final var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                routingNodes.startShard(logger, iterator.initialize("node-2", null, 0L, changes), changes);
                break;
            }
        }

        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0) {
                assert shardRouting.primary() == false;
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-0", null, 0L, changes);
                    case 2 -> routingNodes.startShard(logger, iterator.initialize("node-0", null, 0L, changes), changes);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(logger, iterator.initialize("node-1", null, 0L, changes), changes),
                        "node-0",
                        0L,
                        changes
                    );
                }
                break;
            }
        }

        assertTrue(
            executeAndReroute(
                desiredBalanceService,
                ClusterState.builder(clusterState)
                    .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), routingNodes))
                    .build()
            )
        );

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-2", "node-0"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );
    }

    public void testSimulatesAchievingDesiredBalanceBeforeDelegating() {

        final var allocateCalled = new AtomicBoolean();
        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                assertTrue(allocateCalled.compareAndSet(false, true));
                // whatever the allocation in the current cluster state, the desired balance service should start by moving all the
                // known shards to their desired locations before delegating to the inner allocator
                for (final var routingNode : allocation.routingNodes()) {
                    assertThat(
                        allocation.routingNodes().toString(),
                        routingNode.numberOfOwningShards(),
                        equalTo(routingNode.nodeId().equals("node-2") ? 0 : 2)
                    );
                    for (final var shardRouting : routingNode) {
                        assertTrue(shardRouting.toString(), shardRouting.started());
                    }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var clusterState = getInitialClusterState();

        // first, manually assign the shards to their expected locations to pre-populate the desired balance
        final var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        final var desiredRoutingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = desiredRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            desiredRoutingNodes.startShard(
                logger,
                iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes),
                changes
            );
        }
        assertTrue(
            executeAndReroute(
                desiredBalanceService,
                ClusterState.builder(clusterState)
                    .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), desiredRoutingNodes))
                    .build()
            )
        );

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

        // now create a cluster state with the routing table in a random state
        final var randomRoutingNodes = clusterState.mutableRoutingNodes();
        for (int shard = 0; shard < 2; shard++) {
            final var primaryRoutingState = randomFrom(ShardRoutingState.values());
            final var replicaRoutingState = switch (primaryRoutingState) {
                case UNASSIGNED, INITIALIZING -> ShardRoutingState.UNASSIGNED;
                case STARTED -> randomFrom(ShardRoutingState.values());
                case RELOCATING -> randomValueOtherThan(ShardRoutingState.RELOCATING, () -> randomFrom(ShardRoutingState.values()));
            };
            final var nodes = new ArrayList<>(List.of("node-0", "node-1", "node-2"));
            Randomness.shuffle(nodes);

            if (primaryRoutingState == ShardRoutingState.UNASSIGNED) {
                continue;
            }
            for (final var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary()) {
                    switch (primaryRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(
                            logger,
                            iterator.initialize(nodes.remove(0), null, 0L, changes),
                            changes
                        );
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(logger, iterator.initialize(nodes.remove(0), null, 0L, changes), changes),
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
            for (final var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary() == false) {
                    switch (replicaRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(
                            logger,
                            iterator.initialize(nodes.remove(0), null, 0L, changes),
                            changes
                        );
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(logger, iterator.initialize(nodes.remove(0), null, 0L, changes), changes),
                            nodes.remove(0),
                            0L,
                            changes
                        );
                    }
                    break;
                }
            }
        }

        allocateCalled.set(false);
        assertFalse(
            executeAndReroute(
                desiredBalanceService,
                ClusterState.builder(clusterState)
                    .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), randomRoutingNodes))
                    .build()
            )
        );
        assertTrue(allocateCalled.get());
        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );
    }

    static ClusterState getInitialClusterState() {
        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes.masterNodeId("node-0").localNodeId("node-0"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();
    }

    /**
     * @return a {@link DesiredBalanceService} which allocates unassigned primaries to node-0 and unassigned replicas to node-1
     */
    private static DesiredBalanceService getAllocatingDesiredBalanceService() {
        return new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (allocation.routingNodes()
                        .assignedShards(shardRouting.shardId())
                        .stream()
                        .anyMatch(r -> r.primary() && r.started())) {
                            unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                        }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });
    }

    private static boolean executeAndRerouteWithIgnoredShards(
        DesiredBalanceService desiredBalanceService,
        ClusterState clusterState,
        List<ShardRouting> ignoredShards
    ) {
        return executeAndReroute(desiredBalanceService, clusterState, true, ignoredShards);
    }

    private static boolean executeAndRerouteStale(DesiredBalanceService desiredBalanceService, ClusterState clusterState) {
        return executeAndReroute(desiredBalanceService, clusterState, false, List.of());
    }

    private static boolean executeAndReroute(DesiredBalanceService desiredBalanceService, ClusterState clusterState) {
        return executeAndReroute(desiredBalanceService, clusterState, true, List.of());
    }

    private static boolean executeAndReroute(
        DesiredBalanceService desiredBalanceService,
        ClusterState clusterState,
        boolean isFresh,
        List<ShardRouting> ignoredShards
    ) {
        return desiredBalanceService.updateDesiredBalanceAndReroute(
            new DesiredBalanceInput(
                new RoutingAllocation(new AllocationDeciders(List.of()), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, 0L),
                ignoredShards
            ),
            () -> isFresh
        );
    }

    private static void assertDesiredAssignments(DesiredBalanceService desiredBalanceService, Map<ShardId, Set<String>> expected) {
        assertThat(
            desiredBalanceService.getCurrentDesiredBalance()
                .desiredAssignments()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Set.copyOf(e.getValue()))),
            equalTo(expected)
        );
    }

}
