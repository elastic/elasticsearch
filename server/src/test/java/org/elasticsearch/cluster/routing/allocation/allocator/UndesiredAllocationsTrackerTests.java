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
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.AdvancingTimeProvider;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UndesiredAllocationsTrackerTests extends ESTestCase {

    public void testShardsArePrunedWhenRemovedFromRoutingTable() {
        final int primaryShards = randomIntBetween(2, 5);
        final int numberOfIndices = randomIntBetween(2, 5);
        final int numberOfNodes = randomIntBetween(2, 5);

        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder()
                .put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), numberOfIndices * primaryShards)
                .build()
        );
        final var advancingTimeProvider = new AdvancingTimeProvider();
        final var undesiredAllocationsTracker = new UndesiredAllocationsTracker(clusterSettings, advancingTimeProvider);

        final var indexNames = IntStream.range(0, numberOfIndices).mapToObj(i -> "index-" + i).toArray(String[]::new);
        final var state = ClusterStateCreationUtils.state(numberOfNodes, indexNames, primaryShards);
        final var routingNodes = RoutingNodes.immutable(state.globalRoutingTable(), state.nodes());

        // Mark all primary shards as undesired
        routingNodes.forEach(routingNode -> {
            routingNode.forEach(shardRouting -> {
                if (shardRouting.primary()) {
                    undesiredAllocationsTracker.trackUndesiredAllocation(shardRouting);
                }
            });
        });
        assertEquals(numberOfIndices * primaryShards, undesiredAllocationsTracker.getUndesiredAllocations().size());

        // Simulate an index being deleted
        ClusterState stateWithIndexRemoved = removeRandomIndex(state);

        // Run cleanup with new RoutingNodes
        undesiredAllocationsTracker.cleanup(
            RoutingNodes.immutable(stateWithIndexRemoved.globalRoutingTable(), stateWithIndexRemoved.nodes())
        );
        assertEquals((numberOfIndices - 1) * primaryShards, undesiredAllocationsTracker.getUndesiredAllocations().size());
        assertTrue(
            undesiredAllocationsTracker.getUndesiredAllocations()
                .values()
                .stream()
                .allMatch(
                    allocation -> stateWithIndexRemoved.routingTable(ProjectId.DEFAULT).index(allocation.shardId().getIndex()) != null
                )
        );
    }

    public void testNewestRecordsArePurgedWhenLimitIsDecreased() {
        final var initialMaximum = randomIntBetween(10, 20);
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder().put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), initialMaximum).build()
        );
        final var advancingTimeProvider = new AdvancingTimeProvider();
        final var undesiredAllocationsTracker = new UndesiredAllocationsTracker(clusterSettings, advancingTimeProvider);
        final var indexName = randomIdentifier();
        final var index = new Index(indexName, indexName);
        final var indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        final var discoveryNodes = randomDiscoveryNodes(randomIntBetween(initialMaximum / 2, initialMaximum));

        // The shards with the lowest IDs will have the earliest timestamps
        for (int i = 0; i < initialMaximum; i++) {
            final var routing = createAssignedRouting(index, i, discoveryNodes);
            indexRoutingTableBuilder.addShard(routing);
            undesiredAllocationsTracker.trackUndesiredAllocation(routing);
        }
        final var routingNodes = RoutingNodes.immutable(
            GlobalRoutingTable.builder().put(ProjectId.DEFAULT, RoutingTable.builder().add(indexRoutingTableBuilder).build()).build(),
            discoveryNodes
        );

        // Reduce the maximum
        final var reducedMaximum = randomIntBetween(1, initialMaximum);
        clusterSettings.applySettings(
            Settings.builder().put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), reducedMaximum).build()
        );

        // We shouldn't purge the entries from the setting updater thread
        assertEquals(initialMaximum, undesiredAllocationsTracker.getUndesiredAllocations().size());

        // We should purge the most recent entries in #cleanup
        undesiredAllocationsTracker.cleanup(routingNodes);
        assertEquals(reducedMaximum, undesiredAllocationsTracker.getUndesiredAllocations().size());
        final var remainingShardIds = undesiredAllocationsTracker.getUndesiredAllocations()
            .values()
            .stream()
            .map(allocation -> allocation.shardId().id())
            .collect(Collectors.toSet());
        assertEquals(IntStream.range(0, reducedMaximum).boxed().collect(Collectors.toSet()), remainingShardIds);
    }

    public void testCannotTrackMoreShardsThanTheLimit() {
        final int maxToTrack = randomIntBetween(1, 10);
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder().put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), maxToTrack).build()
        );
        final var advancingTimeProvider = new AdvancingTimeProvider();
        final var undesiredAllocationsTracker = new UndesiredAllocationsTracker(clusterSettings, advancingTimeProvider);
        final var index = new Index(randomIdentifier(), randomIdentifier());

        final int shardsToAdd = randomIntBetween(maxToTrack + 1, maxToTrack * 2);
        for (int i = 0; i < shardsToAdd; i++) {
            final var routing = createAssignedRouting(index, i);
            undesiredAllocationsTracker.trackUndesiredAllocation(routing);
        }

        // Only the first {maxToTrack} shards should be tracked
        assertEquals(maxToTrack, undesiredAllocationsTracker.getUndesiredAllocations().size());
        final var trackedShardIds = undesiredAllocationsTracker.getUndesiredAllocations()
            .values()
            .stream()
            .map(allocation -> allocation.shardId().id())
            .collect(Collectors.toSet());
        assertEquals(IntStream.range(0, maxToTrack).boxed().collect(Collectors.toSet()), trackedShardIds);
    }

    public void testUndesiredAllocationsAreIdentifiableDespiteMetadataChanges() {
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder().put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), randomIntBetween(1, 10)).build()
        );
        final var advancingTimeProvider = new AdvancingTimeProvider();
        final var undesiredAllocationsTracker = new UndesiredAllocationsTracker(clusterSettings, advancingTimeProvider);
        final var index = new Index(randomIdentifier(), randomIdentifier());

        ShardRouting shardRouting = createAssignedRouting(index, 0);

        undesiredAllocationsTracker.trackUndesiredAllocation(shardRouting);
        assertEquals(1, undesiredAllocationsTracker.getUndesiredAllocations().size());

        // move to started
        shardRouting = shardRouting.moveToStarted(randomNonNegativeLong());
        undesiredAllocationsTracker.trackUndesiredAllocation(shardRouting);
        assertEquals(1, undesiredAllocationsTracker.getUndesiredAllocations().size());

        // start a relocation
        shardRouting = shardRouting.relocate(randomIdentifier(), randomNonNegativeLong());
        undesiredAllocationsTracker.trackUndesiredAllocation(shardRouting);
        assertEquals(1, undesiredAllocationsTracker.getUndesiredAllocations().size());

        // cancel that relocation
        shardRouting = shardRouting.cancelRelocation();
        undesiredAllocationsTracker.removeTracking(shardRouting);
        assertEquals(0, undesiredAllocationsTracker.getUndesiredAllocations().size());
    }

    public void testMaybeLogUndesiredAllocations() {
        final int maxShardsToTrack = randomIntBetween(2, 8);
        final var warningThreshold = TimeValue.timeValueMinutes(randomIntBetween(1, 5));
        final var logInterval = TimeValue.timeValueMinutes(randomIntBetween(1, 5));
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder()
                .put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), maxShardsToTrack)
                .put(UndesiredAllocationsTracker.UNDESIRED_ALLOCATION_DURATION_LOG_INTERVAL_SETTING.getKey(), logInterval)
                .put(UndesiredAllocationsTracker.UNDESIRED_ALLOCATION_DURATION_LOG_THRESHOLD_SETTING.getKey(), warningThreshold)
                .build()
        );
        final var advancingTimeProvider = new AdvancingTimeProvider();
        final var undesiredAllocationsTracker = new UndesiredAllocationsTracker(clusterSettings, advancingTimeProvider);
        final var indexName = randomIdentifier();

        final var state = ClusterStateCreationUtils.state(2, new String[] { indexName }, 1);
        final var shardRouting = state.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard();
        final var routingNodes = RoutingNodes.immutable(state.globalRoutingTable(), state.nodes());
        final var alwaysSaysNo = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return allocation.decision(Decision.NO, "test_no_decider", "Always says no");
            }
        };
        final var alwaysSaysYes = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return allocation.decision(Decision.YES, "test_yes_decider", "Always says yes");
            }
        };
        final var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of(alwaysSaysNo, alwaysSaysYes)),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        final var currentNodeId = shardRouting.currentNodeId();
        final var otherNodeId = state.nodes().getNodes().keySet().stream().filter(n -> n.equals(currentNodeId) == false).findFirst().get();

        undesiredAllocationsTracker.trackUndesiredAllocation(shardRouting);

        final String shardInUndesiredLocationLogString = "Shard * has been in an undesired allocation for *";

        final var desiredBalance = new DesiredBalance(
            randomNonNegativeInt(),
            Map.of(shardRouting.shardId(), new ShardAssignment(Set.of(otherNodeId), 1, 0, 0))
        );
        // Nothing should be logged because we haven't passed the minimal time
        MockLog.assertThatLogger(
            () -> undesiredAllocationsTracker.maybeLogUndesiredShardsWarning(routingNodes, allocation, desiredBalance),
            UndesiredAllocationsTracker.class,
            new MockLog.UnseenEventExpectation(
                "undesired allocation log",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                shardInUndesiredLocationLogString
            )
        );

        // Advance past the threshold
        advancingTimeProvider.advanceByMillis(randomLongBetween(warningThreshold.millis() + 1, warningThreshold.millis() * 2));

        // We should log now because we've passed the threshold
        MockLog.assertThatLogger(
            () -> undesiredAllocationsTracker.maybeLogUndesiredShardsWarning(routingNodes, allocation, desiredBalance),
            UndesiredAllocationsTracker.class,
            new MockLog.SeenEventExpectation(
                "undesired allocation log",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                shardInUndesiredLocationLogString
            ),
            new MockLog.SeenEventExpectation(
                "no decision for other node",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "Shard * allocation decision for node [" + otherNodeId + "]: [NO(Always says no)]"
            ),
            new MockLog.UnseenEventExpectation(
                "yes decision for other node",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "*Always says yes*"
            )
        );

        // Rate-limiting should prevent us from logging again immediately
        MockLog.assertThatLogger(
            () -> undesiredAllocationsTracker.maybeLogUndesiredShardsWarning(routingNodes, allocation, desiredBalance),
            UndesiredAllocationsTracker.class,
            new MockLog.UnseenEventExpectation(
                "undesired allocation log",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                shardInUndesiredLocationLogString
            )
        );

        // Advance past the log interval
        advancingTimeProvider.advanceByMillis(randomLongBetween(logInterval.millis() + 1, logInterval.millis() * 2));

        // Test logging where desired node has left the cluster
        final var absentDesiredNodeId = randomIdentifier() + "-left-cluster";
        final var desiredBalanceNodeLeft = new DesiredBalance(
            randomNonNegativeInt(),
            Map.of(shardRouting.shardId(), new ShardAssignment(Set.of(absentDesiredNodeId), 1, 0, 0))
        );
        MockLog.assertThatLogger(
            () -> undesiredAllocationsTracker.maybeLogUndesiredShardsWarning(routingNodes, allocation, desiredBalanceNodeLeft),
            UndesiredAllocationsTracker.class,
            new MockLog.SeenEventExpectation(
                "undesired allocation log",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                shardInUndesiredLocationLogString
            ),
            new MockLog.SeenEventExpectation(
                "no decision for other node",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "Shard * desired node [" + absentDesiredNodeId + "] has left the cluster"
            )
        );

        // Advance past the log interval
        advancingTimeProvider.advanceByMillis(randomLongBetween(logInterval.millis() + 1, logInterval.millis() * 2));

        // Test logging is skipped where there is no desired balance for the shard (not sure if this can happen, but just to be safe)
        try (Releasable ignored = undesiredAllocationsTracker.disableMissingAllocationAssertions()) {
            final var desiredBalanceWithNoEntryForShard = new DesiredBalance(randomNonNegativeInt(), Map.of());
            MockLog.assertThatLogger(
                () -> undesiredAllocationsTracker.maybeLogUndesiredShardsWarning(
                    routingNodes,
                    allocation,
                    desiredBalanceWithNoEntryForShard
                ),
                UndesiredAllocationsTracker.class,
                new MockLog.UnseenEventExpectation(
                    "undesired allocation log",
                    UndesiredAllocationsTracker.class.getName(),
                    Level.WARN,
                    shardInUndesiredLocationLogString
                )
            );
        }
    }

    public void testMaybeLogUndesiredAllocationsWillNotLogWhenNodeAndShardRolesAreMismatched() {
        final var indexName = randomIdentifier();
        final var clusterState = ClusterStateCreationUtils.buildServerlessRoleNodes(indexName, 1, 2, 2, 0);
        var index = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).getIndex();
        final var searchNode = getRandomNodeWithRole(clusterState, DiscoveryNodeRole.SEARCH_ROLE);
        final var indexingNode = getRandomNodeWithRole(clusterState, DiscoveryNodeRole.INDEX_ROLE);

        // Allocate a shard to the search node and a shard to the indexing node
        final var indexShardRouting = createAssignedRouting(index, 0, indexingNode, ShardRouting.Role.INDEX_ONLY, true).moveToStarted(
            randomNonNegativeLong()
        );
        final var searchShardRouting = createAssignedRouting(index, 0, searchNode, ShardRouting.Role.SEARCH_ONLY, false).moveToStarted(
            randomNonNegativeLong()
        );
        final var clusterStateWithRoutingsAdded = ClusterState.builder(clusterState)
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(
                        ProjectId.DEFAULT,
                        RoutingTable.builder()
                            .add(IndexRoutingTable.builder(index).addShard(indexShardRouting).addShard(searchShardRouting).build())
                    )
                    .build()
            )
            .build();

        final var routingNodes = RoutingNodes.immutable(
            clusterStateWithRoutingsAdded.globalRoutingTable(),
            clusterStateWithRoutingsAdded.nodes()
        );
        final var allocation = new RoutingAllocation(new AllocationDeciders(List.<AllocationDecider>of(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return allocation.decision(Decision.NO, "test_no_decider", "role: " + shardRouting.role());
            }
        })), clusterStateWithRoutingsAdded, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, randomNonNegativeLong());

        final int maxShardsToTrack = randomIntBetween(2, 8);
        final var warningThreshold = TimeValue.timeValueMinutes(randomIntBetween(1, 5));
        final var logInterval = TimeValue.timeValueMinutes(randomIntBetween(1, 5));
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder()
                .put(UndesiredAllocationsTracker.MAX_UNDESIRED_ALLOCATIONS_TO_TRACK.getKey(), maxShardsToTrack)
                .put(UndesiredAllocationsTracker.UNDESIRED_ALLOCATION_DURATION_LOG_INTERVAL_SETTING.getKey(), logInterval)
                .put(UndesiredAllocationsTracker.UNDESIRED_ALLOCATION_DURATION_LOG_THRESHOLD_SETTING.getKey(), warningThreshold)
                .build()
        );
        final var advancingTimeProvider = new AdvancingTimeProvider();
        final var undesiredAllocationsTracker = new UndesiredAllocationsTracker(clusterSettings, advancingTimeProvider);

        undesiredAllocationsTracker.trackUndesiredAllocation(indexShardRouting);
        undesiredAllocationsTracker.trackUndesiredAllocation(searchShardRouting);

        // Advance past the warning threshold
        advancingTimeProvider.advanceByMillis(randomLongBetween(warningThreshold.millis() + 1, warningThreshold.millis() * 2));

        MockLog.assertThatLogger(
            () -> undesiredAllocationsTracker.maybeLogUndesiredShardsWarning(
                routingNodes,
                allocation,
                new DesiredBalance(
                    0,
                    Map.of(
                        searchShardRouting.shardId(),
                        new ShardAssignment(
                            clusterStateWithRoutingsAdded.nodes()
                                .stream()
                                .filter(node -> node != searchNode && node != indexingNode)
                                .map(DiscoveryNode::getId)
                                .collect(Collectors.toSet()),
                            2,
                            0,
                            0
                        )
                    )
                )
            ),
            UndesiredAllocationsTracker.class,
            new MockLog.SeenEventExpectation(
                "Decision for indexing node/index shard",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "Shard * allocation decision for node [index_*]: [NO(role: INDEX_ONLY)]"
            ),
            new MockLog.SeenEventExpectation(
                "Decision for search node/search shard",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "Shard * allocation decision for node [search_*]: [NO(role: SEARCH_ONLY)]"
            ),
            new MockLog.UnseenEventExpectation(
                "Decision for index node/search shard",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "Shard * allocation decision for node [index_*]: [NO(role: SEARCH_ONLY)]"
            ),
            new MockLog.UnseenEventExpectation(
                "Decision for search node/index shard",
                UndesiredAllocationsTracker.class.getName(),
                Level.WARN,
                "Shard * allocation decision for node [search_*]: [NO(role: INDEX_ONLY)]"
            )
        );
    }

    private static DiscoveryNode getRandomNodeWithRole(ClusterState clusterState, DiscoveryNodeRole indexRole) {
        return randomFrom(clusterState.nodes().stream().filter(n -> n.getRoles().contains(indexRole)).toList());
    }

    private ClusterState removeRandomIndex(ClusterState state) {
        RoutingTable originalRoutingTable = state.routingTable(ProjectId.DEFAULT);
        RoutingTable updatedRoutingTable = RoutingTable.builder(originalRoutingTable)
            .remove(randomFrom(originalRoutingTable.indicesRouting().keySet()))
            .build();
        return ClusterState.builder(state)
            .routingTable(GlobalRoutingTable.builder().put(ProjectId.DEFAULT, updatedRoutingTable).build())
            .build();
    }

    private ShardRouting createAssignedRouting(Index index, int shardId) {
        return createAssignedRouting(index, shardId, null);
    }

    private ShardRouting createAssignedRouting(Index index, int shardId, @Nullable DiscoveryNodes discoveryNodes) {
        return createAssignedRouting(
            index,
            shardId,
            discoveryNodes == null ? null : randomFrom(discoveryNodes.getAllNodes()),
            randomFrom(randomFrom(ShardRouting.Role.DEFAULT, ShardRouting.Role.INDEX_ONLY)),
            true
        );
    }

    private ShardRouting createAssignedRouting(
        Index index,
        int shardId,
        @Nullable DiscoveryNode discoveryNode,
        ShardRouting.Role role,
        boolean primary
    ) {
        final var nodeId = discoveryNode == null ? randomAlphaOfLength(10) : discoveryNode.getId();
        return ShardRouting.newUnassigned(
            new ShardId(index, shardId),
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, randomIdentifier()),
            role
        ).initialize(nodeId, null, randomNonNegativeLong());
    }

    private DiscoveryNodes randomDiscoveryNodes(int numberOfNodes) {
        final var nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(DiscoveryNodeUtils.create("node-" + i));
        }
        return nodes.build();
    }
}
