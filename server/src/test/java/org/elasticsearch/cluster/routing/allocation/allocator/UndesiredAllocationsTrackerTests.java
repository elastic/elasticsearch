/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.AdvancingTimeProvider;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

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
        final var nodeId = discoveryNodes == null ? randomAlphaOfLength(10) : randomFrom(discoveryNodes.getNodes().keySet());
        return ShardRouting.newUnassigned(
            new ShardId(index, shardId),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, randomIdentifier()),
            randomFrom(ShardRouting.Role.DEFAULT, ShardRouting.Role.INDEX_ONLY)
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
