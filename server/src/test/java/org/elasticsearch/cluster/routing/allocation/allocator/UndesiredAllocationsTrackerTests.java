/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class UndesiredAllocationsTrackerTests extends ESTestCase {

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
        final var discoveryNodesBuilder = DiscoveryNodes.builder();

        // The shards with the lowest IDs will have the earliest timestamps
        for (int i = 0; i < initialMaximum; i++) {
            final var routing = createAssignedRouting(index, i, discoveryNodesBuilder);
            indexRoutingTableBuilder.addShard(routing);
            undesiredAllocationsTracker.trackUndesiredAllocation(routing);
        }
        final var routingNodes = RoutingNodes.immutable(
            GlobalRoutingTable.builder().put(ProjectId.DEFAULT, RoutingTable.builder().add(indexRoutingTableBuilder).build()).build(),
            discoveryNodesBuilder.build()
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
        final var remainingShardIds = StreamSupport.stream(undesiredAllocationsTracker.getUndesiredAllocations().spliterator(), false)
            .map(olc -> olc.key.shardId().id())
            .collect(Collectors.toSet());
        assertEquals(IntStream.range(0, reducedMaximum).boxed().collect(Collectors.toSet()), remainingShardIds);
    }

    private ShardRouting createAssignedRouting(Index index, int shardId, DiscoveryNodes.Builder discoveryNodesBuilder) {
        final var nodeId = randomAlphaOfLength(10);
        discoveryNodesBuilder.add(DiscoveryNodeUtils.create(nodeId));
        return ShardRouting.newUnassigned(
            new ShardId(index, shardId),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, randomIdentifier()),
            randomFrom(ShardRouting.Role.DEFAULT, ShardRouting.Role.INDEX_ONLY)
        ).initialize(nodeId, null, randomNonNegativeLong());
    }
}
