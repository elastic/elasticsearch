/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class WriteLoadPerShardSimulatorTests extends ESTestCase {

    private static final RoutingChangesObserver NOOP = new RoutingChangesObserver() {
    };
    public static final String[] INDICES = { "indexOne", "indexTwo", "indexThree" };

    /**
     * We should not adjust the values if there's no movement
     */
    public void testNoShardMovement() {
        final var originalNode0WriteLoadStats = randomUsageStats();
        final var originalNode1WriteLoadStats = randomUsageStats();
        final var allocation = createRoutingAllocation(originalNode0WriteLoadStats, originalNode1WriteLoadStats, Set.of());

        final var writeLoadPerShardSimulator = new WriteLoadPerShardSimulator(allocation);
        final var calculatedNodeUsageStates = writeLoadPerShardSimulator.nodeUsageStatsForThreadPools();
        assertThat(calculatedNodeUsageStates, Matchers.aMapWithSize(2));
        assertThat(
            calculatedNodeUsageStates.get("node_0").threadPoolUsageStatsMap().get("write"),
            sameInstance(originalNode0WriteLoadStats)
        );
        assertThat(
            calculatedNodeUsageStates.get("node_1").threadPoolUsageStatsMap().get("write"),
            sameInstance(originalNode1WriteLoadStats)
        );
    }

    public void testMovementOfAShardWillReduceThreadPoolUtilisation() {
        final var originalNode0WriteLoadStats = randomUsageStats();
        final var originalNode1WriteLoadStats = randomUsageStats();
        final var allocation = createRoutingAllocation(originalNode0WriteLoadStats, originalNode1WriteLoadStats, Set.of());
        final var writeLoadPerShardSimulator = new WriteLoadPerShardSimulator(allocation);

        // Relocate a random shard from node_0 to node_1
        final var randomShard = randomFrom(StreamSupport.stream(allocation.routingNodes().node("node_0").spliterator(), false).toList());
        final var moveShardTuple = allocation.routingNodes().relocateShard(randomShard, "node_1", randomNonNegativeLong(), "testing", NOOP);
        writeLoadPerShardSimulator.simulateShardStarted(moveShardTuple.v2());

        final var calculatedNodeUsageStates = writeLoadPerShardSimulator.nodeUsageStatsForThreadPools();
        assertThat(calculatedNodeUsageStates, Matchers.aMapWithSize(2));

        // Some node_0 utilization should have been moved to node_1
        assertThat(
            getAverageWritePoolUtilization(writeLoadPerShardSimulator, "node_0"),
            lessThan(originalNode0WriteLoadStats.averageThreadPoolUtilization())
        );
        assertThat(
            getAverageWritePoolUtilization(writeLoadPerShardSimulator, "node_1"),
            greaterThan(originalNode1WriteLoadStats.averageThreadPoolUtilization())
        );
    }

    public void testMovementFollowedByMovementBackWillNotChangeAnything() {
        final var originalNode0WriteLoadStats = randomUsageStats();
        final var originalNode1WriteLoadStats = randomUsageStats();
        final var allocation = createRoutingAllocation(originalNode0WriteLoadStats, originalNode1WriteLoadStats, Set.of());
        final var writeLoadPerShardSimulator = new WriteLoadPerShardSimulator(allocation);

        // Relocate a random shard from node_0 to node_1
        final long expectedShardSize = randomNonNegativeLong();
        final var randomShard = randomFrom(StreamSupport.stream(allocation.routingNodes().node("node_0").spliterator(), false).toList());
        final var moveShardTuple = allocation.routingNodes().relocateShard(randomShard, "node_1", expectedShardSize, "testing", NOOP);
        writeLoadPerShardSimulator.simulateShardStarted(moveShardTuple.v2());
        final ShardRouting movedAndStartedShard = allocation.routingNodes().startShard(moveShardTuple.v2(), NOOP, expectedShardSize);

        // Some node_0 utilization should have been moved to node_1
        assertThat(
            getAverageWritePoolUtilization(writeLoadPerShardSimulator, "node_0"),
            lessThan(originalNode0WriteLoadStats.averageThreadPoolUtilization())
        );
        assertThat(
            getAverageWritePoolUtilization(writeLoadPerShardSimulator, "node_1"),
            greaterThan(originalNode1WriteLoadStats.averageThreadPoolUtilization())
        );

        // Then move it back
        final var moveBackTuple = allocation.routingNodes()
            .relocateShard(movedAndStartedShard, "node_0", expectedShardSize, "testing", NOOP);
        writeLoadPerShardSimulator.simulateShardStarted(moveBackTuple.v2());

        // The utilization numbers should be back to their original values
        assertThat(
            getAverageWritePoolUtilization(writeLoadPerShardSimulator, "node_0"),
            equalTo(originalNode0WriteLoadStats.averageThreadPoolUtilization())
        );
        assertThat(
            getAverageWritePoolUtilization(writeLoadPerShardSimulator, "node_1"),
            equalTo(originalNode1WriteLoadStats.averageThreadPoolUtilization())
        );
    }

    public void testMovementBetweenNodesWithNoThreadPoolStats() {
        final var originalNode0WriteLoadStats = randomBoolean() ? randomUsageStats() : null;
        final var originalNode1WriteLoadStats = randomBoolean() ? randomUsageStats() : null;
        final var allocation = createRoutingAllocation(
            originalNode0WriteLoadStats,
            originalNode1WriteLoadStats,
            new HashSet<>(randomSubsetOf(Arrays.asList(INDICES)))
        );
        final var writeLoadPerShardSimulator = new WriteLoadPerShardSimulator(allocation);

        // Relocate a random shard from node_0 to node_1
        final long expectedShardSize = randomNonNegativeLong();
        final var randomShard = randomFrom(StreamSupport.stream(allocation.routingNodes().node("node_0").spliterator(), false).toList());
        final var moveShardTuple = allocation.routingNodes().relocateShard(randomShard, "node_1", expectedShardSize, "testing", NOOP);
        writeLoadPerShardSimulator.simulateShardStarted(moveShardTuple.v2());
        allocation.routingNodes().startShard(moveShardTuple.v2(), NOOP, expectedShardSize);

        final var generated = writeLoadPerShardSimulator.nodeUsageStatsForThreadPools();
        assertThat(generated.containsKey("node_0"), equalTo(originalNode0WriteLoadStats != null));
        assertThat(generated.containsKey("node_1"), equalTo(originalNode1WriteLoadStats != null));
    }

    private float getAverageWritePoolUtilization(WriteLoadPerShardSimulator writeLoadPerShardSimulator, String nodeId) {
        final var generatedNodeUsageStates = writeLoadPerShardSimulator.nodeUsageStatsForThreadPools();
        final var node0WritePoolStats = generatedNodeUsageStates.get(nodeId).threadPoolUsageStatsMap().get("write");
        return node0WritePoolStats.averageThreadPoolUtilization();
    }

    private NodeUsageStatsForThreadPools.ThreadPoolUsageStats randomUsageStats() {
        return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            randomIntBetween(4, 16),
            randomFloatBetween(0.1f, 1.0f, true),
            randomLongBetween(0, 60_000)
        );
    }

    private RoutingAllocation createRoutingAllocation(
        NodeUsageStatsForThreadPools.ThreadPoolUsageStats node0WriteLoadStats,
        NodeUsageStatsForThreadPools.ThreadPoolUsageStats node1WriteLoadStats,
        Set<String> indicesWithNoWriteLoad
    ) {
        final Map<String, NodeUsageStatsForThreadPools> nodeUsageStats = new HashMap<>();
        if (node0WriteLoadStats != null) {
            nodeUsageStats.put("node_0", new NodeUsageStatsForThreadPools("node_0", Map.of("write", node0WriteLoadStats)));
        }
        if (node1WriteLoadStats != null) {
            nodeUsageStats.put("node_1", new NodeUsageStatsForThreadPools("node_1", Map.of("write", node1WriteLoadStats)));
        }

        final ClusterState clusterState = createClusterState();
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(nodeUsageStats)
            .shardWriteLoads(
                clusterState.metadata()
                    .getProject(ProjectId.DEFAULT)
                    .stream()
                    .filter(index -> indicesWithNoWriteLoad.contains(index.getIndex().getName()) == false)
                    .flatMap(index -> IntStream.range(0, 3).mapToObj(shardNum -> new ShardId(index.getIndex(), shardNum)))
                    .collect(Collectors.toUnmodifiableMap(shardId -> shardId, shardId -> randomDoubleBetween(0.1, 5.0, true)))
            )
            .build();

        return new RoutingAllocation(
            new AllocationDeciders(List.of()),
            clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        ).mutableCloneForSimulation();
    }

    private ClusterState createClusterState() {
        return ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(INDICES, 3, 0);
    }
}
