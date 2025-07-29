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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ShardMovementWriteLoadSimulatorTests extends ESTestCase {

    private static final RoutingChangesObserver NOOP = new RoutingChangesObserver() {
    };
    private static final String[] INDICES = { "indexOne", "indexTwo", "indexThree" };

    /**
     * We should not adjust the values if there's no movement
     */
    public void testNoShardMovement() {
        final var originalNode0ThreadPoolStats = randomThreadPoolUsageStats();
        final var originalNode1ThreadPoolStats = randomThreadPoolUsageStats();
        final var allocation = createRoutingAllocationWithRandomisedWriteLoads(
            originalNode0ThreadPoolStats,
            originalNode1ThreadPoolStats,
            Set.of()
        );

        final var shardMovementWriteLoadSimulator = new ShardMovementWriteLoadSimulator(allocation);
        final var calculatedNodeUsageStates = shardMovementWriteLoadSimulator.simulatedNodeUsageStatsForThreadPools();
        assertThat(calculatedNodeUsageStates, Matchers.aMapWithSize(2));
        assertThat(
            calculatedNodeUsageStates.get("node_0").threadPoolUsageStatsMap().get("write"),
            sameInstance(originalNode0ThreadPoolStats)
        );
        assertThat(
            calculatedNodeUsageStates.get("node_1").threadPoolUsageStatsMap().get("write"),
            sameInstance(originalNode1ThreadPoolStats)
        );
    }

    public void testMovementOfAShardWillMoveThreadPoolUtilisation() {
        final var originalNode0ThreadPoolStats = randomThreadPoolUsageStats();
        final var originalNode1ThreadPoolStats = randomThreadPoolUsageStats();
        final var allocation = createRoutingAllocationWithRandomisedWriteLoads(
            originalNode0ThreadPoolStats,
            originalNode1ThreadPoolStats,
            Set.of()
        );
        final var shardMovementWriteLoadSimulator = new ShardMovementWriteLoadSimulator(allocation);

        // Relocate a random shard from node_0 to node_1
        final var randomShard = randomFrom(StreamSupport.stream(allocation.routingNodes().node("node_0").spliterator(), false).toList());
        final var expectedShardSize = randomNonNegativeLong();
        final var moveShardTuple = allocation.routingNodes().relocateShard(randomShard, "node_1", expectedShardSize, "testing", NOOP);
        shardMovementWriteLoadSimulator.simulateShardStarted(moveShardTuple.v2());
        final ShardRouting movedAndStartedShard = allocation.routingNodes().startShard(moveShardTuple.v2(), NOOP, expectedShardSize);

        final var calculatedNodeUsageStats = shardMovementWriteLoadSimulator.simulatedNodeUsageStatsForThreadPools();
        assertThat(calculatedNodeUsageStats, Matchers.aMapWithSize(2));

        final var shardWriteLoad = allocation.clusterInfo().getShardWriteLoads().get(randomShard.shardId());
        final var expectedUtilisationReductionAtSource = shardWriteLoad / originalNode0ThreadPoolStats.totalThreadPoolThreads();
        final var expectedUtilisationIncreaseAtDestination = shardWriteLoad / originalNode1ThreadPoolStats.totalThreadPoolThreads();

        // Some node_0 utilization should have been moved to node_1
        if (expectedUtilisationReductionAtSource > originalNode0ThreadPoolStats.averageThreadPoolUtilization()) {
            // We don't return utilization less than zero because that makes no sense
            assertThat(getAverageWritePoolUtilization(shardMovementWriteLoadSimulator, "node_0"), equalTo(0.0f));
        } else {
            assertThat(
                (double) originalNode0ThreadPoolStats.averageThreadPoolUtilization() - getAverageWritePoolUtilization(
                    shardMovementWriteLoadSimulator,
                    "node_0"
                ),
                closeTo(expectedUtilisationReductionAtSource, 0.001f)
            );
        }
        assertThat(
            (double) getAverageWritePoolUtilization(shardMovementWriteLoadSimulator, "node_1") - originalNode1ThreadPoolStats
                .averageThreadPoolUtilization(),
            closeTo(expectedUtilisationIncreaseAtDestination, 0.001f)
        );

        // Then move it back
        final var moveBackTuple = allocation.routingNodes()
            .relocateShard(movedAndStartedShard, "node_0", expectedShardSize, "testing", NOOP);
        shardMovementWriteLoadSimulator.simulateShardStarted(moveBackTuple.v2());

        // The utilization numbers should return to their original values
        assertThat(
            getAverageWritePoolUtilization(shardMovementWriteLoadSimulator, "node_0"),
            equalTo(originalNode0ThreadPoolStats.averageThreadPoolUtilization())
        );
        assertThat(
            getAverageWritePoolUtilization(shardMovementWriteLoadSimulator, "node_1"),
            equalTo(originalNode1ThreadPoolStats.averageThreadPoolUtilization())
        );
    }

    public void testMovementBetweenNodesWithNoThreadPoolAndWriteLoadStats() {
        final var originalNode0ThreadPoolStats = randomBoolean() ? randomThreadPoolUsageStats() : null;
        final var originalNode1ThreadPoolStats = randomBoolean() ? randomThreadPoolUsageStats() : null;
        final var allocation = createRoutingAllocationWithRandomisedWriteLoads(
            originalNode0ThreadPoolStats,
            originalNode1ThreadPoolStats,
            new HashSet<>(randomSubsetOf(Arrays.asList(INDICES)))
        );
        final var shardMovementWriteLoadSimulator = new ShardMovementWriteLoadSimulator(allocation);

        // Relocate a random shard from node_0 to node_1
        final var expectedShardSize = randomNonNegativeLong();
        final var randomShard = randomFrom(StreamSupport.stream(allocation.routingNodes().node("node_0").spliterator(), false).toList());
        final var moveShardTuple = allocation.routingNodes().relocateShard(randomShard, "node_1", expectedShardSize, "testing", NOOP);
        shardMovementWriteLoadSimulator.simulateShardStarted(moveShardTuple.v2());
        allocation.routingNodes().startShard(moveShardTuple.v2(), NOOP, expectedShardSize);

        final var simulated = shardMovementWriteLoadSimulator.simulatedNodeUsageStatsForThreadPools();
        assertThat(simulated.containsKey("node_0"), equalTo(originalNode0ThreadPoolStats != null));
        assertThat(simulated.containsKey("node_1"), equalTo(originalNode1ThreadPoolStats != null));
    }

    private float getAverageWritePoolUtilization(ShardMovementWriteLoadSimulator shardMovementWriteLoadSimulator, String nodeId) {
        final var generatedNodeUsageStates = shardMovementWriteLoadSimulator.simulatedNodeUsageStatsForThreadPools();
        final var node0WritePoolStats = generatedNodeUsageStates.get(nodeId).threadPoolUsageStatsMap().get("write");
        return node0WritePoolStats.averageThreadPoolUtilization();
    }

    private NodeUsageStatsForThreadPools.ThreadPoolUsageStats randomThreadPoolUsageStats() {
        return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            randomIntBetween(4, 16),
            randomBoolean() ? 0.0f : randomFloatBetween(0.1f, 1.0f, true),
            randomLongBetween(0, 60_000)
        );
    }

    private RoutingAllocation createRoutingAllocationWithRandomisedWriteLoads(
        NodeUsageStatsForThreadPools.ThreadPoolUsageStats node0ThreadPoolStats,
        NodeUsageStatsForThreadPools.ThreadPoolUsageStats node1ThreadPoolStats,
        Set<String> indicesWithNoWriteLoad
    ) {
        final Map<String, NodeUsageStatsForThreadPools> nodeUsageStats = new HashMap<>();
        if (node0ThreadPoolStats != null) {
            nodeUsageStats.put("node_0", new NodeUsageStatsForThreadPools("node_0", Map.of("write", node0ThreadPoolStats)));
        }
        if (node1ThreadPoolStats != null) {
            nodeUsageStats.put("node_1", new NodeUsageStatsForThreadPools("node_1", Map.of("write", node1ThreadPoolStats)));
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
                    .collect(
                        Collectors.toUnmodifiableMap(
                            shardId -> shardId,
                            shardId -> randomBoolean() ? 0.0f : randomDoubleBetween(0.1, 5.0, true)
                        )
                    )
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
