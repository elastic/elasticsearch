/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShardMoveNodeCacheCommitmentSimulatorTests extends ESTestCase {

    /**
     * Unassigned shard could mean a new shard not assigned to a node, or an existing shard that is unassigned for whatever reason.
     */
    public void testUnassignedShardOnlyAddsToTargetNode() {
        var nodeId = "node-0";
        var shard = createUnassignedShard(nodeId);

        final long cacheSize = randomLongBetween(100, 1000);
        final long initialBoostedCommitment = randomLongBetween(0, 300);
        final long initialUnboostedCommitment = randomLongBetween(0, 300);
        final long boostedRequirement = randomLongBetween(1, 100);
        final long unboostedRequirement = randomLongBetween(1, 50);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(nodeId, new NodeCacheSizeAndCommitments(cacheSize, initialBoostedCommitment, initialUnboostedCommitment))
            )
            .shardCacheRequirements(
                Map.of(shard.shardId(), new BoostedAndUnboostedCacheRequirements(boostedRequirement, unboostedRequirement))
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getSimulatedNodeCacheSizeAndCommitments().get(nodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    initialBoostedCommitment + boostedRequirement,
                    initialUnboostedCommitment + unboostedRequirement
                )
            )
        );
    }

    public void testRelocatingShardMovesCommitmentFromSourceToTarget() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        final long cacheSize = randomLongBetween(100, 1000);
        final long fromNodeInitialBoostedCommitment = randomLongBetween(50, 300);
        final long fromNodeInitialUnboostedCommitment = randomLongBetween(50, 300);
        final long toNodeInitialBoostedCommitment = randomLongBetween(0, 100);
        final long toNodeInitialUnboostedCommitment = randomLongBetween(0, 100);
        final long boostedRequirement = randomLongBetween(1, 50);
        final long unboostedRequirement = randomLongBetween(1, 50);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    fromNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, fromNodeInitialBoostedCommitment, fromNodeInitialUnboostedCommitment),
                    toNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, toNodeInitialBoostedCommitment, toNodeInitialUnboostedCommitment)
                )
            )
            .shardCacheRequirements(
                Map.of(shard.shardId(), new BoostedAndUnboostedCacheRequirements(boostedRequirement, unboostedRequirement))
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        simulator.simulateShardStarted(shard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        assertThat(
            updatedCommitments.get(fromNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    fromNodeInitialBoostedCommitment - boostedRequirement,
                    fromNodeInitialUnboostedCommitment - unboostedRequirement
                )
            )
        );
        assertThat(
            updatedCommitments.get(toNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    toNodeInitialBoostedCommitment + boostedRequirement,
                    toNodeInitialUnboostedCommitment + unboostedRequirement
                )
            )
        );
    }

    public void testUnaffectedWhenShardHasNoRequirement() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        final long cacheSize = randomLongBetween(100, 1000);
        final long fromNodeBoostedCommitment = randomLongBetween(0, 300);
        final long fromNodeUnboostedCommitment = randomLongBetween(0, 300);
        final long toNodeBoostedCommitment = randomLongBetween(0, 300);
        final long toNodeUnboostedCommitment = randomLongBetween(0, 300);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    fromNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, fromNodeBoostedCommitment, fromNodeUnboostedCommitment),
                    toNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, toNodeBoostedCommitment, toNodeUnboostedCommitment)
                )
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        simulator.simulateShardStarted(shard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        assertThat(
            updatedCommitments.get(fromNodeId),
            equalTo(new NodeCacheSizeAndCommitments(cacheSize, fromNodeBoostedCommitment, fromNodeUnboostedCommitment))
        );
        assertThat(
            updatedCommitments.get(toNodeId),
            equalTo(new NodeCacheSizeAndCommitments(cacheSize, toNodeBoostedCommitment, toNodeUnboostedCommitment))
        );
    }

    public void testUnaffectedWhenNodeHasNoTrackedCommitment() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        var clusterInfo = ClusterInfo.builder()
            .shardCacheRequirements(
                Map.of(shard.shardId(), new BoostedAndUnboostedCacheRequirements(randomLongBetween(1, 100), randomLongBetween(1, 50)))
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        simulator.simulateShardStarted(shard);

        assertThat(simulator.getSimulatedNodeCacheSizeAndCommitments(), equalTo(Map.of()));
    }

    /**
     * A shard's requirement can be real for boosted but the sentinel
     * {@link BoostedAndUnboostedCacheRequirements#NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT} for unboosted. Only the boosted
     * component should move.
     */
    public void testHandlesSentinelUnboostedRequirement() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        final long cacheSize = randomLongBetween(100, 1000);
        final long fromNodeInitialBoostedCommitment = randomLongBetween(50, 300);
        final long fromNodeInitialUnboostedCommitment = randomLongBetween(0, 300);
        final long toNodeInitialBoostedCommitment = randomLongBetween(0, 100);
        final long toNodeInitialUnboostedCommitment = randomLongBetween(0, 100);
        final long boostedRequirement = randomLongBetween(1, 50);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    fromNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, fromNodeInitialBoostedCommitment, fromNodeInitialUnboostedCommitment),
                    toNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, toNodeInitialBoostedCommitment, toNodeInitialUnboostedCommitment)
                )
            )
            .shardCacheRequirements(
                Map.of(
                    shard.shardId(),
                    new BoostedAndUnboostedCacheRequirements(boostedRequirement, NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT)
                )
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        simulator.simulateShardStarted(shard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        assertThat(
            updatedCommitments.get(fromNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    fromNodeInitialBoostedCommitment - boostedRequirement,
                    fromNodeInitialUnboostedCommitment
                )
            )
        );
        assertThat(
            updatedCommitments.get(toNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    toNodeInitialBoostedCommitment + boostedRequirement,
                    toNodeInitialUnboostedCommitment
                )
            )
        );
    }

    /**
     * Symmetric to {@link #testHandlesSentinelUnboostedRequirement}: a shard's requirement can be real for unboosted but the
     * sentinel for boosted. Only the unboosted component should move.
     */
    public void testHandlesSentinelBoostedRequirement() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        final long cacheSize = randomLongBetween(100, 1000);
        final long fromNodeInitialBoostedCommitment = randomLongBetween(0, 300);
        final long fromNodeInitialUnboostedCommitment = randomLongBetween(50, 300);
        final long toNodeInitialBoostedCommitment = randomLongBetween(0, 100);
        final long toNodeInitialUnboostedCommitment = randomLongBetween(0, 100);
        final long unboostedRequirement = randomLongBetween(1, 50);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    fromNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, fromNodeInitialBoostedCommitment, fromNodeInitialUnboostedCommitment),
                    toNodeId,
                    new NodeCacheSizeAndCommitments(cacheSize, toNodeInitialBoostedCommitment, toNodeInitialUnboostedCommitment)
                )
            )
            .shardCacheRequirements(
                Map.of(
                    shard.shardId(),
                    new BoostedAndUnboostedCacheRequirements(NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT, unboostedRequirement)
                )
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        simulator.simulateShardStarted(shard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        assertThat(
            updatedCommitments.get(fromNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    fromNodeInitialBoostedCommitment,
                    fromNodeInitialUnboostedCommitment - unboostedRequirement
                )
            )
        );
        assertThat(
            updatedCommitments.get(toNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    toNodeInitialBoostedCommitment,
                    toNodeInitialUnboostedCommitment + unboostedRequirement
                )
            )
        );
    }

    /**
     * The desired balance computer now always simulates relocating shards as started before deducting their commitment
     * from the source (#154504), so a well-formed {@link ClusterInfo} snapshot should never produce a negative commitment.
     * If it would, that signals a real bug rather than an expected stale-data scenario, so the simulator relies on an
     * explicit {@code >= 0} assertion to catch it, instead of silently clamping. These two tests cover the boosted and
     * unboosted components independently, since only the first failing assertion in the method actually throws.
     */
    public void testThrowsWhenBoostedCommitmentWouldGoNegative() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        final long fromNodeBoostedCommitment = randomLongBetween(1, 10);
        final long boostedRequirement = randomLongBetween(fromNodeBoostedCommitment + 1, fromNodeBoostedCommitment + 100);
        final long fromNodeUnboostedCommitment = randomLongBetween(50, 100);
        final long unboostedRequirement = randomLongBetween(1, fromNodeUnboostedCommitment);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    fromNodeId,
                    new NodeCacheSizeAndCommitments(500, fromNodeBoostedCommitment, fromNodeUnboostedCommitment),
                    toNodeId,
                    new NodeCacheSizeAndCommitments(500, 50, 20)
                )
            )
            .shardCacheRequirements(
                Map.of(shard.shardId(), new BoostedAndUnboostedCacheRequirements(boostedRequirement, unboostedRequirement))
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        var error = expectThrows(AssertionError.class, () -> simulator.simulateShardStarted(shard));
        assertThat(error.getMessage(), containsString("boosted cache commitment for node [" + fromNodeId + "] went negative"));
    }

    public void testThrowsWhenUnboostedCommitmentWouldGoNegative() {
        var fromNodeId = "node-0";
        var toNodeId = "node-1";
        var shard = relocatingShard(fromNodeId, toNodeId);

        final long fromNodeBoostedCommitment = randomLongBetween(50, 100);
        final long boostedRequirement = randomLongBetween(1, fromNodeBoostedCommitment);
        final long fromNodeUnboostedCommitment = randomLongBetween(1, 10);
        final long unboostedRequirement = randomLongBetween(fromNodeUnboostedCommitment + 1, fromNodeUnboostedCommitment + 100);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    fromNodeId,
                    new NodeCacheSizeAndCommitments(500, fromNodeBoostedCommitment, fromNodeUnboostedCommitment),
                    toNodeId,
                    new NodeCacheSizeAndCommitments(500, 50, 20)
                )
            )
            .shardCacheRequirements(
                Map.of(shard.shardId(), new BoostedAndUnboostedCacheRequirements(boostedRequirement, unboostedRequirement))
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        var error = expectThrows(AssertionError.class, () -> simulator.simulateShardStarted(shard));
        assertThat(error.getMessage(), containsString("unboosted cache commitment for node [" + fromNodeId + "] went negative"));
    }

    private static ShardRouting createUnassignedShard(String nodeId) {
        return shardRoutingBuilder(new ShardId("my-index", "_na_", 0), nodeId, true, INITIALIZING).withRecoverySource(
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        ).build();
    }

    private static ShardRouting relocatingShard(String fromNodeId, String toNodeId) {
        return shardRoutingBuilder(new ShardId("my-index", "_na_", 0), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();
    }
}
