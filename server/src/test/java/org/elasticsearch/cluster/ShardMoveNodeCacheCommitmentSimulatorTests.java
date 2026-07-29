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
     * {@link ClusterInfo} snapshots can contain contradictory shard placement information, so a movement can
     * make a node's cache commitment go negative even though the desired balance computer normally simulates
     * relocating shards as started before deducting their commitment from the source (#154504). Rather than
     * asserting this can't happen, the simulator clamps to 0. These two tests cover the boosted and unboosted
     * components independently.
     */
    public void testClampsWhenBoostedCommitmentWouldGoNegative() {
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
        simulator.simulateShardStarted(shard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        // boosted would go negative (fromNodeBoostedCommitment - boostedRequirement < 0) -> clamped to 0
        assertThat(updatedCommitments.get(fromNodeId).boostedCacheCommitmentInBytes(), equalTo(0L));
        // unboosted stays non-negative, so it's unaffected by the clamp
        assertThat(
            updatedCommitments.get(fromNodeId).unboostedCacheCommitmentInBytes(),
            equalTo(fromNodeUnboostedCommitment - unboostedRequirement)
        );
    }

    public void testClampsWhenUnboostedCommitmentWouldGoNegative() {
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
        simulator.simulateShardStarted(shard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        // boosted stays non-negative, so it's unaffected by the clamp
        assertThat(
            updatedCommitments.get(fromNodeId).boostedCacheCommitmentInBytes(),
            equalTo(fromNodeBoostedCommitment - boostedRequirement)
        );
        // unboosted would go negative (fromNodeUnboostedCommitment - unboostedRequirement < 0) -> clamped to 0
        assertThat(updatedCommitments.get(fromNodeId).unboostedCacheCommitmentInBytes(), equalTo(0L));
    }

    /**
     * A node touched by several shard moves within the same simulation should have its deltas accumulated and
     * clamped only once against the initial value, not clamped after each individual move. Clamping after each
     * move would lose information: e.g. -15 then +20 clamped step-by-step yields 20, but the correct net delta
     * (+5) clamped once yields the same result as if no intermediate dip had ever occurred.
     */
    public void testClampsOnceAfterAccumulatingMultipleMoves() {
        var nodeId = "node-0";
        var otherNodeId = "node-1";
        var initialBoostedCommitment = 10L;

        var awayShard = relocatingShard(nodeId, otherNodeId, 0);
        var firstIncomingShard = relocatingShard(otherNodeId, nodeId, 1);
        var secondIncomingShard = relocatingShard(otherNodeId, nodeId, 2);

        var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    nodeId,
                    new NodeCacheSizeAndCommitments(500, initialBoostedCommitment, 0),
                    otherNodeId,
                    new NodeCacheSizeAndCommitments(500, 50, 0)
                )
            )
            .shardCacheRequirements(
                Map.of(
                    awayShard.shardId(),
                    new BoostedAndUnboostedCacheRequirements(15, NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT),
                    firstIncomingShard.shardId(),
                    new BoostedAndUnboostedCacheRequirements(10, NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT),
                    secondIncomingShard.shardId(),
                    new BoostedAndUnboostedCacheRequirements(10, NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT)
                )
            )
            .build();

        var simulator = new ShardMoveNodeCacheCommitmentSimulator(clusterInfo);
        // -15, would dip nodeId's commitment to -5 if applied and clamped immediately
        simulator.simulateShardStarted(awayShard);
        // +10, +10: net delta across all three moves is -15+10+10=5
        simulator.simulateShardStarted(firstIncomingShard);
        simulator.simulateShardStarted(secondIncomingShard);

        var updatedCommitments = simulator.getSimulatedNodeCacheSizeAndCommitments();
        assertThat(updatedCommitments.get(nodeId).boostedCacheCommitmentInBytes(), equalTo(initialBoostedCommitment + 5));
    }

    private static ShardRouting createUnassignedShard(String nodeId) {
        return shardRoutingBuilder(new ShardId("my-index", "_na_", 0), nodeId, true, INITIALIZING).withRecoverySource(
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        ).build();
    }

    private static ShardRouting relocatingShard(String fromNodeId, String toNodeId) {
        return relocatingShard(fromNodeId, toNodeId, 0);
    }

    private static ShardRouting relocatingShard(String fromNodeId, String toNodeId, int shardNum) {
        return shardRoutingBuilder(new ShardId("my-index", "_na_", shardNum), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();
    }
}
