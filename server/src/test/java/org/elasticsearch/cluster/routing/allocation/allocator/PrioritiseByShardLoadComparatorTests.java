/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.routing.allocation.allocator.PrioritiseByShardLoadComparator.THRESHOLD_RATIO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class PrioritiseByShardLoadComparatorTests extends ESAllocationTestCase {

    /**
     * Test for {@link PrioritiseByShardLoadComparator}
     */
    public void testPrioritiseByShardLoadComparator() {
        final double maxWriteLoad = randomDoubleBetween(0.0, 100.0, true);
        final double writeLoadThreshold = maxWriteLoad * THRESHOLD_RATIO;
        final int numberOfShardsWithMaxWriteLoad = between(1, 5);
        final int numberOfShardsWithWriteLoadBetweenThresholdAndMax = between(0, 50);
        final int numberOfShardsWithWriteLoadBelowThreshold = between(0, 50);
        final int numberOfShardsWithNoWriteLoad = between(0, 50);
        final int totalShards = numberOfShardsWithMaxWriteLoad + numberOfShardsWithWriteLoadBetweenThresholdAndMax
            + numberOfShardsWithWriteLoadBelowThreshold + numberOfShardsWithNoWriteLoad;

        // We create single-shard indices for simplicity's sake and to make it clear the shards are independent of each other
        final var indices = new ArrayList<IndexMetadata.Builder>();
        for (int i = 0; i < totalShards; i++) {
            indices.add(anIndex("index-" + i).numberOfShards(1).numberOfReplicas(0));
        }

        final var nodeId = randomIdentifier();
        final var clusterState = createStateWithIndices(List.of(nodeId), shardId -> nodeId, indices.toArray(IndexMetadata.Builder[]::new));

        final var allShards = clusterState.routingTable(ProjectId.DEFAULT).allShards().collect(toSet());
        final var shardWriteLoads = new HashMap<ShardId, Double>();
        addRandomWriteLoadAndRemoveShard(shardWriteLoads, allShards, numberOfShardsWithMaxWriteLoad, () -> maxWriteLoad);
        addRandomWriteLoadAndRemoveShard(
            shardWriteLoads,
            allShards,
            numberOfShardsWithWriteLoadBetweenThresholdAndMax,
            () -> randomDoubleBetween(writeLoadThreshold, maxWriteLoad, true)
        );
        addRandomWriteLoadAndRemoveShard(
            shardWriteLoads,
            allShards,
            numberOfShardsWithWriteLoadBelowThreshold,
            () -> randomDoubleBetween(0, writeLoadThreshold, true)
        );
        assertThat(allShards, hasSize(numberOfShardsWithNoWriteLoad));

        final ClusterInfo clusterInfo = ClusterInfo.builder().shardWriteLoads(shardWriteLoads).build();

        // Assign all shards to node
        final var allocatedRoutingNodes = clusterState.getRoutingNodes().mutableCopy();
        for (ShardRouting shardRouting : allocatedRoutingNodes.unassigned()) {
            allocatedRoutingNodes.initializeShard(shardRouting, nodeId, null, randomNonNegativeLong(), RoutingChangesObserver.NOOP);
        }

        final var comparator = new PrioritiseByShardLoadComparator(clusterInfo, allocatedRoutingNodes.node(nodeId));

        logger.info("--> testing shard movement priority comparator, maxValue={}, threshold={}", maxWriteLoad, writeLoadThreshold);
        var sortedShards = allocatedRoutingNodes.getAssignedShards().values().stream().flatMap(List::stream).sorted(comparator).toList();

        for (ShardRouting shardRouting : sortedShards) {
            logger.info("--> {}: {}", shardRouting.shardId(), shardWriteLoads.getOrDefault(shardRouting.shardId(), -1.0));
        }

        double lastWriteLoad = 0.0;
        int currentIndex = 0;

        logger.info("--> expecting {} between threshold and max in ascending order", numberOfShardsWithWriteLoadBetweenThresholdAndMax);
        for (int i = 0; i < numberOfShardsWithWriteLoadBetweenThresholdAndMax; i++) {
            final var currentShardId = sortedShards.get(currentIndex++).shardId();
            assertThat(shardWriteLoads, hasKey(currentShardId));
            final double currentWriteLoad = shardWriteLoads.get(currentShardId);
            if (i == 0) {
                lastWriteLoad = currentWriteLoad;
            } else {
                assertThat(currentWriteLoad, greaterThanOrEqualTo(lastWriteLoad));
            }
        }
        logger.info("--> expecting {} below threshold in descending order", numberOfShardsWithWriteLoadBelowThreshold);
        for (int i = 0; i < numberOfShardsWithWriteLoadBelowThreshold; i++) {
            final var currentShardId = sortedShards.get(currentIndex++).shardId();
            assertThat(shardWriteLoads, hasKey(currentShardId));
            final double currentWriteLoad = shardWriteLoads.get(currentShardId);
            if (i == 0) {
                lastWriteLoad = currentWriteLoad;
            } else {
                assertThat(currentWriteLoad, lessThanOrEqualTo(lastWriteLoad));
            }
        }
        logger.info("--> expecting {} at max", numberOfShardsWithMaxWriteLoad);
        for (int i = 0; i < numberOfShardsWithMaxWriteLoad; i++) {
            final var currentShardId = sortedShards.get(currentIndex++).shardId();
            assertThat(shardWriteLoads, hasKey(currentShardId));
            final double currentWriteLoad = shardWriteLoads.get(currentShardId);
            assertThat(currentWriteLoad, equalTo(maxWriteLoad));
        }
        logger.info("--> expecting {} missing", numberOfShardsWithNoWriteLoad);
        for (int i = 0; i < numberOfShardsWithNoWriteLoad; i++) {
            final var currentShardId = sortedShards.get(currentIndex++);
            assertThat(shardWriteLoads, not(hasKey(currentShardId.shardId())));
        }
    }

    public void testCompareReturnsZeroWhenEveryLoadIsMissing() {
        final RoutingNode node = startedShardsOnSingleNode(2);
        final var shards = shardsOn(node);
        final var comparator = writeLoadComparator(node, Map.of());

        assertComparesEqual(comparator, shards.get(0), shards.get(1));
    }

    public void testCompareReturnsZeroWhenBothShardsHaveNoLoad() {
        final RoutingNode node = startedShardsOnSingleNode(3);
        final var shards = shardsOn(node);
        final var comparator = writeLoadComparator(node, Map.of(shards.get(2).shardId(), 10.0));

        assertComparesEqual(comparator, shards.get(0), shards.get(1));
    }

    /**
     * Two shards share a load strictly inside one band. The other shard holds the node maximum,
     * so the pair is neither missing nor at the maximum.
     */
    public void testCompareReturnsZeroWhenLoadsAreEqualInsideABand() {
        final double maxLoad = 10.0;
        final double threshold = maxLoad * THRESHOLD_RATIO;
        // Upper band is [threshold, max). Lower band is [0, threshold).
        final double equalLoad = randomBoolean() ? (threshold + maxLoad) / 2 : threshold / 2;

        final RoutingNode node = startedShardsOnSingleNode(3);
        final var shards = shardsOn(node);
        final var comparator = writeLoadComparator(
            node,
            Map.of(shards.get(0).shardId(), equalLoad, shards.get(1).shardId(), equalLoad, shards.get(2).shardId(), maxLoad)
        );

        assertComparesEqual(comparator, shards.get(0), shards.get(1));
    }

    /**
     * Every compared shard is at the node maximum, so neither is preferred.
     */
    public void testCompareReturnsZeroWhenBothShardsHaveTheNodeMaximum() {
        final double maxLoad = randomDoubleBetween(1.0, 100.0, true);
        final RoutingNode node = startedShardsOnSingleNode(2);
        final var shards = shardsOn(node);
        final var comparator = writeLoadComparator(node, Map.of(shards.get(0).shardId(), maxLoad, shards.get(1).shardId(), maxLoad));

        assertComparesEqual(comparator, shards.get(0), shards.get(1));
    }

    /**
     * Randomly select a shard and add a random write-load for it
     *
     * @param shardWriteLoads The map of shards to write-loads, this will be added to
     * @param shards The set of shards to select from, selected shards will be removed from this set
     * @param count The number of shards to generate write loads for
     * @param writeLoadSupplier The supplier of random write loads to use
     */
    private void addRandomWriteLoadAndRemoveShard(
        Map<ShardId, Double> shardWriteLoads,
        Set<ShardRouting> shards,
        int count,
        DoubleSupplier writeLoadSupplier
    ) {
        for (int i = 0; i < count; i++) {
            final var shardRouting = randomFrom(shards);
            shardWriteLoads.put(shardRouting.shardId(), writeLoadSupplier.getAsDouble());
            shards.remove(shardRouting);
        }
    }

    private static IndexMetadata.Builder anIndex(String name) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)).numberOfShards(1).numberOfReplicas(0);
    }

    private static void assertComparesEqual(PrioritiseByShardLoadComparator comparator, ShardRouting left, ShardRouting right) {
        assertThat(comparator.compare(left, right), equalTo(0));
        assertThat(comparator.compare(right, left), equalTo(0));
    }

    private static PrioritiseByShardLoadComparator writeLoadComparator(RoutingNode node, Map<ShardId, Double> shardWriteLoads) {
        return new PrioritiseByShardLoadComparator(ClusterInfo.builder().shardWriteLoads(shardWriteLoads).build(), node);
    }

    private RoutingNode startedShardsOnSingleNode(int shardCount) {
        final String nodeId = randomIdentifier();
        final var indices = new IndexMetadata.Builder[shardCount];
        for (int i = 0; i < shardCount; i++) {
            indices[i] = anIndex("index-" + i);
        }
        final ClusterState clusterState = createStateWithIndices(List.of(nodeId), shardId -> nodeId, true, indices);
        return clusterState.getRoutingNodes().node(nodeId);
    }

    private static List<ShardRouting> shardsOn(RoutingNode node) {
        final var shards = new ArrayList<ShardRouting>();
        for (ShardRouting shard : node) {
            shards.add(shard);
        }
        return shards;
    }
}
