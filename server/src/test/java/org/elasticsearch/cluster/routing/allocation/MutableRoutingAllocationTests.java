/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MutableRoutingAllocationTests extends ESAllocationTestCase {

    public void testCacheInvalidatedOnShardStarted() {
        String sourceNodeId = randomIdentifier();
        String otherNodeId = randomValueOtherThan(sourceNodeId, ESTestCase::randomIdentifier);
        double initialSourceValue = randomWriteLoadProportion();
        double otherValue = randomWriteLoadProportion();
        double recomputedValue = randomValueOtherThan(initialSourceValue, MutableRoutingAllocationTests::randomWriteLoadProportion);

        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        MutableRoutingAllocation allocation = newAllocation(clusterInfo, sourceNodeId, otherNodeId);

        clusterInfo.nodeMaxShardWriteLoadProportion(sourceNodeId, () -> initialSourceValue);
        clusterInfo.nodeMaxShardWriteLoadProportion(otherNodeId, () -> otherValue);

        ShardRouting initializing = TestShardRouting.newShardRouting(
            randomIdentifier(),
            0,
            sourceNodeId,
            true,
            ShardRoutingState.INITIALIZING
        );
        ShardRouting started = initializing.moveToStarted(0L);

        allocation.changes().shardStarted(initializing, started);

        // The entry for the started shard's node has been invalidated, so we recompute and store the new value.
        // If the entry had not been invalidated, the stale cached value would mismatch the supplier and trip an assertion.
        assertThat(clusterInfo.nodeMaxShardWriteLoadProportion(sourceNodeId, () -> recomputedValue), equalTo(recomputedValue));
        // Other nodes' cache entries are untouched.
        assertThat(clusterInfo.nodeMaxShardWriteLoadProportion(otherNodeId, () -> otherValue), equalTo(otherValue));
    }

    public void testCacheInvalidatedOnRelocationStarted() {
        String sourceNodeId = randomIdentifier();
        String targetNodeId = randomValueOtherThan(sourceNodeId, ESTestCase::randomIdentifier);
        double initialSourceValue = randomWriteLoadProportion();
        double targetValue = randomWriteLoadProportion();
        double recomputedValue = randomValueOtherThan(initialSourceValue, MutableRoutingAllocationTests::randomWriteLoadProportion);

        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        MutableRoutingAllocation allocation = newAllocation(clusterInfo, sourceNodeId, targetNodeId);

        clusterInfo.nodeMaxShardWriteLoadProportion(sourceNodeId, () -> initialSourceValue);
        clusterInfo.nodeMaxShardWriteLoadProportion(targetNodeId, () -> targetValue);

        ShardRouting started = TestShardRouting.newShardRouting(randomAlphaOfLength(8), 0, sourceNodeId, true, ShardRoutingState.STARTED);
        ShardRouting source = started.relocate(targetNodeId, 0L);
        ShardRouting target = source.getTargetRelocatingShard();

        allocation.changes().relocationStarted(started, target, randomAlphaOfLength(10));

        // Source node entry invalidated; target node entry untouched (only an INITIALIZING shard appears there,
        // and the cache only depends on STARTED shards).
        assertThat(clusterInfo.nodeMaxShardWriteLoadProportion(sourceNodeId, () -> recomputedValue), equalTo(recomputedValue));
        assertThat(clusterInfo.nodeMaxShardWriteLoadProportion(targetNodeId, () -> targetValue), equalTo(targetValue));
    }

    private static MutableRoutingAllocation newAllocation(ClusterInfo clusterInfo, String sourceNodeId, String targetNodeId) {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode(sourceNodeId)).add(newNode(targetNodeId)))
            .build();
        RoutingAllocation mutable = TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).mutable();
        return (MutableRoutingAllocation) (randomBoolean() ? mutable.mutableCloneForSimulation() : mutable);
    }

    private static double randomWriteLoadProportion() {
        return randomDoubleBetween(0.0, 1.0, true);
    }
}
