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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Arrays;
import java.util.List;

public class TestRoutingAllocationFactory {

    /**
     * Create a mutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static MutableRoutingAllocation mutable(
        AllocationDeciders deciders,
        RoutingNodes routingNodes,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        return new MutableRoutingAllocation(deciders, routingNodes, clusterState, clusterInfo, shardSizeInfo, currentNanoTime, false);
    }

    /**
     * Create a mutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static MutableRoutingAllocation mutable(
        AllocationDeciders deciders,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        return new MutableRoutingAllocation(
            deciders,
            clusterState.mutableRoutingNodes(),
            clusterState,
            clusterInfo,
            shardSizeInfo,
            currentNanoTime,
            false
        );
    }

    /**
     * Create a mutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static MutableRoutingAllocation mutable(ClusterState clusterState) {
        return new MutableRoutingAllocation(
            new AllocationDeciders(List.of()),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime(),
            false
        );
    }

    /**
     * Create an immutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static RoutingAllocation immutable(ClusterState clusterState) {
        return new ImmutableRoutingAllocation(
            new AllocationDeciders(List.of()),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    /**
     * Create an immutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static RoutingAllocation immutable(AllocationDeciders allocationDeciders, ClusterState clusterState, ClusterInfo clusterInfo) {
        return new ImmutableRoutingAllocation(
            allocationDeciders,
            clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    /**
     * Create an immutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static RoutingAllocation immutable(
        AllocationDeciders allocationDeciders,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        return new ImmutableRoutingAllocation(allocationDeciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime);
    }

    /**
     * Create an immutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static RoutingAllocation immutable(ClusterState clusterState, AllocationDecider... allocationDeciders) {
        return new ImmutableRoutingAllocation(
            new AllocationDeciders(Arrays.asList(allocationDeciders)),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    /**
     * Create an immutable routing allocation
     * <p>
     * Only for use in tests
     */
    public static RoutingAllocation immutable(AllocationDeciders allocationDeciders, ClusterState clusterState) {
        return new ImmutableRoutingAllocation(
            allocationDeciders,
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }
}
