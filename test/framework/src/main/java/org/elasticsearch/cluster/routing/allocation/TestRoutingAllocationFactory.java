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

import static org.elasticsearch.test.ESTestCase.randomBoolean;

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
            AllocationDeciders.EMPTY,
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
     * Build a {@link RoutingAllocation} for the given cluster state.
     */
    public static Builder forClusterState(ClusterState clusterState) {
        return new Builder(clusterState);
    }

    public static final class Builder {
        private final ClusterState clusterState;
        private AllocationDeciders allocationDeciders = AllocationDeciders.EMPTY;
        private RoutingNodes routingNodes;
        private ClusterInfo clusterInfo = ClusterInfo.EMPTY;
        private SnapshotShardSizeInfo shardSizeInfo = SnapshotShardSizeInfo.EMPTY;
        private long currentNanoTime = System.nanoTime();

        private Builder(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        public Builder allocationDeciders(AllocationDeciders allocationDeciders) {
            this.allocationDeciders = allocationDeciders;
            return this;
        }

        public Builder allocationDeciders(AllocationDecider... allocationDeciders) {
            this.allocationDeciders = new AllocationDeciders(Arrays.asList(allocationDeciders));
            return this;
        }

        public Builder clusterInfo(ClusterInfo clusterInfo) {
            this.clusterInfo = clusterInfo;
            return this;
        }

        public Builder shardSizeInfo(SnapshotShardSizeInfo shardSizeInfo) {
            this.shardSizeInfo = shardSizeInfo;
            return this;
        }

        public Builder routingNodes(RoutingNodes routingNodes) {
            this.routingNodes = routingNodes;
            return this;
        }

        public Builder currentNanoTime(long currentNanoTime) {
            this.currentNanoTime = currentNanoTime;
            return this;
        }

        public RoutingAllocation build() {
            return randomBoolean() ? mutable() : immutable();
        }

        public RoutingAllocation immutable() {
            return new ImmutableRoutingAllocation(allocationDeciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime);
        }

        public MutableRoutingAllocation mutable() {
            return new MutableRoutingAllocation(
                allocationDeciders,
                routingNodes,
                clusterState,
                clusterInfo,
                shardSizeInfo,
                currentNanoTime,
                false
            );
        }
    }
}
