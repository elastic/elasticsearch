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
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import static org.elasticsearch.test.ESTestCase.randomBoolean;

/**
 * A Factory for creating {@link RoutingAllocation} instances for testing. {@link RoutingAllocation} instances are usually
 * created by the {@link AllocationService} only. This utility allows us to test the infrastructure that interfaces with them
 * without needing to create an {@link AllocationService} instance.
 */
public class TestRoutingAllocationFactory {

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
        private RoutingChangesObserver shardChangesObserver = RoutingChangesObserver.NOOP;

        private Builder(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        public Builder allocationDeciders(AllocationDeciders allocationDeciders) {
            this.allocationDeciders = allocationDeciders;
            return this;
        }

        public Builder allocationDeciders(AllocationDecider... allocationDeciders) {
            this.allocationDeciders = new AllocationDeciders(allocationDeciders);
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

        public Builder shardChangesObserver(RoutingChangesObserver shardChangesObserver) {
            this.shardChangesObserver = shardChangesObserver;
            return this;
        }

        /**
         * Build either an immutable or a mutable {@link RoutingAllocation} randomly
         */
        public RoutingAllocation build() {
            return randomBoolean() ? mutable() : immutable();
        }

        /**
         * Build an immutable {@link RoutingAllocation}
         */
        public RoutingAllocation immutable() {
            assert routingNodes == null : "Attempted to specify RoutingNodes for an immutable RoutingAllocation";
            return new ImmutableRoutingAllocation(allocationDeciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime);
        }

        /**
         * Build a mutable {@link RoutingAllocation}
         */
        public RoutingAllocation mutable() {
            return new MutableRoutingAllocation(
                allocationDeciders,
                routingNodes != null ? routingNodes : clusterState.mutableRoutingNodes(),
                clusterState,
                clusterInfo,
                shardSizeInfo,
                currentNanoTime,
                false,
                shardChangesObserver
            );
        }
    }
}
