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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

final class ImmutableRoutingAllocation extends RoutingAllocation {

    /**
     * Creates a new {@link ImmutableRoutingAllocation}
     * @param deciders {@link AllocationDeciders} to used to make decisions for routing allocations
     * @param clusterState cluster state before rerouting
     * @param clusterInfo {@link ClusterInfo} to use for allocation decisions
     * @param currentNanoTime the nano time to use for all delay allocation calculation (typically {@link System#nanoTime()})
     */
    ImmutableRoutingAllocation(
        AllocationDeciders deciders,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        super(deciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime, false);
    }

    @Override
    public void removeAllocationId(ShardRouting shardRouting) {
        assert false : "Should not be called";
    }

    @Override
    public RoutingChangesObserver changes() {
        return RoutingChangesObserver.NOOP;
    }

    @Override
    public boolean routingNodesChanged() {
        return false;
    }

    @Override
    public void setSimulatedClusterInfo(ClusterInfo clusterInfo) {
        assert false : "Should not be called";
    }

    @Override
    public RoutingAllocation immutableClone() {
        return this;
    }
}
