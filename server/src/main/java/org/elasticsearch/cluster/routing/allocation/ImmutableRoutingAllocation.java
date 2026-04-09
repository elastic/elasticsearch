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
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.core.Releasable;
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
        super(deciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime);
    }

    @Override
    public void ignoreDisable(boolean ignoreDisable) {
        assert false : "This should never be called on an immutable routing allocation";
    }

    @Override
    public boolean ignoreDisable() {
        return false;
    }

    @Override
    public void removeAllocationId(ShardRouting shardRouting) {
        assert false : "This should never be called on an immutable routing allocation";
    }

    /**
     * Returns {@link RoutingChangesObserver#NOOP} since this is an immutable {@link RoutingAllocation}
     * and there should be no routing changes to observe
     */
    @Override
    public RoutingChangesObserver changes() {
        return RoutingChangesObserver.NOOP;
    }

    @Override
    public Metadata updateMetadataWithRoutingChanges(GlobalRoutingTable newRoutingTable) {
        assert false : "This should never be called on an immutable routing allocation";
        return metadata();
    }

    @Override
    public RestoreInProgress updateRestoreInfoWithRoutingChanges(RestoreInProgress restoreInProgress) {
        assert false : "This should never be called on an immutable routing allocation";
        return restoreInProgress;
    }

    @Override
    public boolean routingNodesChanged() {
        return false;
    }

    @Override
    public boolean hasPendingAsyncFetch() {
        return false;
    }

    @Override
    public void setHasPendingAsyncFetch() {
        assert false : "This should never be called on an immutable routing allocation";
    }

    @Override
    public boolean isSimulating() {
        return false;
    }

    @Override
    public boolean isReconciling() {
        return false;
    }

    @Override
    public Releasable withReconcilingFlag() {
        assert false : "This should never be called on an immutable routing allocation";
        return () -> {};
    }

    @Override
    public void setSimulatedClusterInfo(ClusterInfo clusterInfo) {
        assert false : "This should never be called on an immutable routing allocation";
    }
}
