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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

final class MutableRoutingAllocation extends RoutingAllocation {

    private final IndexMetadataUpdater indexMetadataUpdater = new IndexMetadataUpdater();
    private final RoutingNodesChangedObserver nodesChangedObserver = new RoutingNodesChangedObserver();
    private final RestoreService.RestoreInProgressUpdater restoreInProgressUpdater = new RestoreService.RestoreInProgressUpdater();
    private final ResizeSourceIndexSettingsUpdater resizeSourceIndexUpdater = new ResizeSourceIndexSettingsUpdater();

    private final RoutingChangesObserver routingChangesObserver;
    private final RoutingNodes routingNodes;
    private final boolean isSimulating;
    private boolean isReconciling;
    private boolean ignoreDisable;

    /**
     * Creates a new {@link RoutingAllocation}
     * @param deciders {@link AllocationDeciders} to used to make decisions for routing allocations
     * @param routingNodes Routing nodes in the current cluster or {@code null} if using those in the given cluster state
     * @param clusterState cluster state before rerouting
     * @param clusterInfo {@link ClusterInfo} to use for allocation decisions
     * @param currentNanoTime the nano time to use for all delay allocation calculation (typically {@link System#nanoTime()})
     * @param isSimulating {@code true} if "transient" deciders should be ignored because we are simulating the final allocation
     */
    MutableRoutingAllocation(
        AllocationDeciders deciders,
        RoutingNodes routingNodes,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime,
        boolean isSimulating,
        RoutingChangesObserver shardChangesObserver
    ) {
        super(deciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime);
        if (routingNodes == null || routingNodes.isReadOnly()) {
            throw new IllegalArgumentException("Must provide a mutable routing nodes instance");
        }
        this.routingNodes = routingNodes;
        this.isSimulating = isSimulating;
        this.routingChangesObserver = new RoutingChangesObserver.DelegatingRoutingChangesObserver(
            isSimulating
                ? new RoutingChangesObserver[] {
                    nodesChangedObserver,
                    indexMetadataUpdater,
                    restoreInProgressUpdater,
                    resizeSourceIndexUpdater,
                    new MaxWriteLoadProportionCacheInvalidator() }
                : new RoutingChangesObserver[] {
                    nodesChangedObserver,
                    indexMetadataUpdater,
                    restoreInProgressUpdater,
                    resizeSourceIndexUpdater,
                    new MaxWriteLoadProportionCacheInvalidator(),
                    shardChangesObserver }
        );
    }

    /**
     * Get current routing nodes
     * @return routing nodes
     */
    @Override
    public RoutingNodes routingNodes() {
        if (routingNodes != null) {
            return routingNodes;
        }
        return super.routingNodes();
    }

    @Override
    public void ignoreDisable(boolean ignoreDisable) {
        this.ignoreDisable = ignoreDisable;
    }

    @Override
    public boolean ignoreDisable() {
        return ignoreDisable;
    }

    @Override
    public RoutingChangesObserver changes() {
        return routingChangesObserver;
    }

    @Override
    public RoutingAllocation immutableClone() {
        GlobalRoutingTable routingTable = clusterState.globalRoutingTable();
        return new ImmutableRoutingAllocation(
            deciders,
            routingNodesChanged()
                ? ClusterState.builder(clusterState).routingTable(routingTable.rebuild(routingNodes(), metadata())).build()
                : clusterState,
            clusterInfo,
            shardSizeInfo,
            currentNanoTime
        );
    }

    @Override
    public void setSimulatedClusterInfo(ClusterInfo clusterInfo) {
        assert isSimulating : "Should be called only while simulating";
        this.clusterInfo = clusterInfo;
    }

    /**
     * Remove the allocation id of the provided shard from the set of in-sync shard copies
     */
    @Override
    public void removeAllocationId(ShardRouting shardRouting) {
        indexMetadataUpdater.removeAllocationId(shardRouting);
    }

    /**
     * Returns updated {@link Metadata} based on the changes that were made to the routing nodes
     */
    @Override
    public Metadata updateMetadataWithRoutingChanges(GlobalRoutingTable newRoutingTable) {
        Metadata metadata = indexMetadataUpdater.applyChanges(metadata(), newRoutingTable);
        return resizeSourceIndexUpdater.applyChanges(metadata, newRoutingTable);
    }

    /**
     * Returns updated {@link RestoreInProgress} based on the changes that were made to the routing nodes
     */
    @Override
    public RestoreInProgress updateRestoreInfoWithRoutingChanges(RestoreInProgress restoreInProgress) {
        return restoreInProgressUpdater.applyChanges(restoreInProgress);
    }

    /**
     * Returns true iff changes were made to the routing nodes
     */
    @Override
    public boolean routingNodesChanged() {
        return nodesChangedObserver.isChanged();
    }

    /**
     * @return {@code true} if this allocation computation is trying to reconcile towards a previously-computed allocation and therefore
     *                      path-dependent allocation blockers should be ignored.
     */
    @Override
    public boolean isReconciling() {
        return isReconciling;
    }

    @Override
    public boolean isSimulating() {
        return isSimulating;
    }

    /**
     * Set the {@link #isReconciling} flag, and return a {@link Releasable} which clears it again.
     */
    @Override
    public Releasable withReconcilingFlag() {
        assert isReconciling == false : "already reconciling";
        isReconciling = true;
        return () -> isReconciling = false;
    }

    /**
     * Whenever a shard moves in or out of {@link org.elasticsearch.cluster.routing.ShardRoutingState#STARTED}, we invalidate any
     * cached max write-load proportion values for the affected node(s)
     */
    private class MaxWriteLoadProportionCacheInvalidator implements RoutingChangesObserver {

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            clusterInfo.invalidateNodeMaxShardWriteLoadProportion(startedShard.currentNodeId());
        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, String reason) {
            clusterInfo.invalidateNodeMaxShardWriteLoadProportion(startedShard.currentNodeId());
        }
    }
}
