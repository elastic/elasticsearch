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
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

public final class MutableRoutingAllocation extends RoutingAllocation {

    private final IndexMetadataUpdater indexMetadataUpdater = new IndexMetadataUpdater();
    private final RoutingNodesChangedObserver nodesChangedObserver = new RoutingNodesChangedObserver();
    private final RestoreService.RestoreInProgressUpdater restoreInProgressUpdater = new RestoreService.RestoreInProgressUpdater();
    private final ResizeSourceIndexSettingsUpdater resizeSourceIndexUpdater = new ResizeSourceIndexSettingsUpdater();

    private final RoutingChangesObserver routingChangesObserver;
    private final RoutingNodes routingNodes;

    /**
     * Creates a new {@link RoutingAllocation}
     * @param deciders {@link AllocationDeciders} to used to make decisions for routing allocations
     * @param routingNodes Routing nodes in the current cluster or {@code null} if using those in the given cluster state
     * @param clusterState cluster state before rerouting
     * @param clusterInfo {@link ClusterInfo} to use for allocation decisions
     * @param currentNanoTime the nano time to use for all delay allocation calculation (typically {@link System#nanoTime()})
     */
    MutableRoutingAllocation(
        AllocationDeciders deciders,
        RoutingNodes routingNodes,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        this(deciders, routingNodes, clusterState, clusterInfo, shardSizeInfo, currentNanoTime, false);
    }

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
        boolean isSimulating
    ) {
        super(deciders, clusterState, clusterInfo, shardSizeInfo, currentNanoTime, isSimulating);
        if (routingNodes == null || routingNodes.isReadOnly()) {
            throw new IllegalArgumentException("Must provide a mutable routing nodes instance");
        }
        this.routingNodes = routingNodes;
        this.routingChangesObserver = new RoutingChangesObserver.DelegatingRoutingChangesObserver(
            isSimulating
                ? new RoutingChangesObserver[] {
                    nodesChangedObserver,
                    indexMetadataUpdater,
                    restoreInProgressUpdater,
                    resizeSourceIndexUpdater }
                : new RoutingChangesObserver[] {
                    nodesChangedObserver,
                    indexMetadataUpdater,
                    restoreInProgressUpdater,
                    resizeSourceIndexUpdater,
                    new ShardChangesObserver() }
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

    public void setSimulatedClusterInfo(ClusterInfo clusterInfo) {
        assert isSimulating : "Should be called only while simulating";
        this.clusterInfo = clusterInfo;
    }

    /**
     * Remove the allocation id of the provided shard from the set of in-sync shard copies
     */
    public void removeAllocationId(ShardRouting shardRouting) {
        indexMetadataUpdater.removeAllocationId(shardRouting);
    }

    /**
     * Returns updated {@link Metadata} based on the changes that were made to the routing nodes
     */
    public Metadata updateMetadataWithRoutingChanges(GlobalRoutingTable newRoutingTable) {
        Metadata metadata = indexMetadataUpdater.applyChanges(metadata(), newRoutingTable);
        return resizeSourceIndexUpdater.applyChanges(metadata, newRoutingTable);
    }

    /**
     * Returns updated {@link RestoreInProgress} based on the changes that were made to the routing nodes
     */
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
}
