/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RestoreService.RestoreInProgressUpdater;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * The {@link RoutingAllocation} keep the state of the current allocation
 * of shards and holds the {@link AllocationDeciders} which are responsible
 *  for the current routing state.
 */
public class RoutingAllocation {

    private final AllocationDeciders deciders;

    @Nullable
    private final RoutingNodes routingNodes;

    private final ClusterState clusterState;

    private ClusterInfo clusterInfo;

    private final SnapshotShardSizeInfo shardSizeInfo;

    private Map<ShardId, Set<String>> ignoredShardToNodes = null;

    private boolean ignoreDisable = false;

    private DebugMode debugDecision = DebugMode.OFF;

    private boolean hasPendingAsyncFetch = false;

    private final long currentNanoTime;
    private final boolean isSimulating;
    private boolean isReconciling;

    private final IndexMetadataUpdater indexMetadataUpdater = new IndexMetadataUpdater();
    private final RoutingNodesChangedObserver nodesChangedObserver = new RoutingNodesChangedObserver();
    private final RestoreInProgressUpdater restoreInProgressUpdater = new RestoreInProgressUpdater();
    private final ResizeSourceIndexSettingsUpdater resizeSourceIndexUpdater = new ResizeSourceIndexSettingsUpdater();
    private final RoutingChangesObserver routingChangesObserver = new RoutingChangesObserver.DelegatingRoutingChangesObserver(
        nodesChangedObserver,
        indexMetadataUpdater,
        restoreInProgressUpdater,
        resizeSourceIndexUpdater
    );

    private final Map<String, SingleNodeShutdownMetadata> nodeReplacementTargets;

    @Nullable
    private final DesiredNodes desiredNodes;

    // Tracks the sizes of the searchable snapshots that aren't yet registered in ClusterInfo by their cluster node id
    private final Map<String, Long> unaccountedSearchableSnapshotSizes;

    public RoutingAllocation(
        AllocationDeciders deciders,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        this(deciders, null, clusterState, clusterInfo, shardSizeInfo, currentNanoTime);
    }

    /**
     * Creates a new {@link RoutingAllocation}
     * @param deciders {@link AllocationDeciders} to used to make decisions for routing allocations
     * @param routingNodes Routing nodes in the current cluster or {@code null} if using those in the given cluster state
     * @param clusterState cluster state before rerouting
     * @param clusterInfo information about node disk usage and shard disk usage
     * @param shardSizeInfo information about snapshot shard sizes
     * @param currentNanoTime the nano time to use for all delay allocation calculation (typically {@link System#nanoTime()})
     */
    public RoutingAllocation(
        AllocationDeciders deciders,
        @Nullable RoutingNodes routingNodes,
        ClusterState clusterState,
        @Nullable ClusterInfo clusterInfo,
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
     * @param currentNanoTime the nano time to use for all delay allocation calculation (typically {@link System#nanoTime()})
     * @param isSimulating {@code true} if "transient" deciders should be ignored because we are simulating the final allocation
     */
    private RoutingAllocation(
        AllocationDeciders deciders,
        @Nullable RoutingNodes routingNodes,
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime,
        boolean isSimulating
    ) {
        this.deciders = deciders;
        this.routingNodes = routingNodes;
        this.clusterState = clusterState;
        this.clusterInfo = clusterInfo;
        this.shardSizeInfo = shardSizeInfo;
        this.currentNanoTime = currentNanoTime;
        this.isSimulating = isSimulating;
        this.nodeReplacementTargets = nodeReplacementTargets(clusterState);
        this.desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        this.unaccountedSearchableSnapshotSizes = unaccountedSearchableSnapshotSizes(clusterState, clusterInfo);
    }

    private static Map<String, SingleNodeShutdownMetadata> nodeReplacementTargets(ClusterState clusterState) {
        Map<String, SingleNodeShutdownMetadata> nodeReplacementTargets = new HashMap<>();
        for (SingleNodeShutdownMetadata shutdown : clusterState.metadata().nodeShutdowns().getAll().values()) {
            if (shutdown.getType() == SingleNodeShutdownMetadata.Type.REPLACE) {
                nodeReplacementTargets.put(shutdown.getTargetNodeName(), shutdown);
            }
        }
        return Map.copyOf(nodeReplacementTargets);
    }

    private static Map<String, Long> unaccountedSearchableSnapshotSizes(ClusterState clusterState, ClusterInfo clusterInfo) {
        Map<String, Long> unaccountedSearchableSnapshotSizes = new HashMap<>();
        if (clusterInfo != null) {
            for (RoutingNode node : clusterState.getRoutingNodes()) {
                DiskUsage usage = clusterInfo.getNodeMostAvailableDiskUsages().get(node.nodeId());
                ClusterInfo.ReservedSpace reservedSpace = clusterInfo.getReservedSpace(node.nodeId(), usage != null ? usage.getPath() : "");
                long totalSize = 0;
                for (ShardRouting shard : node.started()) {
                    if (shard.getExpectedShardSize() > 0
                        && clusterState.metadata().getIndexSafe(shard.index()).isSearchableSnapshot()
                        && reservedSpace.containsShardId(shard.shardId()) == false
                        && clusterInfo.getShardSize(shard) == null) {
                        totalSize += shard.getExpectedShardSize();
                    }
                }
                if (totalSize > 0) {
                    unaccountedSearchableSnapshotSizes.put(node.nodeId(), totalSize);
                }
            }
        }
        return Collections.unmodifiableMap(unaccountedSearchableSnapshotSizes);
    }

    /** returns the nano time captured at the beginning of the allocation. used to make sure all time based decisions are aligned */
    public long getCurrentNanoTime() {
        return currentNanoTime;
    }

    /**
     * Get {@link AllocationDeciders} used for allocation
     * @return {@link AllocationDeciders} used for allocation
     */
    public AllocationDeciders deciders() {
        return this.deciders;
    }

    /**
     * Get routing table of current nodes
     * @return current routing table
     */
    public RoutingTable routingTable() {
        return clusterState.routingTable();
    }

    /**
     * Get current routing nodes
     * @return routing nodes
     */
    public RoutingNodes routingNodes() {
        if (routingNodes != null) {
            return routingNodes;
        }
        return clusterState.getRoutingNodes();
    }

    /**
     * Get metadata of routing nodes
     * @return Metadata of routing nodes
     */
    public Metadata metadata() {
        return clusterState.metadata();
    }

    /**
     * Get discovery nodes in current routing
     * @return discovery nodes
     */
    public DiscoveryNodes nodes() {
        return clusterState.nodes();
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public ClusterInfo clusterInfo() {
        return clusterInfo;
    }

    public SnapshotShardSizeInfo snapshotShardSizeInfo() {
        return shardSizeInfo;
    }

    @Nullable
    public DesiredNodes desiredNodes() {
        return desiredNodes;
    }

    /**
     * Returns a map of target node name to replacement shutdown
     */
    public Map<String, SingleNodeShutdownMetadata> replacementTargetShutdowns() {
        return this.nodeReplacementTargets;
    }

    public void ignoreDisable(boolean ignoreDisable) {
        this.ignoreDisable = ignoreDisable;
    }

    public boolean ignoreDisable() {
        return this.ignoreDisable;
    }

    public void setDebugMode(DebugMode debug) {
        this.debugDecision = debug;
    }

    public void debugDecision(boolean debug) {
        this.debugDecision = debug ? DebugMode.ON : DebugMode.OFF;
    }

    public boolean debugDecision() {
        return this.debugDecision != DebugMode.OFF;
    }

    public DebugMode getDebugMode() {
        return this.debugDecision;
    }

    public void addIgnoreShardForNode(ShardId shardId, String nodeId) {
        if (ignoredShardToNodes == null) {
            ignoredShardToNodes = new HashMap<>();
        }
        ignoredShardToNodes.computeIfAbsent(shardId, k -> new HashSet<>()).add(nodeId);
    }

    /**
     * Returns whether the given node id should be ignored from consideration when {@link AllocationDeciders}
     * is deciding whether to allocate the specified shard id to that node.  The node will be ignored if
     * the specified shard failed on that node, triggering the current round of allocation.  Since the shard
     * just failed on that node, we don't want to try to reassign it there, if the node is still a part
     * of the cluster.
     *
     * @param shardId the shard id to be allocated
     * @param nodeId the node id to check against
     * @return true if the node id should be ignored in allocation decisions, false otherwise
     */
    public boolean shouldIgnoreShardForNode(ShardId shardId, String nodeId) {
        if (ignoredShardToNodes == null) {
            return false;
        }
        Set<String> nodes = ignoredShardToNodes.get(shardId);
        return nodes != null && nodes.contains(nodeId);
    }

    public Set<String> getIgnoreNodes(ShardId shardId) {
        if (ignoredShardToNodes == null) {
            return emptySet();
        }
        Set<String> ignore = ignoredShardToNodes.get(shardId);
        if (ignore == null) {
            return emptySet();
        }
        return Set.copyOf(ignore);
    }

    /**
     * Remove the allocation id of the provided shard from the set of in-sync shard copies
     */
    public void removeAllocationId(ShardRouting shardRouting) {
        indexMetadataUpdater.removeAllocationId(shardRouting);
    }

    /**
     * Returns observer to use for changes made to the routing nodes
     */
    public RoutingChangesObserver changes() {
        return routingChangesObserver;
    }

    /**
     * Returns updated {@link Metadata} based on the changes that were made to the routing nodes
     */
    public Metadata updateMetadataWithRoutingChanges(RoutingTable newRoutingTable) {
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
    public boolean routingNodesChanged() {
        return nodesChangedObserver.isChanged();
    }

    /**
     * Create a routing decision, including the reason if the debug flag is
     * turned on
     * @param decision decision whether to allow/deny allocation
     * @param deciderLabel a human readable label for the AllocationDecider
     * @param reason a format string explanation of the decision
     * @param params format string parameters
     */
    public Decision decision(Decision decision, String deciderLabel, String reason, Object... params) {
        if (debugDecision()) {
            return Decision.single(decision.type(), deciderLabel, reason, params);
        } else {
            return decision;
        }
    }

    /**
     * Returns <code>true</code> iff the current allocation run has not processed all of the in-flight or available
     * shard or store fetches. Otherwise <code>true</code>
     */
    public boolean hasPendingAsyncFetch() {
        return hasPendingAsyncFetch;
    }

    /**
     * Sets a flag that signals that current allocation run has not processed all of the in-flight or available shard or store fetches.
     * This state is anti-viral and can be reset in on allocation run.
     */
    public void setHasPendingAsyncFetch() {
        this.hasPendingAsyncFetch = true;
    }

    /**
     * Returns an approximation of the size (in bytes) of the unaccounted searchable snapshots before the allocation
     */
    public long unaccountedSearchableSnapshotSize(RoutingNode routingNode) {
        return unaccountedSearchableSnapshotSizes.getOrDefault(routingNode.nodeId(), 0L);
    }

    /**
     * @return {@code true} if this allocation computation is trying to simulate the final allocation and therefore "transient" allocation
     *                      blockers should be ignored.
     */
    public boolean isSimulating() {
        return isSimulating;
    }

    /**
     * @return {@code true} if this allocation computation is trying to reconcile towards a previously-computed allocation and therefore
     *                      path-dependent allocation blockers should be ignored.
     */
    public boolean isReconciling() {
        return isReconciling;
    }

    /**
     * Set the {@link #isReconciling} flag, and return a {@link Releasable} which clears it again.
     */
    public Releasable withReconcilingFlag() {
        assert isReconciling == false : "already reconciling";
        isReconciling = true;
        return () -> isReconciling = false;
    }

    public void setSimulatedClusterInfo(ClusterInfo clusterInfo) {
        assert isSimulating : "Should be called only while simulating";
        this.clusterInfo = clusterInfo;
    }

    public RoutingAllocation immutableClone() {
        return new RoutingAllocation(
            deciders,
            routingNodesChanged()
                ? ClusterState.builder(clusterState)
                    .routingTable(RoutingTable.of(clusterState.routingTable().version(), routingNodes))
                    .build()
                : clusterState,
            clusterInfo,
            shardSizeInfo,
            currentNanoTime
        );
    }

    public RoutingAllocation mutableCloneForSimulation() {
        return new RoutingAllocation(
            deciders,
            clusterState.mutableRoutingNodes(),
            clusterState,
            clusterInfo,
            shardSizeInfo,
            currentNanoTime,
            true
        );
    }

    public enum DebugMode {
        /**
         * debug mode is off
         */
        OFF,
        /**
         * debug mode is on
         */
        ON,
        /**
         * debug mode is on, but YES decisions from a {@link org.elasticsearch.cluster.routing.allocation.decider.Decision.Multi}
         * are not included.
         */
        EXCLUDE_YES_DECISIONS
    }
}
