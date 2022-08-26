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
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RestoreService.RestoreInProgressUpdater;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

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

    private final ClusterInfo clusterInfo;

    private final SnapshotShardSizeInfo shardSizeInfo;

    private Map<ShardId, Set<String>> ignoredShardToNodes = null;

    private boolean ignoreDisable = false;

    private DebugMode debugDecision = DebugMode.OFF;

    private boolean hasPendingAsyncFetch = false;

    private final long currentNanoTime;

    private final IndexMetadataUpdater indexMetadataUpdater = new IndexMetadataUpdater();
    private final RoutingNodesChangedObserver nodesChangedObserver = new RoutingNodesChangedObserver();
    private final RestoreInProgressUpdater restoreInProgressUpdater = new RestoreInProgressUpdater();
    private final RoutingChangesObserver routingChangesObserver = new RoutingChangesObserver.DelegatingRoutingChangesObserver(
        nodesChangedObserver,
        indexMetadataUpdater,
        restoreInProgressUpdater
    );

    private final Map<String, SingleNodeShutdownMetadata> nodeReplacementTargets;

    private final Map<String, SingleNodeShutdownMetadata> nodeShutdowns;

    @Nullable
    private final DesiredNodes desiredNodes;

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
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        long currentNanoTime
    ) {
        this.deciders = deciders;
        this.routingNodes = routingNodes;
        this.clusterState = clusterState;
        this.clusterInfo = clusterInfo;
        this.shardSizeInfo = shardSizeInfo;
        this.currentNanoTime = currentNanoTime;
        this.nodeShutdowns = clusterState.metadata().nodeShutdowns();
        Map<String, SingleNodeShutdownMetadata> targetNameToShutdown = new HashMap<>();
        for (SingleNodeShutdownMetadata shutdown : nodeShutdowns.values()) {
            if (shutdown.getType() == SingleNodeShutdownMetadata.Type.REPLACE) {
                targetNameToShutdown.put(shutdown.getTargetNodeName(), shutdown);
            }
        }
        this.nodeReplacementTargets = Map.copyOf(targetNameToShutdown);
        this.desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
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
     * Returns the map of node id to shutdown metadata currently in the cluster
     */
    public Map<String, SingleNodeShutdownMetadata> nodeShutdowns() {
        return nodeShutdowns;
    }

    /**
     * Returns a map of target node name to replacement shutdown
     */
    public Map<String, SingleNodeShutdownMetadata> replacementTargetShutdowns() {
        return this.nodeReplacementTargets;
    }

    @SuppressWarnings("unchecked")
    public <T extends ClusterState.Custom> T custom(String key) {
        return (T) clusterState.customs().get(key);
    }

    public Map<String, ClusterState.Custom> getCustoms() {
        return clusterState.getCustoms();
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
        return indexMetadataUpdater.applyChanges(metadata(), newRoutingTable);
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
