/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.IncomingRecoveryLimits;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/// The slots that a target has for incoming non-critical recoveries, which relocations share with the recoveries of unassigned shards
/// added for performance reasons and with snapshot restores. [RelocationSourceShutdownTimeouts] shares them when it turns the time left
/// until the grace deadline of a shutting-down node, see [ShutdownWarmingDeadline], into a warming timeout. It only counts the
/// relocations as using the slots.
public class TargetRelocationSlotsEstimator {

    /// The values of the master settings at which the master leaves the throttling of the recoveries to a search node to the data nodes, as
    /// with data node recovery throttling (DNRT)
    private static final Map<Setting<Integer>, Integer> MASTER_THROTTLING_DISABLED_VALUES = Map.of(
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
        Integer.MAX_VALUE,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
        Integer.MAX_VALUE,
        ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
        -1
    );

    private volatile int maxConcurrentNonCriticalRecoveries;
    private volatile boolean masterThrottlingDisabled;

    public TargetRelocationSlotsEstimator(ClusterSettings clusterSettings, Settings nodeSettings, ByteSizeValue maxHeap) {
        DataNodeRecoveryThrottlingSettings.watchIncomingRecoveryLimits(
            clusterSettings,
            nodeSettings,
            maxHeap,
            limits -> this.maxConcurrentNonCriticalRecoveries = limits.maxConcurrentNonCriticalRecoveries()
        );
        this.masterThrottlingDisabled = masterThrottlingDisabled(clusterSettings::get);
        clusterSettings.addSettingsUpdateConsumer(
            settings -> this.masterThrottlingDisabled = masterThrottlingDisabled(setting -> setting.get(settings)),
            List.copyOf(MASTER_THROTTLING_DISABLED_VALUES.keySet())
        );
    }

    /// The slots known to be available to the relocations to the target: the data node limit, or, while the master throttles and reserves
    /// no slots, only the ones in use by the `relocationsInFlight`. The data node limit is enforced by the target whoever else throttles,
    /// so the relocations in flight beyond it are queued on the target and do not use slots.
    ///
    /// The settings are the ones of this node, and during a rolling upgrade they may be ahead of the master's, which still throttles and
    /// leaves the shards it has not got to waiting in the cluster state, whereas with data node recovery throttling the master starts all
    /// the relocations of a shutting-down node at once. So the settings are trusted only when the cluster state agrees, see
    /// [#allSearchShardsOnShuttingDownNodesRelocating].
    public int knownAvailableSlots(final ClusterState state, final int relocationsInFlight) {
        final int maxRecoveries = maxConcurrentNonCriticalRecoveries;
        return dataNodeRecoveryThrottlingEnabled(maxRecoveries) && allSearchShardsOnShuttingDownNodesRelocating(state)
            ? maxRecoveries
            : Math.min(maxRecoveries, relocationsInFlight);
    }

    /// The data node limit on the incoming non-critical recoveries of a target. It bounds the relocations to the target only while the
    /// data nodes throttle them, see [#dataNodeRecoveryThrottlingEnabled()].
    public int maxConcurrentRelocations() {
        return maxConcurrentNonCriticalRecoveries;
    }

    /// Whether the data nodes throttle the relocations, and not the master, see [#dataNodeRecoveryThrottlingEnabled(int)].
    public boolean dataNodeRecoveryThrottlingEnabled() {
        return dataNodeRecoveryThrottlingEnabled(maxConcurrentNonCriticalRecoveries);
    }

    /// The data nodes throttle the relocations, and not the master, when their non-critical limit is finite and the master does not
    /// throttle, see [#masterThrottlingDisabled]. The data node settings are only registered by the stateless plugin, and are unbounded by
    /// default. While the master throttles, the cluster state shows only the relocations it has started, whatever the data node limit is.
    // TODO: move to DataNodeRecoveryThrottlingSettings (elastic/elasticsearch-team#5653)
    private boolean dataNodeRecoveryThrottlingEnabled(int maxRecoveries) {
        return maxRecoveries < IncomingRecoveryLimits.UNBOUNDED.maxConcurrentNonCriticalRecoveries() && masterThrottlingDisabled;
    }

    /// Whether every search shard on the search nodes marked for removal has already started to relocate. Shards that are not started
    /// yet (initializing) cannot relocate, so they do not count.
    private static boolean allSearchShardsOnShuttingDownNodesRelocating(final ClusterState state) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        for (String nodeId : nodeShutdowns.getAllNodeIds()) {
            final var discoveryNode = state.nodes().get(nodeId);
            if (nodeShutdowns.isNodeMarkedForRemoval(nodeId) == false
                || discoveryNode == null
                || discoveryNode.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()) == false) {
                continue;
            }
            final var routingNode = state.getRoutingNodes().node(nodeId);
            // a search node only holds search shards, as a promotable shard is only allowed on an index node
            if (routingNode != null && routingNode.numberOfShardsWithState(ShardRoutingState.STARTED) > 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean masterThrottlingDisabled(final Function<Setting<Integer>, Integer> currentValue) {
        return MASTER_THROTTLING_DISABLED_VALUES.entrySet()
            .stream()
            .allMatch(disabledValue -> disabledValue.getValue().equals(currentValue.apply(disabledValue.getKey())));
    }
}
