/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterState;
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
/// added for performance reasons and with snapshot restores. It only counts the relocations as using the slots.
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
    /// [SearchShardRelocations#hasPendingRelocationsOnShuttingDownNodes].
    public int knownAvailableSlots(final ClusterState state, final int relocationsInFlight) {
        final int maxRecoveries = maxConcurrentNonCriticalRecoveries;
        return dataNodeRecoveryThrottlingEnabled(maxRecoveries)
            && SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(state) == false
                ? maxRecoveries
                : Math.min(maxRecoveries, relocationsInFlight);
    }

    /// The slots of a target for the non-critical recoveries, as the relocations: the data node limit on the non-critical recoveries. It is
    /// very large when the data nodes have no limit. The critical recoveries are not taken into account, as they have slots of their own
    /// above this limit, and only take the non-critical ones when they exceed those, which is rare and short-lived.
    public int slotsForNonCriticalRecoveries() {
        return maxConcurrentNonCriticalRecoveries;
    }

    /// The data nodes throttle the relocations, and not the master, when their non-critical limit is finite and the master does not
    /// throttle, see [#masterThrottlingDisabled]. The data node settings are only registered by the stateless plugin, and are unbounded by
    /// default. While the master throttles, the cluster state shows only the relocations it has started, whatever the data node limit is.
    // TODO: move to DataNodeRecoveryThrottlingSettings (elastic/elasticsearch-team#5653)
    private boolean dataNodeRecoveryThrottlingEnabled(int maxRecoveries) {
        return maxRecoveries < IncomingRecoveryLimits.UNBOUNDED.maxConcurrentNonCriticalRecoveries() && masterThrottlingDisabled;
    }

    private static boolean masterThrottlingDisabled(final Function<Setting<Integer>, Integer> currentValue) {
        return MASTER_THROTTLING_DISABLED_VALUES.entrySet()
            .stream()
            .allMatch(disabledValue -> disabledValue.getValue().equals(currentValue.apply(disabledValue.getKey())));
    }
}
