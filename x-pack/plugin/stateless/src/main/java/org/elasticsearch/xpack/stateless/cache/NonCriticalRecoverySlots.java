/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

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
/// added for performance reasons and with snapshot restores. [RelocationSourceShutdownTimeouts] and
/// [RelocationOtherNodeShutdownTimeouts] share them when they turn the time left until the grace deadline of a shutting-down node, see
/// [ShutdownWarmingDeadlines], into a warming timeout. They only count the relocations as using the slots.
class NonCriticalRecoverySlots {

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

    NonCriticalRecoverySlots(ClusterSettings clusterSettings, Settings nodeSettings, ByteSizeValue maxHeap) {
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

    /// The slots that the relocations to the target can count on: the data node limit when the data nodes throttle, or, while the master
    /// throttles and reserves no slots, only the ones in use by the `relocationsInFlight`.
    int effectiveSlots(final int relocationsInFlight) {
        return dataNodeRecoveryThrottlingEnabled() ? maxConcurrentNonCriticalRecoveries : relocationsInFlight;
    }

    /// The data nodes throttle the relocations, and not the master, when their non-critical limit is finite and the master does not
    /// throttle, see [#masterThrottlingDisabled]. The data node settings are only registered by the stateless plugin, and are unbounded by
    /// default. While the master throttles, the cluster state shows only the relocations it has started, whatever the data node limit is.
    // TODO: move to DataNodeRecoveryThrottlingSettings (elastic/elasticsearch-team#5653)
    boolean dataNodeRecoveryThrottlingEnabled() {
        return maxConcurrentNonCriticalRecoveries < IncomingRecoveryLimits.UNBOUNDED.maxConcurrentNonCriticalRecoveries()
            && masterThrottlingDisabled;
    }

    private static boolean masterThrottlingDisabled(final Function<Setting<Integer>, Integer> currentValue) {
        return MASTER_THROTTLING_DISABLED_VALUES.entrySet()
            .stream()
            .allMatch(disabledValue -> disabledValue.getValue().equals(currentValue.apply(disabledValue.getKey())));
    }
}
