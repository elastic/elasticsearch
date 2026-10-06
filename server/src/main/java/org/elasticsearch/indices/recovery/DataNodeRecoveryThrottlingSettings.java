/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;

import java.util.List;

/// Central class to define the data node recovery throttling (DNRT) settings that bound the number of shard recoveries on a node.
///
/// Note that the bandwidth and chunking settings are defined in [RecoverySettings].
/// The recovery gate and direct cancellation settings are defined in [RecoveryGateMonitor] and
/// [org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService] respectively.
///
/// All the below settings are currently registered only by the stateless plugin (see [#settings()]) and are
/// disabled or unbounded elsewhere. They are not yet part of `ClusterSettings#BUILT_IN_CLUSTER_SETTINGS`. Therefore,
/// on stateful clusters they cannot be configured, and the data node falls back to unbounded behavior. The master
/// then controls recovery throttling (see [org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider],
/// [org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider] and
/// `StatelessThrottlingConcurrentRecoveriesAllocationDecider`).
///
/// TODO: register [#settings()] in `BUILT_IN_CLUSTER_SETTINGS` once DNRT is ready for stateful (elasticsearch-team#2805).
///
/// Target (incoming) and source (outgoing) recovery throttling each use distinct levers.
///
/// ## Target (incoming) side
///
/// The combination of [#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING] and
/// [#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING] defines the max number of target recoveries
/// that can run concurrently on the data node, as `min(fixed, ceil(heapGb * perHeapGb))`.
/// This bound is applied by [ThrottlingRecoveryService], which then queues the recoveries that exceed it in
/// [org.elasticsearch.cluster.routing.ShardRouting.RecoveryPriority] order.
///
/// The additional [#INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING] defines which proportion of
/// this max limit can be used specifically for relocations.
///
/// ## Source (outgoing) side
///
/// The combination of [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING] and
/// [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING] defines the max number of recoveries for which
/// the data node can concurrently be the source, again as `min(fixed, ceil(heapGb * perHeapGb))`. Requests that exceed it are
/// queued in FIFO order and started as slots free up. The two settings are not applied by the same services:
///
/// - [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING] (source side, stateful and stateless): bounds peer
///   recoveries on stateful nodes via [PeerRecoverySourceService], and primary relocations on stateless indexing nodes via
///   `StatelessPrimaryRelocationSourceService`.
/// - [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING] (source side, stateless only): bounds primary
///   relocations on stateless indexing nodes only, and is not applied to stateful peer recoveries.
///
public final class DataNodeRecoveryThrottlingSettings {

    private DataNodeRecoveryThrottlingSettings() {}

    /// Controls the max number of concurrent recoveries allowed on this data node. Excludes peer recoveries for which this
    /// node is the source, see [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING].
    /// Includes both recoveries of unassigned shards and relocations.
    ///
    /// Note that the effective max concurrent recoveries limit also takes into account the heap-based throttle
    /// [#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING]. The effective max concurrent recoveries
    /// is then `min(max_concurrent_incoming_recoveries, ceil(heapGb * max_concurrent_incoming_recoveries_per_heap_gb))`.
    /// See [ThrottlingRecoveryService].
    ///
    /// See also [#INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING] which imposes an additional
    /// throttle on relocations only.
    ///
    /// Applies to: target side, stateful and stateless. Currently only registered by the stateless plugin, elsewhere disabled.
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_incoming_recoveries",
        Integer.MAX_VALUE,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /// Controls the heap-based limit on concurrent incoming recoveries. Excludes peer recoveries for which this
    /// node is the source, see [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING].
    /// Includes both recoveries of unassigned shards and relocations. Must be strictly positive: 0 is disallowed
    /// (consistent with the minimum of the other recovery throttle settings).
    ///
    /// Note that the effective max concurrent recoveries limit also takes into account the static throttle
    /// [#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING]. The effective max concurrent recoveries
    /// is then `min(max_concurrent_incoming_recoveries, ceil(heapGb * max_concurrent_incoming_recoveries_per_heap_gb))`.
    /// See [ThrottlingRecoveryService].
    ///
    /// See also [#INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING] which imposes an additional
    /// throttle on relocations only.
    ///
    /// Applies to: target side, stateful and stateless. Currently only registered by the stateless plugin, elsewhere disabled.
    public static final Setting<Double> INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING = Setting.doubleSetting(
        "indices.recovery.max_concurrent_incoming_recoveries_per_heap_gb",
        Double.MAX_VALUE,
        Double.MIN_NORMAL,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /// Controls the max proportion of the max allowed concurrent recoveries count that may be used for relocation recoveries.
    /// The max allowed concurrent recovery count is derived from [#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING]
    /// and [#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING]. See [ThrottlingRecoveryService].
    ///
    /// Accepts values like `0.5` or `"50%"`. Must be strictly positive: 0 is disallowed (consistent with the minimum of
    /// the other recovery throttle settings).
    ///
    /// Applies to: target side, stateful and stateless. Currently only registered by the stateless plugin, elsewhere disabled.
    public static final Setting<RatioValue> INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING = Setting.ratioSetting(
        "indices.recovery.incoming_recoveries_max_relocation_proportion",
        RatioValue.ONE_HUNDRED_PERCENT,
        RatioValue.ofPercent(Double.MIN_NORMAL),
        RatioValue.ONE_HUNDRED_PERCENT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /// Maximum number of outgoing recoveries a node may run concurrently as a source. On stateful nodes this bounds peer
    /// recoveries ([PeerRecoverySourceService]). On stateless indexing nodes it bounds primary relocations. Requests that
    /// arrive when all slots are occupied are queued in FIFO order and started as slots free up.
    ///
    /// On stateless indexing nodes, the effective limit also takes into account the heap-based throttle
    /// [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING]:
    /// `min(max_concurrent_outgoing_recoveries, ceil(heapGb * max_concurrent_outgoing_recoveries_per_heap_gb))`. The heap-based
    /// throttle is not applied to stateful peer recoveries.
    ///
    /// Applies to: source side, stateful and stateless. Currently only registered by the stateless plugin, elsewhere disabled.
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_outgoing_recoveries",
        Integer.MAX_VALUE,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// Controls the heap-based limit on concurrent outgoing primary relocations on stateless indexing nodes. Must be strictly
    /// positive: 0 is disallowed (consistent with the min of [#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING]).
    ///
    /// The effective limit is `min(max_concurrent_outgoing_recoveries, ceil(heapGb * max_concurrent_outgoing_recoveries_per_heap_gb))`.
    /// See `ThrottledPrimaryRelocations` in the stateless plugin.
    ///
    /// Applies to: source side, stateless only. It is only consumed by the stateless plugin because we have not yet decided
    /// whether to support heap-proportional throttling of outgoing peer recoveries on stateful clusters. The setting name is
    /// general enough that it could start applying to [PeerRecoverySourceService] later without a rename.
    public static final Setting<Double> INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING = Setting.doubleSetting(
        "indices.recovery.max_concurrent_outgoing_recoveries_per_heap_gb",
        Double.MAX_VALUE,
        Double.MIN_NORMAL,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /// All the settings defined in this class, to be registered together.
    public static List<Setting<?>> settings() {
        return List.of(
            INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING,
            INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING,
            INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING,
            INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
            INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING
        );
    }
}
