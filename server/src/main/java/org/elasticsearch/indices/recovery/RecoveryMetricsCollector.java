/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/// Collects and emits recovery metrics.
public class RecoveryMetricsCollector implements IndexEventListener, RecoverySchedulingListener {

    private static final Logger logger = LogManager.getLogger(RecoveryMetricsCollector.class);

    public static final String RECOVERY_TOTAL_COUNT_METRIC = "es.recovery.shard.count.total";
    public static final String RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS = "es.recovery.shard.total.time";
    public static final String RECOVERY_INDEX_TIME_METRIC_IN_SECONDS = "es.recovery.shard.index.time";
    public static final String RECOVERY_TRANSLOG_TIME_METRIC_IN_SECONDS = "es.recovery.shard.translog.time";

    public static final String CURRENT_PEER_RECOVERIES_AS_SOURCE = "es.recovery.peer.source.active.current";
    public static final String QUEUED_PEER_RECOVERIES_AS_SOURCE = "es.recovery.peer.source.queued.current";
    public static final String CURRENT_PEER_RECOVERIES_AS_TARGET = "es.recovery.peer.target.active.current";
    public static final String QUEUED_PEER_RECOVERIES_AS_TARGET = "es.recovery.peer.target.queued.current";
    public static final String CURRENT_STORE_RECOVERIES = "es.recovery.store.active.current";
    public static final String QUEUED_STORE_RECOVERIES = "es.recovery.store.queued.current";

    public static final String RECOVERY_DIRECT_CANCELLATIONS_METRIC = "es.recovery.shard.directcancellations.total";
    public static final String RECOVERY_GATE_BLOCKED_TOTAL_METRIC = "es.recovery.gate.blocked.total";
    public static final String RECOVERY_GATE_BLOCKED_DURATION_METRIC = "es.recovery.gate.blocked.time";
    public static final String RECOVERY_GATE_NAME_ATTRIBUTE_KEY = "es_recovery_gate_name";

    public static final RecoveryMetricsCollector NOOP = new RecoveryMetricsCollector(TelemetryProvider.NOOP);

    private final LongCounter shardRecoveryTotalMetric;
    private final LongHistogram shardRecoveryTotalTimeMetric;
    private final LongHistogram shardRecoveryIndexTimeMetric;
    private final LongHistogram shardRecoveryTranslogTimeMetric;

    private final LongUpDownCounter activePeerRecoveriesAsSourceMetric;
    private final LongUpDownCounter queuedPeerRecoveriesAsSourceMetric;
    private final LongUpDownCounter activePeerRecoveriesAsTargetMetric;
    private final LongUpDownCounter queuedPeerRecoveriesAsTargetMetric;
    private final LongUpDownCounter activeStoreRecoveriesMetric;
    private final LongUpDownCounter queuedStoreRecoveriesMetric;

    private final LongCounter shardRecoveryDirectCancellationsMetric;
    private final LongCounter recoveryGateBlockedMetric;
    private final LongHistogram recoveryGateBlockedDurationMetric;

    public RecoveryMetricsCollector(TelemetryProvider telemetryProvider) {
        final MeterRegistry meterRegistry = telemetryProvider.getMeterRegistry();
        shardRecoveryTotalMetric = meterRegistry.registerLongCounter(
            RECOVERY_TOTAL_COUNT_METRIC,
            "Number of times shard recovery has happened",
            "unit"
        );
        shardRecoveryTotalTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS,
            "Total elapsed shard recovery time in seconds",
            "seconds"
        );
        shardRecoveryIndexTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_INDEX_TIME_METRIC_IN_SECONDS,
            "Elapsed shard index (stage) recovery time in seconds",
            "seconds"
        );
        shardRecoveryTranslogTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_TRANSLOG_TIME_METRIC_IN_SECONDS,
            "Elapsed shard translog (stage) recovery time in seconds",
            "seconds"
        );
        activePeerRecoveriesAsSourceMetric = meterRegistry.registerLongUpDownCounter(
            CURRENT_PEER_RECOVERIES_AS_SOURCE,
            "Number of currently active peer recoveries for which this node is the source",
            "unit"
        );
        queuedPeerRecoveriesAsSourceMetric = meterRegistry.registerLongUpDownCounter(
            QUEUED_PEER_RECOVERIES_AS_SOURCE,
            "Number of currently queued peer recoveries for which this node is the source",
            "unit"
        );
        activePeerRecoveriesAsTargetMetric = meterRegistry.registerLongUpDownCounter(
            CURRENT_PEER_RECOVERIES_AS_TARGET,
            "Number of currently active peer recoveries for which this node is the target",
            "unit"
        );
        queuedPeerRecoveriesAsTargetMetric = meterRegistry.registerLongUpDownCounter(
            QUEUED_PEER_RECOVERIES_AS_TARGET,
            "Number of currently queued peer recoveries for which this node is the target",
            "unit"
        );
        activeStoreRecoveriesMetric = meterRegistry.registerLongUpDownCounter(
            CURRENT_STORE_RECOVERIES,
            "Number of currently active non-peer recoveries",
            "unit"
        );
        queuedStoreRecoveriesMetric = meterRegistry.registerLongUpDownCounter(
            QUEUED_STORE_RECOVERIES,
            "Number of currently queued non-peer recoveries",
            "unit"
        );
        shardRecoveryDirectCancellationsMetric = meterRegistry.registerLongCounter(
            RECOVERY_DIRECT_CANCELLATIONS_METRIC,
            "Number of shard recoveries that have been directly cancelled by the master, while queued or started",
            "unit"
        );
        recoveryGateBlockedMetric = meterRegistry.registerLongCounter(
            RECOVERY_GATE_BLOCKED_TOTAL_METRIC,
            "Number of times recovery dispatch entered the blocked state",
            "unit"
        );
        recoveryGateBlockedDurationMetric = meterRegistry.registerLongHistogram(
            RECOVERY_GATE_BLOCKED_DURATION_METRIC,
            "Duration recovery dispatch remained blocked by recovery gates",
            "ms"
        );
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        try {
            if (indexShard.state() == IndexShardState.RECOVERING) {
                final RecoveryState recoveryState = indexShard.recoveryState();
                assert recoveryState != null;
                if (recoveryState.getStage() == Stage.DONE) {
                    shardRecoveryTotalMetric.increment();
                    final Map<String, Object> metricLabels = recoveryTimeMetricLabels(indexShard);
                    shardRecoveryTotalTimeMetric.record(recoveryState.getTimer().time() / 1000, metricLabels);
                    shardRecoveryIndexTimeMetric.record(recoveryState.getIndex().time() / 1000, metricLabels);
                    shardRecoveryTranslogTimeMetric.record(recoveryState.getTranslog().time() / 1000, metricLabels);
                }
            }
        } catch (Exception e) {
            logger.warn("Unexpected error during pushing index recovery metrics", e);
        } finally {
            listener.onResponse(null);
        }
    }

    private static Map<String, Object> recoveryTimeMetricLabels(IndexShard indexShard) {
        return Map.of(
            "es_is_primary",
            indexShard.routingEntry().primary(),
            "es_recovery_type",
            indexShard.recoveryState().getRecoverySource().getType().name()
        );
    }

    @Override
    public void onRecoveryCancelledBeforeQueuingOnTarget(RecoverySource.Type type) {
        // Record this as queued in metrics for simplicity, we can refine the distinction later on if needed
        shardRecoveryDirectCancellationsMetric.incrementBy(1, directCancellationMetricLabels(type, RecoverySchedulingState.QUEUED));
    }

    @Override
    public void onRecoveryQueuedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> queuedStoreRecoveriesMetric.add(
                1,
                storeRecoveryTargetLifecycleMetricLabels(type, priorityGroup)
            );
            case PEER -> queuedPeerRecoveriesAsTargetMetric.add(1, peerRecoveryTargetLifecycleMetricLabels(priorityGroup));
        }
    }

    @Override
    public void onPeerRecoveryQueuedOnSource() {
        queuedPeerRecoveriesAsSourceMetric.add(1, peerRecoverySourceLifecycleMetricLabels());
    }

    @Override
    public void onQueuedRecoveryDiscardedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> queuedStoreRecoveriesMetric.add(
                -1,
                storeRecoveryTargetLifecycleMetricLabels(type, priorityGroup)
            );
            case PEER -> queuedPeerRecoveriesAsTargetMetric.add(-1, peerRecoveryTargetLifecycleMetricLabels(priorityGroup));
        }
    }

    @Override
    public void onQueuedPeerRecoveryDiscardedOnSource() {
        queuedPeerRecoveriesAsSourceMetric.add(-1, peerRecoverySourceLifecycleMetricLabels());
    }

    @Override
    public void onQueuedRecoveryCancelledOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> queuedStoreRecoveriesMetric.add(
                -1,
                storeRecoveryTargetLifecycleMetricLabels(type, priorityGroup)
            );
            case PEER -> queuedPeerRecoveriesAsTargetMetric.add(-1, peerRecoveryTargetLifecycleMetricLabels(priorityGroup));
        }
        shardRecoveryDirectCancellationsMetric.incrementBy(1, directCancellationMetricLabels(type, RecoverySchedulingState.QUEUED));
    }

    @Override
    public void onPeerRecoveryStartedOnSource() {
        activePeerRecoveriesAsSourceMetric.add(1, peerRecoverySourceLifecycleMetricLabels());
    }

    @Override
    public void onRecoveryDequeuedAndStartedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> {
                queuedStoreRecoveriesMetric.add(-1, storeRecoveryTargetLifecycleMetricLabels(type, priorityGroup));
                activeStoreRecoveriesMetric.add(1, storeRecoveryTargetLifecycleMetricLabels(type, priorityGroup));
            }
            case PEER -> {
                queuedPeerRecoveriesAsTargetMetric.add(-1, peerRecoveryTargetLifecycleMetricLabels(priorityGroup));
                activePeerRecoveriesAsTargetMetric.add(1, peerRecoveryTargetLifecycleMetricLabels(priorityGroup));
            }
        }
    }

    @Override
    public void onPeerRecoveryDequeuedAndStartedOnSource() {
        queuedPeerRecoveriesAsSourceMetric.add(-1, peerRecoverySourceLifecycleMetricLabels());
        activePeerRecoveriesAsSourceMetric.add(1, peerRecoverySourceLifecycleMetricLabels());
    }

    @Override
    public void onStartedRecoveryCancelledOnTarget(RecoverySource.Type type) {
        shardRecoveryDirectCancellationsMetric.incrementBy(1, directCancellationMetricLabels(type, RecoverySchedulingState.STARTED));
    }

    @Override
    public void onRecoveryCompletedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> activeStoreRecoveriesMetric.add(
                -1,
                storeRecoveryTargetLifecycleMetricLabels(type, priorityGroup)
            );
            case PEER -> activePeerRecoveriesAsTargetMetric.add(-1, peerRecoveryTargetLifecycleMetricLabels(priorityGroup));
        }
    }

    @Override
    public void onPeerRecoveryCompletedOnSource() {
        activePeerRecoveriesAsSourceMetric.add(-1, peerRecoverySourceLifecycleMetricLabels());
    }

    @Override
    public void onRecoveriesBlocked(String gateName) {
        recoveryGateBlockedMetric.incrementBy(1, Map.of(RECOVERY_GATE_NAME_ATTRIBUTE_KEY, gateName));
    }

    @Override
    public void onRecoveriesUnblocked(long blockedTimeMillis) {
        recoveryGateBlockedDurationMetric.record(blockedTimeMillis);
    }

    private static Map<String, Object> storeRecoveryTargetLifecycleMetricLabels(RecoverySource.Type type, PriorityGroup priorityGroup) {
        return Map.of("es_recovery_type", type.name(), "es_recovery_priority_group", priorityGroup.name());
    }

    private static Map<String, Object> peerRecoveryTargetLifecycleMetricLabels(PriorityGroup priorityGroup) {
        return Map.of("es_recovery_priority_group", priorityGroup.name());
    }

    private static Map<String, Object> peerRecoverySourceLifecycleMetricLabels() {
        return Map.of();
    }

    private static Map<String, Object> directCancellationMetricLabels(RecoverySource.Type type, RecoverySchedulingState state) {
        return Map.of("es_recovery_type", type.name(), "es_recovery_scheduling_state", state.name());
    }

    private enum RecoverySchedulingState {
        QUEUED,
        STARTED
    }
}
