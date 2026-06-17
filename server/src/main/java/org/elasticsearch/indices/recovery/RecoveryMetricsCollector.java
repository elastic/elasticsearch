/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
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
            "primary",
            indexShard.routingEntry().primary(),
            "es_recovery_type",
            indexShard.recoveryState().getRecoverySource().getType().name()
        );
    }

    @Override
    public void onRecoveryQueued(RecoverySource.Type type, RecoveryDirection direction) {
        updateQueuedRecovery(type, direction, 1);
    }

    @Override
    public void onRecoveryStarted(RecoverySource.Type type, RecoveryDirection direction) {
        updateQueuedRecovery(type, direction, -1);
        updateActiveRecovery(type, direction, 1);
    }

    @Override
    public void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryDirection direction) {
        updateQueuedRecovery(type, direction, -1);
    }

    @Override
    public void onRecoveryCompleted(RecoverySource.Type type, RecoveryDirection direction) {
        updateActiveRecovery(type, direction, -1);
    }

    private void updateQueuedRecovery(RecoverySource.Type type, RecoveryDirection direction, int delta) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> {
                queuedStoreRecoveriesMetric.add(delta, recoveryLifecycleMetricLabels(type));
            }
            case PEER -> {
                if (direction == RecoveryDirection.TARGET) {
                    queuedPeerRecoveriesAsTargetMetric.add(delta);
                } else if (direction == RecoveryDirection.SOURCE) {
                    queuedPeerRecoveriesAsSourceMetric.add(delta);
                } else {
                    assert false : "peer recovery should have a direction";
                }
            }
        }
    }

    private void updateActiveRecovery(RecoverySource.Type type, RecoveryDirection direction, int delta) {
        switch (type) {
            case EMPTY_STORE, EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, RESHARD_SPLIT -> {
                activeStoreRecoveriesMetric.add(delta, recoveryLifecycleMetricLabels(type));
            }
            case PEER -> {
                if (direction == RecoveryDirection.TARGET) {
                    activePeerRecoveriesAsTargetMetric.add(delta);
                } else if (direction == RecoveryDirection.SOURCE) {
                    activePeerRecoveriesAsSourceMetric.add(delta);
                } else {
                    assert false : "peer recovery should have a direction";
                }
            }
        }
    }

    private static Map<String, Object> recoveryLifecycleMetricLabels(RecoverySource.Type type) {
        return Map.of("es_recovery_type", type.name());
    }
}
