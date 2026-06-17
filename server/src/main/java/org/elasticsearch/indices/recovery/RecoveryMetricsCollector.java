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
public class RecoveryMetricsCollector implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RecoveryMetricsCollector.class);

    public static final String RECOVERY_TOTAL_COUNT_METRIC = "es.recovery.shard.count.total";
    public static final String RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS = "es.recovery.shard.total.time";
    public static final String RECOVERY_INDEX_TIME_METRIC_IN_SECONDS = "es.recovery.shard.index.time";
    public static final String RECOVERY_TRANSLOG_TIME_METRIC_IN_SECONDS = "es.recovery.shard.translog.time";
    public static final String ACTIVE_OUTGOING_PEER_RECOVERIES_METRIC = "es.recovery.peer.source.active.current";
    public static final String QUEUED_OUTGOING_PEER_RECOVERIES_METRIC = "es.recovery.peer.source.queued.current";

    public static final RecoveryMetricsCollector NOOP = new RecoveryMetricsCollector(TelemetryProvider.NOOP);

    private final LongCounter shardRecoveryTotalMetric;
    private final LongHistogram shardRecoveryTotalTimeMetric;
    private final LongHistogram shardRecoveryIndexTimeMetric;
    private final LongHistogram shardRecoveryTranslogTimeMetric;
    private final LongUpDownCounter activeOutgoingPeerRecoveriesMetric;
    private final LongUpDownCounter queuedOutgoingPeerRecoveriesMetric;

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
        activeOutgoingPeerRecoveriesMetric = meterRegistry.registerLongUpDownCounter(
            ACTIVE_OUTGOING_PEER_RECOVERIES_METRIC,
            "Number of currently active outgoing peer recoveries from this source node",
            "unit"
        );
        queuedOutgoingPeerRecoveriesMetric = meterRegistry.registerLongUpDownCounter(
            QUEUED_OUTGOING_PEER_RECOVERIES_METRIC,
            "Number of outgoing peer recoveries queued on this source node awaiting available slots",
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
                    final Map<String, Object> metricLabels = recoveryMetricLabels(indexShard);
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

    /// Increment active outgoing recovery count by one.
    public void outgoingPeerRecoveryStarted() {
        activeOutgoingPeerRecoveriesMetric.add(1);
    }

    /// Decrement active outgoing recovery count by one.
    public void outgoingPeerRecoveryCompleted() {
        activeOutgoingPeerRecoveriesMetric.add(-1);
    }

    /// Increment queued outgoing recovery count by one.
    public void outgoingPeerRecoveryEnqueued() {
        queuedOutgoingPeerRecoveriesMetric.add(1);
    }

    /// Decrement queued outgoing recovery count by one.
    public void outgoingPeerRecoveryDequeued() {
        queuedOutgoingPeerRecoveriesMetric.add(-1);
    }

    private static Map<String, Object> recoveryMetricLabels(IndexShard indexShard) {
        return Map.of(
            "primary",
            indexShard.routingEntry().primary(),
            "recovery_type",
            indexShard.recoveryState().getRecoverySource().getType().name()
        );
    }
}
