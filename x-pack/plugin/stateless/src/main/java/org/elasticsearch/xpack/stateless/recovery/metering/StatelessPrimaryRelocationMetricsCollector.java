/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.metering;

import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.recovery.RelocationSourceMetrics;

/// Collects timing histograms for the primary shard relocation protocol in stateless deployments.
///
/// Tracks the duration of each phase on both the relocation source (initial flush, permit acquisition,
/// second flush, handoff) and the relocation target (pre-recovery, indexing shard state read, engine open).
/// Registered only on index nodes; search nodes do not participate in primary relocation.
public class StatelessPrimaryRelocationMetricsCollector {

    // The total relocation duration is already covered by es.recovery.shard.total.time (target-side recovery timer);
    // these phases break it down further.
    public static final String RELOCATION_INITIAL_FLUSH_TIME_METRIC_IN_SECONDS = "es.recovery.shard.primary.relocation.initial_flush.time";
    public static final String RELOCATION_ACQUIRE_PERMITS_TIME_METRIC_IN_SECONDS =
        "es.recovery.shard.primary.relocation.acquire_permits.time";
    public static final String RELOCATION_SECOND_FLUSH_TIME_METRIC_IN_SECONDS = "es.recovery.shard.primary.relocation.second_flush.time";
    public static final String RELOCATION_HANDOFF_TIME_METRIC_IN_SECONDS = "es.recovery.shard.primary.relocation.handoff.time";
    public static final String RELOCATION_TARGET_PRE_RECOVERY_TIME_METRIC_IN_SECONDS =
        "es.recovery.shard.primary.relocation.target.pre_recovery.time";
    public static final String RELOCATION_TARGET_READ_INDEXING_SHARD_STATE_TIME_METRIC_IN_SECONDS =
        "es.recovery.shard.primary.relocation.target.read_indexing_shard_state.time";
    public static final String RELOCATION_TARGET_OPEN_ENGINE_TIME_METRIC_IN_SECONDS =
        "es.recovery.shard.primary.relocation.target.open_engine.time";

    public static final StatelessPrimaryRelocationMetricsCollector NOOP = new StatelessPrimaryRelocationMetricsCollector(
        MeterRegistry.NOOP
    );

    private final DoubleHistogram relocationInitialFlushDurationMetric;
    private final DoubleHistogram relocationAcquirePermitsDurationMetric;
    private final DoubleHistogram relocationSecondFlushDurationMetric;
    private final DoubleHistogram relocationHandoffDurationMetric;
    private final DoubleHistogram relocationTargetPreRecoveryDurationMetric;
    private final DoubleHistogram relocationTargetReadIndexingShardStateDurationMetric;
    private final DoubleHistogram relocationTargetOpenEngineDurationMetric;

    public StatelessPrimaryRelocationMetricsCollector(MeterRegistry meterRegistry) {
        relocationInitialFlushDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_INITIAL_FLUSH_TIME_METRIC_IN_SECONDS,
            "Time spent in the initial flush before acquiring all primary operation permits, measured on the source",
            "seconds"
        );
        relocationAcquirePermitsDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_ACQUIRE_PERMITS_TIME_METRIC_IN_SECONDS,
            "Time spent acquiring all primary operation permits during relocation, measured on the source",
            "seconds"
        );
        relocationSecondFlushDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_SECOND_FLUSH_TIME_METRIC_IN_SECONDS,
            "Time spent in the second flush after acquiring permits, measured on the source",
            "seconds"
        );
        relocationHandoffDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_HANDOFF_TIME_METRIC_IN_SECONDS,
            "Round-trip duration of the primary relocation handoff context phase, measured on the source",
            "seconds"
        );
        relocationTargetPreRecoveryDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_TARGET_PRE_RECOVERY_TIME_METRIC_IN_SECONDS,
            "Time spent in IndexShard#preRecovery during primary relocation handoff on the target",
            "seconds"
        );
        relocationTargetReadIndexingShardStateDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_TARGET_READ_INDEXING_SHARD_STATE_TIME_METRIC_IN_SECONDS,
            "Time spent in ObjectStoreService#readIndexingShardState (BCC chain walk) during primary relocation handoff on the target",
            "seconds"
        );
        relocationTargetOpenEngineDurationMetric = meterRegistry.registerDoubleHistogram(
            RELOCATION_TARGET_OPEN_ENGINE_TIME_METRIC_IN_SECONDS,
            "Time spent opening the engine (and activating with primary context) during primary relocation handoff on the target",
            "seconds"
        );
    }

    public void recordRelocationSourceMetrics(RelocationSourceMetrics metrics) {
        relocationInitialFlushDurationMetric.record(metrics.initialFlushDurationInMillis() / 1000.0);
        relocationAcquirePermitsDurationMetric.record(metrics.acquirePermitsDurationInMillis() / 1000.0);
        relocationSecondFlushDurationMetric.record(metrics.secondFlushDurationInMillis() / 1000.0);
        relocationHandoffDurationMetric.record(metrics.handoffDurationInMillis() / 1000.0);
    }

    public void recordRelocationTargetPreRecoveryDuration(long durationInMillis) {
        relocationTargetPreRecoveryDurationMetric.record(durationInMillis / 1000.0);
    }

    public void recordRelocationTargetReadIndexingShardStateDuration(long durationInMillis) {
        relocationTargetReadIndexingShardStateDurationMetric.record(durationInMillis / 1000.0);
    }

    public void recordRelocationTargetOpenEngineDuration(long durationInMillis) {
        relocationTargetOpenEngineDurationMetric.record(durationInMillis / 1000.0);
    }
}
