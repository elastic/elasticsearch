/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.telemetry.metric.LongAsyncGauge;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Metrics describing the snapshot restores that are in progress, and how they end. Only the elected master reports anything.
 * <p>
 * This follows the model of {@link org.elasticsearch.repositories.SnapshotMetrics}. Counters and histograms are recorded by
 * {@link RestoreService} where each restore's events happen, after the cluster state that carries them is published:
 * {@link #restoreStarted} once the state that installs a restore's {@link RestoreInProgress} entry is published, and
 * {@link #restoreCompleted} once the state that removes the completed entry is published. The outcome of each shard is counted at
 * completion, from the restore's final entry, as {@link SnapshotsService} counts unsuccessful shards when it finalizes a snapshot.
 * <p>
 * The gauges are registered when this component starts. Their values come from {@link CachingRestoreMetricsService}, which computes them
 * from the current cluster state when they are polled, as {@link CachingSnapshotAndShardByStateMetricsService} does for snapshots. The
 * restore UUID and index names are deliberately never used as attributes, because they have unbounded cardinality;
 * sizes per restore are reported as histograms when each restore completes instead.
 */
public class RestoreMetrics extends AbstractLifecycleComponent {

    /** Records nothing, for tests and other callers that do not report metrics. */
    public static final RestoreMetrics NOOP = new RestoreMetrics(MeterRegistry.NOOP, m -> {}, m -> {}, m -> {}, m -> {}, m -> {});

    public static final String RESTORES_CURRENT = "es.repositories.restores.current";
    public static final String RESTORES_INDICES_CURRENT = "es.repositories.restores.indices.current";
    public static final String RESTORES_SHARDS_BY_STATE_CURRENT = "es.repositories.restores.shards.by_state.current";
    public static final String RESTORES_SHARDS_RETRYING_CURRENT = "es.repositories.restores.shards.retrying.current";
    public static final String RESTORES_SHARDS_OLDEST_AGE_CURRENT = "es.repositories.restores.shards.oldest_age.current";
    public static final String RESTORES_STARTED_TOTAL = "es.repositories.restores.started.total";
    public static final String RESTORES_COMPLETED_TOTAL = "es.repositories.restores.completed.total";
    public static final String RESTORES_SHARDS_COMPLETED_TOTAL = "es.repositories.restores.shards.completed.total";
    public static final String RESTORES_SHARDS_FAILED_TOTAL = "es.repositories.restores.shards.failed.total";
    public static final String RESTORES_INDICES_HISTOGRAM = "es.repositories.restores.indices.histogram";
    public static final String RESTORES_SHARDS_HISTOGRAM = "es.repositories.restores.shards.histogram";

    /** Whether requests that hit a restoring shard fail with {@code ShardRestoringException}, which separates API-initiated restores. */
    public static final String REPORT_SHARD_RESTORING_ATTRIBUTE = "es_restore_report_shard_restoring";
    /** How a completed restore ended, one of the {@link Outcome} values. */
    public static final String OUTCOME_ATTRIBUTE = "es_restore_outcome";
    /** Where a shard of a restore in progress is, one of the {@link ShardState} values. */
    public static final String SHARD_STATE_ATTRIBUTE = "es_restore_shard_state";
    /** Why a shard restore failed, a {@link RestoreInProgress.ShardRestoreFailureReason} value or {@link #UNKNOWN_FAILURE_REASON}. */
    public static final String FAILURE_REASON_ATTRIBUTE = "es_restore_shard_failure_reason";
    /** The failure reason of a shard whose failure was not classified, because a node that does not classify failures recorded it. */
    public static final String UNKNOWN_FAILURE_REASON = "unknown";

    /**
     * Boundaries for the per-restore size histograms, the same as those of the unsuccessful shards per snapshot histogram. Zero has its own
     * bucket, the low counts are fine-grained, and the top extends well past the default shard limit per index because a restore can
     * cover many indices.
     */
    private static final List<Long> SIZE_BUCKET_BOUNDARIES = List.of(
        0L,
        1L,
        2L,
        5L,
        10L,
        25L,
        50L,
        100L,
        250L,
        500L,
        1000L,
        2500L,
        5000L,
        10_000L,
        50_000L
    );

    /** How a completed restore ended. */
    public enum Outcome {
        /** Every shard was restored. */
        SUCCESS("success"),
        /** Some shards were restored and some failed. */
        PARTIAL("partial"),
        /** Every shard failed. */
        FAILURE("failure");

        private final String value;

        Outcome(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    /** Where a shard of a restore in progress is. */
    public enum ShardState {
        /** Not yet restored and waiting to be allocated to a node. */
        QUEUED("queued"),
        /** Allocated to a node and recovering from the snapshot. */
        RECOVERING("recovering"),
        /** Restored. */
        SUCCEEDED("succeeded"),
        /** Failed to restore. */
        FAILED("failed");

        private final String value;

        ShardState(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    private final MeterRegistry meterRegistry;
    private final Consumer<LongAsyncMeasurement> restoresObserver;
    private final Consumer<LongAsyncMeasurement> indicesObserver;
    private final Consumer<LongAsyncMeasurement> shardsByStateObserver;
    private final Consumer<LongAsyncMeasurement> shardsRetryingObserver;
    private final Consumer<LongAsyncMeasurement> oldestShardAgeObserver;

    private final LongCounter startedCounter;
    private final LongCounter completedCounter;
    private final LongCounter shardsCompletedCounter;
    private final LongCounter shardsFailedCounter;
    private final LongHistogram indicesHistogram;
    private final LongHistogram shardsHistogram;
    private final List<LongAsyncGauge> asyncGauges = new ArrayList<>();

    public RestoreMetrics(MeterRegistry meterRegistry, CachingRestoreMetricsService cachingRestoreMetricsService) {
        this(
            meterRegistry,
            cachingRestoreMetricsService::recordRestoresCurrent,
            cachingRestoreMetricsService::recordIndicesCurrent,
            cachingRestoreMetricsService::recordShardsByState,
            cachingRestoreMetricsService::recordShardsRetrying,
            cachingRestoreMetricsService::recordOldestShardAge
        );
    }

    public RestoreMetrics(
        MeterRegistry meterRegistry,
        Consumer<LongAsyncMeasurement> restoresObserver,
        Consumer<LongAsyncMeasurement> indicesObserver,
        Consumer<LongAsyncMeasurement> shardsByStateObserver,
        Consumer<LongAsyncMeasurement> shardsRetryingObserver,
        Consumer<LongAsyncMeasurement> oldestShardAgeObserver
    ) {
        this.meterRegistry = meterRegistry;
        this.restoresObserver = restoresObserver;
        this.indicesObserver = indicesObserver;
        this.shardsByStateObserver = shardsByStateObserver;
        this.shardsRetryingObserver = shardsRetryingObserver;
        this.oldestShardAgeObserver = oldestShardAgeObserver;
        this.startedCounter = meterRegistry.registerLongCounter(RESTORES_STARTED_TOTAL, "snapshot restores started", "unit");
        this.completedCounter = meterRegistry.registerLongCounter(
            RESTORES_COMPLETED_TOTAL,
            "snapshot restores completed, by outcome",
            "unit"
        );
        this.shardsCompletedCounter = meterRegistry.registerLongCounter(
            RESTORES_SHARDS_COMPLETED_TOTAL,
            "shards restored from a snapshot, counted when their restore completes",
            "unit"
        );
        this.shardsFailedCounter = meterRegistry.registerLongCounter(
            RESTORES_SHARDS_FAILED_TOTAL,
            "shards that failed to restore from a snapshot, by reason, counted when their restore completes",
            "unit"
        );
        this.indicesHistogram = meterRegistry.registerLongHistogram(
            RESTORES_INDICES_HISTOGRAM,
            "indices per completed snapshot restore",
            "unit",
            SIZE_BUCKET_BOUNDARIES
        );
        this.shardsHistogram = meterRegistry.registerLongHistogram(
            RESTORES_SHARDS_HISTOGRAM,
            "shards per completed snapshot restore",
            "unit",
            SIZE_BUCKET_BOUNDARIES
        );
    }

    @Override
    protected void doStart() {
        asyncGauges.add(meterRegistry.registerLongAsyncGauge(RESTORES_CURRENT, "snapshot restores in progress", "unit", restoresObserver));
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                RESTORES_INDICES_CURRENT,
                "indices being restored from a snapshot",
                "unit",
                indicesObserver
            )
        );
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                RESTORES_SHARDS_BY_STATE_CURRENT,
                "shards of snapshot restores in progress, by state",
                "unit",
                shardsByStateObserver
            )
        );
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                RESTORES_SHARDS_RETRYING_CURRENT,
                "shards still restoring that have failed to recover at least once, and are being retried",
                "unit",
                shardsRetryingObserver
            )
        );
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                RESTORES_SHARDS_OLDEST_AGE_CURRENT,
                "the longest time that any shard still restoring has been waiting for or recovering in its current attempt",
                "ms",
                oldestShardAgeObserver
            )
        );
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        Releasables.close(asyncGauges.stream().map(g -> (Releasable) g::close).toList());
    }

    /**
     * Records that a restore started. Called on the master once the cluster state that installs the restore's entry is published.
     */
    public void restoreStarted(RestoreInProgress.Entry entry) {
        startedCounter.incrementBy(1, Map.of(REPORT_SHARD_RESTORING_ATTRIBUTE, entry.reportShardRestoring()));
    }

    /**
     * Records that a restore completed: its outcome and size, and how each of its shards ended. Called on the master once the cluster state
     * that removes the completed entry is published, so each restore is recorded once even if removing it has to be retried.
     */
    public void restoreCompleted(RestoreInProgress.Entry entry) {
        assert entry.state().completed() : "restore [" + entry.uuid() + "] is not completed: " + entry.state();
        long succeeded = 0;
        final Map<String, Long> failedByReason = new HashMap<>();
        for (ShardRestoreStatus status : entry.shards().values()) {
            switch (status.state()) {
                case SUCCESS -> succeeded++;
                case FAILURE -> failedByReason.merge(failureReason(status), 1L, Long::sum);
                case INIT, STARTED -> {
                    assert false : "shard of completed restore [" + entry.uuid() + "] is still restoring: " + status;
                }
            }
        }
        if (succeeded > 0) {
            shardsCompletedCounter.incrementBy(succeeded);
        }
        failedByReason.forEach((reason, count) -> shardsFailedCounter.incrementBy(count, Map.of(FAILURE_REASON_ATTRIBUTE, reason)));

        final Outcome outcome = outcome(succeeded, entry.shards().size());
        completedCounter.incrementBy(
            1,
            Map.of(REPORT_SHARD_RESTORING_ATTRIBUTE, entry.reportShardRestoring(), OUTCOME_ATTRIBUTE, outcome.value())
        );
        final Map<String, Object> outcomeAttributes = Map.of(OUTCOME_ATTRIBUTE, outcome.value());
        indicesHistogram.record(entry.indices().size(), outcomeAttributes);
        shardsHistogram.record(entry.shards().size(), outcomeAttributes);
    }

    private static String failureReason(ShardRestoreStatus status) {
        return status.failureReason() == null ? UNKNOWN_FAILURE_REASON : status.failureReason().value();
    }

    private static Outcome outcome(long succeeded, int shards) {
        if (succeeded == shards) {
            return Outcome.SUCCESS;
        }
        return succeeded == 0 ? Outcome.FAILURE : Outcome.PARTIAL;
    }

}
