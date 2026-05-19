/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.metering;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.util.Map;

public class RecoveryMetricsCollector implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RecoveryMetricsCollector.class);

    public static final String RECOVERY_TOTAL_COUNT_METRIC = "es.recovery.shard.count.total";
    public static final String RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS = "es.recovery.shard.total.time";
    public static final String RECOVERY_INDEX_TIME_METRIC_IN_SECONDS = "es.recovery.shard.index.time";
    public static final String RECOVERY_TRANSLOG_TIME_METRIC_IN_SECONDS = "es.recovery.shard.translog.time";
    public static final String RECOVERY_BYTES_READ_FROM_INDEXING_METRIC = "es.recovery.shard.indexing_node.bytes_read.total";
    public static final String RECOVERY_BYTES_READ_FROM_OBJECT_STORE_METRIC = "es.recovery.shard.object_store.bytes_read.total";
    public static final String RECOVERY_BYTES_WARMED_FROM_INDEXING_METRIC = "es.recovery.shard.indexing_node.bytes_warmed.total";
    public static final String RECOVERY_BYTES_WARMED_FROM_OBJECT_STORE_METRIC = "es.recovery.shard.object_store.bytes_warmed.total";

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

    public static final RecoveryMetricsCollector NOOP = new RecoveryMetricsCollector(TelemetryProvider.NOOP);

    private final LongCounter shardRecoveryTotalMetric;
    private final LongHistogram shardRecoveryTotalTimeMetric;
    private final LongHistogram shardRecoveryIndexTimeMetric;
    private final LongHistogram shardRecoveryTranslogTimeMetric;
    private final LongCounter shardRecoveryTotalBytesReadFromIndexingMetric;
    private final LongCounter shardRecoveryTotalBytesReadFromObjectStoreMetric;
    private final LongCounter shardRecoveryTotalBytesWarmedFromIndexingMetric;
    private final LongCounter shardRecoveryTotalBytesWarmedFromObjectStoreMetric;
    private final DoubleHistogram relocationInitialFlushDurationMetric;
    private final DoubleHistogram relocationAcquirePermitsDurationMetric;
    private final DoubleHistogram relocationSecondFlushDurationMetric;
    private final DoubleHistogram relocationHandoffDurationMetric;
    private final DoubleHistogram relocationTargetPreRecoveryDurationMetric;
    private final DoubleHistogram relocationTargetReadIndexingShardStateDurationMetric;
    private final DoubleHistogram relocationTargetOpenEngineDurationMetric;

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
        shardRecoveryTotalBytesReadFromIndexingMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_READ_FROM_INDEXING_METRIC,
            "Bytes read from indexing node during the shard recovery",
            "bytes"
        );
        shardRecoveryTotalBytesReadFromObjectStoreMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_READ_FROM_OBJECT_STORE_METRIC,
            "Bytes read from object store during the shard recovery",
            "bytes"
        );
        shardRecoveryTotalBytesWarmedFromIndexingMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_WARMED_FROM_INDEXING_METRIC,
            "Bytes warmed from indexing node during the shard recovery",
            "bytes"
        );
        shardRecoveryTotalBytesWarmedFromObjectStoreMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_WARMED_FROM_OBJECT_STORE_METRIC,
            "Bytes warmed from object store during the shard recovery",
            "bytes"
        );
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

    public void recordRelocationInitialFlushDuration(long durationInMillis) {
        relocationInitialFlushDurationMetric.record(durationInMillis / 1000.0);
    }

    public void recordRelocationAcquirePermitsDuration(long durationInMillis) {
        relocationAcquirePermitsDurationMetric.record(durationInMillis / 1000.0);
    }

    public void recordRelocationSecondFlushDuration(long durationInMillis) {
        relocationSecondFlushDurationMetric.record(durationInMillis / 1000.0);
    }

    public void recordRelocationHandoffDuration(long durationInMillis) {
        relocationHandoffDurationMetric.record(durationInMillis / 1000.0);
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

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        try {
            if (indexShard.state() == IndexShardState.RECOVERING) {
                final RecoveryState recoveryState = indexShard.recoveryState();
                assert recoveryState != null;
                if (recoveryState.getStage() == RecoveryState.Stage.DONE) {
                    shardRecoveryTotalMetric.increment();
                    final Map<String, Object> metricLabels = recoveryMetricLabels(indexShard);
                    shardRecoveryTotalTimeMetric.record(recoveryState.getTimer().time() / 1000, metricLabels);
                    shardRecoveryIndexTimeMetric.record(recoveryState.getIndex().time() / 1000, metricLabels);
                    shardRecoveryTranslogTimeMetric.record(recoveryState.getTranslog().time() / 1000, metricLabels);

                    final Store store = indexShard.store();
                    // TODO: ideally read/warmed metrics should be emitted right after corresponding operation is finished (ES-8709)
                    if (indexShard.routingEntry().isPromotableToPrimary() == false) {
                        final SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
                        shardRecoveryTotalBytesReadFromIndexingMetric.incrementBy(
                            searchDirectory.totalBytesReadFromIndexing(),
                            metricLabels
                        );
                        shardRecoveryTotalBytesWarmedFromIndexingMetric.incrementBy(
                            searchDirectory.totalBytesWarmedFromIndexing(),
                            metricLabels
                        );
                    }
                    var blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(store.directory());
                    shardRecoveryTotalBytesReadFromObjectStoreMetric.incrementBy(
                        blobStoreCacheDirectory.totalBytesReadFromObjectStore(),
                        metricLabels
                    );
                    shardRecoveryTotalBytesWarmedFromObjectStoreMetric.incrementBy(
                        blobStoreCacheDirectory.totalBytesWarmedFromObjectStore(),
                        metricLabels
                    );

                }
            }
        } catch (Exception e) {
            logger.warn("Unexpected error during pushing index recovery metrics", e);
        } finally {
            listener.onResponse(null);
        }
    }

    private static Map<String, Object> recoveryMetricLabels(IndexShard indexShard) {
        return Maps.copyMapWithAddedEntry(
            commonMetricLabels(indexShard),
            "recovery_type",
            indexShard.recoveryState().getRecoverySource().getType().name()
        );
    }

    public static Map<String, Object> commonMetricLabels(IndexShard indexShard) {
        return Map.of("primary", indexShard.routingEntry().primary());
    }
}
