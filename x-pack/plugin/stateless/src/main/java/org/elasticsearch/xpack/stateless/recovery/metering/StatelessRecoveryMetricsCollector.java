/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.metering;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;

import java.util.Map;

/// Collects stateless-specific recovery metrics (object store bytes, relocation phases).
/// General-purpose recovery metrics are emitted by [org.elasticsearch.indices.recovery.RecoveryMetricsCollector].
public class StatelessRecoveryMetricsCollector implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessRecoveryMetricsCollector.class);

    public static final String RECOVERY_BYTES_READ_FROM_OBJECT_STORE_METRIC = "es.recovery.shard.object_store.bytes_read.total";
    public static final String RECOVERY_BYTES_WARMED_FROM_OBJECT_STORE_METRIC = "es.recovery.shard.object_store.bytes_warmed.total";

    private final LongCounter shardRecoveryTotalBytesReadFromObjectStoreMetric;
    private final LongCounter shardRecoveryTotalBytesWarmedFromObjectStoreMetric;

    public StatelessRecoveryMetricsCollector(MeterRegistry meterRegistry) {
        shardRecoveryTotalBytesReadFromObjectStoreMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_READ_FROM_OBJECT_STORE_METRIC,
            "Bytes read from object store during the shard recovery",
            "bytes"
        );
        shardRecoveryTotalBytesWarmedFromObjectStoreMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_WARMED_FROM_OBJECT_STORE_METRIC,
            "Bytes warmed from object store during the shard recovery",
            "bytes"
        );
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        try {
            if (indexShard.state() == IndexShardState.RECOVERING) {
                final RecoveryState recoveryState = indexShard.recoveryState();
                assert recoveryState != null;
                if (recoveryState.getStage() == RecoveryState.Stage.DONE) {
                    // TODO: ideally read/warmed metrics should be emitted right after corresponding operation is finished (ES-8709)
                    updateMetrics(indexShard, indexShard.store(), recoveryMetricLabels(indexShard));
                }
            }
        } catch (Exception e) {
            logger.warn("Unexpected error during pushing stateless index recovery metrics", e);
        } finally {
            listener.onResponse(null);
        }
    }

    protected void updateMetrics(final IndexShard indexShard, final Store store, final Map<String, Object> metricLabels) {
        final var blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(store.directory());
        shardRecoveryTotalBytesReadFromObjectStoreMetric.incrementBy(blobStoreCacheDirectory.totalBytesReadFromObjectStore(), metricLabels);
        shardRecoveryTotalBytesWarmedFromObjectStoreMetric.incrementBy(
            blobStoreCacheDirectory.totalBytesWarmedFromObjectStore(),
            metricLabels
        );
    }

    private static Map<String, Object> recoveryMetricLabels(IndexShard indexShard) {
        return Map.of(
            "es_is_primary",
            indexShard.routingEntry().primary(),
            "es_recovery_type",
            indexShard.recoveryState().getRecoverySource().getType().name()
        );
    }
}
