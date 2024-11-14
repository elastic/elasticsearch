/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery.metering;

import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

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
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

public class RecoveryMetricsCollector implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RecoveryMetricsCollector.class);

    public static final String RECOVERY_TOTAL_COUNT_METRIC = "es.recovery.shard.count.total";
    public static final String RECOVERY_TOTAL_TIME_METRIC = "es.recovery.shard.total.time";
    public static final String RECOVERY_INDEX_TIME_METRIC = "es.recovery.shard.index.time";
    public static final String RECOVERY_TRANSLOG_TIME_METRIC = "es.recovery.shard.translog.time";
    public static final String RECOVERY_BYTES_READ_FROM_INDEXING_METRIC = "es.recovery.shard.indexing_node.bytes_read.total";
    public static final String RECOVERY_BYTES_READ_FROM_OBJECT_STORE_METRIC = "es.recovery.shard.object_store.bytes_read.total";
    public static final String RECOVERY_BYTES_WARMED_FROM_INDEXING_METRIC = "es.recovery.shard.indexing_node.bytes_warmed.total";
    public static final String RECOVERY_BYTES_WARMED_FROM_OBJECT_STORE_METRIC = "es.recovery.shard.object_store.bytes_warmed.total";

    private final LongCounter shardRecoveryTotalMetric;
    private final LongHistogram shardRecoveryTotalTimeMetric;
    private final LongHistogram shardRecoveryIndexTimeMetric;
    private final LongHistogram shardRecoveryTranslogTimeMetric;
    private final LongCounter shardRecoveryTotalBytesReadFromIndexingMetric;
    private final LongCounter shardRecoveryTotalBytesReadFromObjectStoreMetric;
    private final LongCounter shardRecoveryTotalBytesWarmedFromIndexingMetric;
    private final LongCounter shardRecoveryTotalBytesWarmedFromObjectStoreMetric;

    public RecoveryMetricsCollector(TelemetryProvider telemetryProvider) {
        final MeterRegistry meterRegistry = telemetryProvider.getMeterRegistry();
        shardRecoveryTotalMetric = meterRegistry.registerLongCounter(
            RECOVERY_TOTAL_COUNT_METRIC,
            "Number of times shard recovery has happened",
            "unit"
        );
        shardRecoveryTotalTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_TOTAL_TIME_METRIC,
            "Total elapsed shard recovery time in millis",
            "milliseconds"
        );
        shardRecoveryIndexTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_INDEX_TIME_METRIC,
            "Elapsed shard index (stage) recovery time in millis",
            "milliseconds"
        );
        shardRecoveryTranslogTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_TRANSLOG_TIME_METRIC,
            "Elapsed shard translog (stage) recovery time in millis",
            "milliseconds"
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
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        try {
            if (indexShard.state() == IndexShardState.POST_RECOVERY) {
                final RecoveryState recoveryState = indexShard.recoveryState();
                assert recoveryState != null;
                if (recoveryState.getStage() == RecoveryState.Stage.DONE) {
                    shardRecoveryTotalMetric.increment();
                    final Map<String, Object> metricLabels = recoveryMetricLabels(indexShard);
                    shardRecoveryTotalTimeMetric.record(recoveryState.getTimer().time(), metricLabels);
                    shardRecoveryIndexTimeMetric.record(recoveryState.getIndex().time(), metricLabels);
                    shardRecoveryTranslogTimeMetric.record(recoveryState.getTranslog().time(), metricLabels);

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
