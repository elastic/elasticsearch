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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

public class RecoveryMetricsCollector implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RecoveryMetricsCollector.class);
    public static final String RECOVERY_TOTAL_TIME_METRIC = "es.recovery.shard.total.time";

    private final LongHistogram shardRecoveryTotalTimeMetric;

    public RecoveryMetricsCollector(TelemetryProvider telemetryProvider) {
        final MeterRegistry meterRegistry = telemetryProvider.getMeterRegistry();
        shardRecoveryTotalTimeMetric = meterRegistry.registerLongHistogram(
            RECOVERY_TOTAL_TIME_METRIC,
            "Elapsed shard recovery time in millis",
            "milliseconds"
        );
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        try {
            if (indexShard.state() == IndexShardState.POST_RECOVERY) {
                RecoveryState recoveryState = indexShard.recoveryState();
                assert recoveryState != null;
                if (recoveryState.getStage() == RecoveryState.Stage.DONE) {
                    final Map<String, Object> metricLabels = commonMetricLabels(indexShard);
                    shardRecoveryTotalTimeMetric.record(recoveryState.getTimer().time(), metricLabels);
                }
            }
        } catch (Exception e) {
            logger.warn("Unexpected error during pushing index recovery metrics", e);
        } finally {
            listener.onResponse(null);
        }
    }

    private static Map<String, Object> commonMetricLabels(IndexShard indexShard) {
        return Map.of(
            "indexName",
            indexShard.shardId().getIndex().getName(),
            "indexUuid",
            indexShard.shardId().getIndex().getUUID(),
            "shardId",
            indexShard.shardId().id(),
            "primary",
            indexShard.routingEntry().primary(),
            "allocationId",
            indexShard.routingEntry().allocationId() != null ? indexShard.routingEntry().allocationId().getId() : "unknown"
        );
    }

}
