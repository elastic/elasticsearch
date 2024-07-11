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

package co.elastic.elasticsearch.stateless.engine.translog;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class TranslogRecoveryMetrics {

    public static final String TRANSLOG_REPLAY_TIME_METRIC = "es.recovery.translog.replay.time";
    public static final String TRANSLOG_OPERATIONS_TOTAL_METRIC = "es.recovery.translog.operations.total";
    public static final String TRANSLOG_OPERATIONS_SIZE_METRIC = "es.recovery.translog.operations.size";
    public static final String TRANSLOG_FILES_TOTAL_METRIC = "es.recovery.translog.files.total";
    public static final String TRANSLOG_FILES_SIZE_METRIC = "es.recovery.translog.files.size";
    public static final String TRANSLOG_FILES_NETWORK_TIME_METRIC = "es.recovery.translog.network.time";

    public static TranslogRecoveryMetrics NOOP = new TranslogRecoveryMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    private final LongHistogram translogReplayTime;
    private final LongCounter translogOperationsTotal;
    private final LongCounter translogOperationsSizeInBytes;
    private final LongCounter translogFilesTotal;
    private final LongCounter translogFilesSize;
    private final LongHistogram translogFilesNetworkTime;

    public TranslogRecoveryMetrics(final MeterRegistry meterRegistry) {

        translogReplayTime = meterRegistry.registerLongHistogram(
            TRANSLOG_REPLAY_TIME_METRIC,
            "Total time spent on reading translog files during shard recovery",
            "nanoseconds"
        );

        translogOperationsTotal = meterRegistry.registerLongCounter(
            TRANSLOG_OPERATIONS_TOTAL_METRIC,
            "Number of operations recovered from translog. Incremented for each operation type (index, delete, noop). "
                + "Filterable by `translog_op_type` metric attribute",
            "count"
        );

        translogOperationsSizeInBytes = meterRegistry.registerLongCounter(
            TRANSLOG_OPERATIONS_SIZE_METRIC,
            "Size in bytes of operations recovered from translog. Incremented for each operation type (index, delete, noop). "
                + "Filterable by `translog_op_type` metric attribute",
            "bytes"
        );

        translogFilesTotal = meterRegistry.registerLongCounter(
            TRANSLOG_FILES_TOTAL_METRIC,
            "Number of translog files read from object store. Incremented for `referenced` and `unreferenced` blobs. "
                + "Filterable by `translog_blob_type` metric attribute",
            "count"
        );

        translogFilesSize = meterRegistry.registerLongCounter(
            TRANSLOG_FILES_SIZE_METRIC,
            "Size in bytes of translog files used during shard recovery. Incremented for `referenced` and `unreferenced` blobs. "
                + "Filterable by `translog_blob_type` metric attribute",
            "bytes"
        );

        translogFilesNetworkTime = meterRegistry.registerLongHistogram(
            TRANSLOG_FILES_NETWORK_TIME_METRIC,
            "Total time spent on fetching translog files over network",
            "nanoseconds"
        );

    }

    public LongHistogram getTranslogReplayTimeHistogram() {
        return translogReplayTime;
    }

    public LongCounter getTranslogOperationsTotalCounter() {
        return translogOperationsTotal;
    }

    public LongCounter getTranslogOperationsSizeCounter() {
        return translogOperationsSizeInBytes;
    }

    public LongCounter getTranslogFilesTotalCounter() {
        return translogFilesTotal;
    }

    public LongCounter getTranslogFilesSizeCounter() {
        return translogFilesSize;
    }

    public LongHistogram getTranslogFilesNetworkTimeHistogram() {
        return translogFilesNetworkTime;
    }

}
