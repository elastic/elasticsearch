/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpResponseInterceptor;

import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Stats collector class that performs metrics initialization and propagation through GCS client
 * calls. This class encapsulates ThreadLocal metrics access.
 */
public class GcsRepositoryStatsCollector {

    private static final ThreadLocal<OperationStats> OPERATION_STATS = new ThreadLocal<>();

    /**
     * All 2xx and 308 codes are success. 404 is expected on blob existence check.
     * Also, 404 is billable error. Other errors 307, 4xx, and 5xx are not billable.
     * <a href="https://cloud.google.com/storage/pricing#price-tables">GCS pricing tables</a>.
     */
    public static final HttpResponseInterceptor METERING_INTERCEPTOR = response -> {
        var stats = getThreadLocal();
        var code = response.getStatusCode();
        stats.reqAtt += 1;
        if (((code >= 200 && code < 300) || code == 308) == false) {
            stats.reqErr += 1;
            switch (code) {
                case 404 -> stats.reqBillableErr += 1;
                case 416 -> stats.reqErrRange += 1;
                case 429 -> stats.reqErrThrottle += 1;
            }
        }
    };
    /**
     * Track operations for billing and REST API
     */
    private final EnumMap<OperationPurpose, EnumMap<StorageOperation, OpsCollector>> restMetering;
    /**
     * Telemetry (APM)
     */
    private final RepositoriesMetrics telemetry;
    private final EnumMap<OperationPurpose, EnumMap<StorageOperation, Map<String, Object>>> telemetryAttributes;
    /**
     * track request duration
     */
    private final LongSupplier timer;

    GcsRepositoryStatsCollector() {
        this(() -> 0L, new RepositoryMetadata(GoogleCloudStorageRepository.TYPE, "", Settings.EMPTY), RepositoriesMetrics.NOOP);
    }

    GcsRepositoryStatsCollector(LongSupplier timer, RepositoryMetadata metadata, RepositoriesMetrics repositoriesMetrics) {
        this.timer = timer;
        this.telemetry = repositoriesMetrics;
        this.restMetering = new EnumMap<>(OperationPurpose.class);
        for (var purpose : OperationPurpose.values()) {
            var operationsMap = new EnumMap<StorageOperation, OpsCollector>(StorageOperation.class);
            for (var op : StorageOperation.values()) {
                operationsMap.put(op, new OpsCollector(new LongAdder(), new LongAdder()));
            }
            restMetering.put(purpose, operationsMap);
        }
        this.telemetryAttributes = new EnumMap<>(OperationPurpose.class);
        if (repositoriesMetrics != RepositoriesMetrics.NOOP) {
            for (var purpose : OperationPurpose.values()) {
                var purposeMap = new EnumMap<StorageOperation, Map<String, Object>>(StorageOperation.class);
                telemetryAttributes.put(purpose, purposeMap);
                for (var operation : StorageOperation.values()) {
                    var attrMap = RepositoriesMetrics.createAttributesMap(metadata, purpose, operation.key);
                    purposeMap.put(operation, attrMap);
                }
            }
        }
    }

    private static OperationStats initAndGetThreadLocal(OperationPurpose purpose, StorageOperation operation, long time) {
        assert OPERATION_STATS.get() == null : "cannot init stats, thread local is not empty";
        var stats = new OperationStats(purpose, operation);
        stats.startTimeMs = time;
        OPERATION_STATS.set(stats);
        return stats;
    }

    static OperationStats getThreadLocal() {
        var stats = OPERATION_STATS.get();
        assert stats != null : "must initialize operation stats";
        return stats;
    }

    private static void setThreadLocal(OperationStats stats) {
        assert OPERATION_STATS.get() == null : "cannot set stats, thread local is not empty";
        OPERATION_STATS.set(stats);
    }

    private static void clearThreadLocal() {
        assert OPERATION_STATS.get() != null : "cannot clear already emptied thread local";
        OPERATION_STATS.remove();
    }

    /**
     * Continue collecting metrics with given OperationStats. Useful for readers and writers.
     */
    public <T> T continueWithStats(OperationStats stats, IOSupplier<T> blobFn) throws IOException {
        setThreadLocal(stats);
        try {
            return blobFn.get();
        } finally {
            clearThreadLocal();
        }
    }

    /**
     * Continue collecting metrics with given OperationStats. Useful for readers and writers.
     */
    public void continueWithStats(OperationStats stats, IORunnable runnable) throws IOException {
        setThreadLocal(stats);
        try {
            runnable.run();
        } finally {
            clearThreadLocal();
        }
    }

    /**
     * Final step in continual collection
     */
    public void finishAndCollect(OperationStats stats, IORunnable runnable) throws IOException {
        setThreadLocal(stats);
        try {
            runnable.run();
            stats.isSuccess = true;
        } finally {
            collect(stats);
            clearThreadLocal();
        }
    }

    /**
     * Final step in continual collection
     */
    public void finishAndCollect(OperationStats stats, Runnable runnable) {
        setThreadLocal(stats);
        try {
            runnable.run();
            stats.isSuccess = true;
        } finally {
            collect(stats);
            clearThreadLocal();
        }
    }

    /**
     * Executes GCS Storage operation in a wrapper that stores metrics in ThreadLocal
     */
    public <T> T runAndCollect(OperationPurpose purpose, StorageOperation operation, IOSupplier<T> blobFn) throws IOException {
        var stats = initAndGetThreadLocal(purpose, operation, timer.getAsLong());
        try {
            var result = blobFn.get();
            stats.isSuccess = true;
            return result;
        } finally {
            collect(stats);
            clearThreadLocal();

        }
    }

    /**
     * Executes GCS Storage operation in a wrapper that stores metrics in ThreadLocal
     */
    public <T> T runAndCollectUnchecked(OperationPurpose purpose, StorageOperation operation, Supplier<T> supplier) {
        var stats = initAndGetThreadLocal(purpose, operation, timer.getAsLong());
        try {
            var result = supplier.get();
            stats.isSuccess = true;
            return result;
        } finally {
            collect(stats);
            clearThreadLocal();
        }
    }

    private void collect(OperationStats stats) {
        if (stats.reqAtt == 0) {
            return; // nothing happened
        }
        var purpose = stats.purpose;
        var operation = stats.operation;
        var opOk = 0;
        var opErr = 0;
        switch (operation) {
            case GET, LIST -> {
                opOk = stats.reqAtt - stats.reqErr + stats.reqBillableErr;
                opErr = stats.reqBillableErr;
            }
            case INSERT -> {
                opOk = stats.isSuccess ? 1 : 0;
                opErr = stats.isSuccess ? 0 : 1;
            }
        }
        if (opOk > 0) {
            var opStats = restMetering.get(purpose).get(operation);
            assert opStats != null;
            opStats.operations.add(opOk);
            opStats.requests.add(stats.reqAtt - stats.reqErr + stats.reqBillableErr);
        }

        if (telemetry != RepositoriesMetrics.NOOP) {
            var attr = telemetryAttributes.get(stats.purpose).get(stats.operation);
            assert attr != null;
            telemetry.operationCounter().incrementBy(opOk, attr);
            telemetry.unsuccessfulOperationCounter().incrementBy(opErr, attr);
            telemetry.requestCounter().incrementBy(stats.reqAtt, attr);
            telemetry.exceptionCounter().incrementBy(stats.reqErr, attr);
            telemetry.exceptionHistogram().record(stats.reqErr, attr);
            telemetry.throttleCounter().incrementBy(stats.reqErrThrottle, attr);
            telemetry.throttleHistogram().record(stats.reqErrThrottle, attr);
            telemetry.requestRangeNotSatisfiedExceptionCounter().incrementBy(stats.reqErrRange, attr);
            telemetry.httpRequestTimeInMillisHistogram().record(stats.startTimeMs, attr);
        }
    }

    public Map<String, BlobStoreActionStats> operationsStats(boolean isServerless) {
        var out = new HashMap<String, BlobStoreActionStats>();
        if (isServerless) {
            // Map<'Purpose_Operation', <operations, requests>
            for (var purposeKv : restMetering.entrySet()) {
                for (var operationKv : restMetering.get(purposeKv.getKey()).entrySet()) {
                    var stat = operationKv.getValue();
                    var ops = stat.operations.sum();
                    var req = stat.requests.sum();
                    out.put(purposeKv.getKey() + "_" + operationKv.getKey(), new BlobStoreActionStats(ops, req));
                }
            }
        } else {
            // Map<'Operation', <operations, requests>
            for (var purposeKv : restMetering.entrySet()) {
                for (var operationKv : purposeKv.getValue().entrySet()) {
                    out.compute(operationKv.getKey().key(), (k, v) -> {
                        var stat = operationKv.getValue();
                        var ops = stat.operations.sum();
                        // TODO update map with (ops,req) when azure ready
                        // var req = stat.requests.sum();
                        if (v == null) {
                            return new BlobStoreActionStats(ops, ops);
                        } else {
                            return new BlobStoreActionStats(v.operations() + ops, v.operations() + ops);
                        }
                    });
                }
            }
        }
        return out;
    }

    record OpsCollector(LongAdder operations, LongAdder requests) {}
}
