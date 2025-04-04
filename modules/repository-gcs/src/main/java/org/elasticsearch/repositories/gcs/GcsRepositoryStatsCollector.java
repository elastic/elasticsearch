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
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Stats collector class that performs metrics initialization and propagation through GCS client
 * calls. This class encapsulates ThreadLocal metrics access.
 */
public class GcsRepositoryStatsCollector {

    static final TimeProvider NOOP_TIMER = new TimeProvider() {
        @Override
        public long relativeTimeInMillis() {
            return 0;
        }

        @Override
        public long relativeTimeInNanos() {
            return 0;
        }

        @Override
        public long rawRelativeTimeInMillis() {
            return 0;
        }

        @Override
        public long absoluteTimeInMillis() {
            return 0;
        }
    };

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
        stats.isLastReqSuccess = true;
        if (((code >= 200 && code < 300) || code == 308 || code == 404) == false) {
            stats.reqErr += 1;
            stats.isLastReqSuccess = false;
            switch (code) {
                case 416 -> stats.reqErrRange += 1;
                case 429 -> stats.reqErrThrottle += 1;
            }
        }
    };

    /**
     * Track operations for billing and REST API
     */
    private final EnumMap<OperationPurpose, EnumMap<StorageOperation, Collector>> collectors;

    /**
     * Telemetry (APM)
     */
    private final RepositoriesMetrics telemetry;
    private final EnumMap<OperationPurpose, EnumMap<StorageOperation, Map<String, Object>>> telemetryAttributes;
    private final TimeProvider timer;

    GcsRepositoryStatsCollector() {
        this(NOOP_TIMER, new RepositoryMetadata(GoogleCloudStorageRepository.TYPE, "", Settings.EMPTY), RepositoriesMetrics.NOOP);
    }

    GcsRepositoryStatsCollector(TimeProvider timer, RepositoryMetadata metadata, RepositoriesMetrics repositoriesMetrics) {
        this.timer = timer;
        this.telemetry = repositoriesMetrics;
        this.collectors = new EnumMap<>(OperationPurpose.class);
        for (var purpose : OperationPurpose.values()) {
            var operationsMap = new EnumMap<StorageOperation, Collector>(StorageOperation.class);
            for (var op : StorageOperation.values()) {
                operationsMap.put(op, new Collector(new LongAdder(), new LongAdder()));
            }
            collectors.put(purpose, operationsMap);
        }
        this.telemetryAttributes = new EnumMap<>(OperationPurpose.class);
        for (var purpose : OperationPurpose.values()) {
            var purposeMap = new EnumMap<StorageOperation, Map<String, Object>>(StorageOperation.class);
            telemetryAttributes.put(purpose, purposeMap);
            for (var operation : StorageOperation.values()) {
                var attrMap = RepositoriesMetrics.createAttributesMap(metadata, purpose, operation.key);
                purposeMap.put(operation, attrMap);
            }
        }
    }

    private static OperationStats initAndGetThreadLocal(OperationPurpose purpose, StorageOperation operation) {
        assert OPERATION_STATS.get() == null : "cannot init stats, thread local is not empty";
        var stats = new OperationStats(purpose, operation);
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
        var t = timer.absoluteTimeInMillis();
        try {
            return blobFn.get();
        } finally {
            stats.totalDuration += timer.absoluteTimeInMillis() - t;
            clearThreadLocal();
        }
    }

    /**
     * Final step in continual collection
     */
    public void finishRunnable(OperationStats stats, IORunnable runnable) throws IOException {
        setThreadLocal(stats);
        var t = timer.absoluteTimeInMillis();
        try {
            runnable.run();
        } finally {
            stats.totalDuration += timer.absoluteTimeInMillis() - t;
            collect(stats);
            clearThreadLocal();
        }
    }

    /**
     * Continue collecting metrics with given OperationStats. Useful for readers and writers.
     */
    public void collectIORunnable(OperationPurpose purpose, StorageOperation operation, IORunnable runnable) throws IOException {
        var stats = initAndGetThreadLocal(purpose, operation);
        var t = timer.absoluteTimeInMillis();
        try {
            runnable.run();
        } finally {
            stats.totalDuration += timer.absoluteTimeInMillis() - t;
            clearThreadLocal();
        }
    }

    public void collectRunnable(OperationPurpose purpose, StorageOperation operation, Runnable runnable) {
        var t = timer.absoluteTimeInMillis();
        var stats = initAndGetThreadLocal(purpose, operation);
        try {
            runnable.run();
        } finally {
            stats.totalDuration += timer.absoluteTimeInMillis() - t;
            collect(stats);
            clearThreadLocal();
        }
    }

    /**
     * Executes GCS Storage operation in a wrapper that stores metrics in ThreadLocal
     */
    public <T> T collectIOSupplier(OperationPurpose purpose, StorageOperation operation, IOSupplier<T> blobFn) throws IOException {
        var t = timer.absoluteTimeInMillis();
        var stats = initAndGetThreadLocal(purpose, operation);
        try {
            return blobFn.get();
        } finally {
            stats.totalDuration += timer.absoluteTimeInMillis() - t;
            collect(stats);
            clearThreadLocal();
        }
    }

    /**
     * Executes GCS Storage operation in a wrapper that stores metrics in ThreadLocal
     */
    public <T> T collectSupplier(OperationPurpose purpose, StorageOperation operation, Supplier<T> blobFn) {
        var t = timer.absoluteTimeInMillis();
        var stats = initAndGetThreadLocal(purpose, operation);
        try {
            return blobFn.get();
        } finally {
            stats.totalDuration += timer.absoluteTimeInMillis() - t;
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
        var operationSuccess = stats.isLastReqSuccess ? 1 : 0;
        var operationErr = stats.isLastReqSuccess ? 0 : 1;
        var collector = collectors.get(purpose).get(operation);
        assert collector != null;
        collector.operations.add(operationSuccess);
        collector.requests.add(stats.reqAtt);

        var attr = telemetryAttributes.get(purpose).get(operation);
        assert attr != null;
        telemetry.operationCounter().incrementBy(operationSuccess, attr);
        telemetry.unsuccessfulOperationCounter().incrementBy(operationErr, attr);
        telemetry.requestCounter().incrementBy(stats.reqAtt, attr);
        telemetry.exceptionCounter().incrementBy(stats.reqErr, attr);
        telemetry.exceptionHistogram().record(stats.reqErr, attr);
        telemetry.throttleCounter().incrementBy(stats.reqErrThrottle, attr);
        telemetry.throttleHistogram().record(stats.reqErrThrottle, attr);
        telemetry.requestRangeNotSatisfiedExceptionCounter().incrementBy(stats.reqErrRange, attr);
        telemetry.httpRequestTimeInMillisHistogram().record(stats.totalDuration, attr);
    }

    public Map<String, BlobStoreActionStats> operationsStats(boolean isServerless) {
        var out = new HashMap<String, BlobStoreActionStats>();
        // Map<'Purpose_Operation', <operations, requests>
        for (var purposeCollector : collectors.entrySet()) {
            for (var operationCollector : collectors.get(purposeCollector.getKey()).entrySet()) {
                var collector = operationCollector.getValue();
                var operations = collector.operations.sum();
                var requests = collector.requests.sum();
                if (isServerless) {
                    // Map<'Purpose_Operation', <operations, requests>
                    out.put(
                        purposeCollector.getKey().getKey() + "_" + operationCollector.getKey().key(),
                        new BlobStoreActionStats(operations, requests)
                    );
                } else {
                    // merge all purposes into Map<'Operation', <operations, requests>
                    out.compute(operationCollector.getKey().key(), (k, v) -> {
                        if (v == null) {
                            return new BlobStoreActionStats(operations, requests);
                        } else {
                            return v.add(new BlobStoreActionStats(operations, requests));
                        }
                    });
                }
            }
        }
        return out;
    }

    record Collector(LongAdder operations, LongAdder requests) {}
}
