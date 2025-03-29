/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RepositoryStatsCollector {

    /**
     * Track billable requests only, required for REST repository metering API
     */
    private final EnumMap<StorageOperation, LongAdder> restMetering;

    /**
     * Telemetry (APM)
     */
    private final RepositoriesMetrics telemetry;
    private final EnumMap<OperationPurpose, EnumMap<StorageOperation, Map<String, Object>>> telemetryAttributes;

    RepositoryStatsCollector(String repoName, RepositoriesMetrics repositoriesMetrics) {
        this.telemetry = repositoriesMetrics;
        this.restMetering = new EnumMap<>(StorageOperation.class);
        for (var op : StorageOperation.values()) {
            restMetering.put(op, new LongAdder());
        }
        this.telemetryAttributes = new EnumMap<>(OperationPurpose.class);
        for (var purpose : OperationPurpose.values()) {
            var purposeMap = new EnumMap<StorageOperation, Map<String, Object>>(StorageOperation.class);
            telemetryAttributes.put(purpose, purposeMap);
            for (var operation : StorageOperation.values()) {
                var attrMap = RepositoriesMetrics.createAttributesMap(GoogleCloudStorageRepository.TYPE, repoName, purpose, operation.key);
            }
        }
    }

    /**
     * Continue collecting metrics with given OperationStats. Useful for readers and writers.
     */
    public <T> T continueAndCollect(OperationStats stats, CheckedSupplier<T, IOException> blobFn) throws IOException {
        OperationStats.set(stats);
        return blobFn.get();
    }

    /**
     * Continue collecting metrics with given OperationStats. Useful for readers and writers.
     */
    public void continueAndCollect(OperationStats stats, CheckedRunnable<IOException> runnable) throws IOException {
        OperationStats.set(stats);
        runnable.run();
    }

    /**
     * Final step in continual collection
     */
    public void finishAndCollect(OperationStats stats, CheckedRunnable<IOException> runnable) throws IOException {
        OperationStats.set(stats);
        try {
            runnable.run();
            stats.isSuccess = true;
        } finally {
            collect(stats);
            OperationStats.clear();
        }
    }

    /**
     * Final step in continual collection
     */
    public void finishAndCollect(OperationStats stats, Runnable runnable) {
        OperationStats.set(stats);
        try {
            runnable.run();
            stats.isSuccess = true;
        } finally {
            collect(stats);
            OperationStats.clear();
        }
    }

    /**
     * Executes GCS Storage operation in a wrapper that stores metrics in ThreadLocal
     */
    public <T> T runAndCollect(OperationPurpose purpose, StorageOperation operation, CheckedSupplier<T, IOException> blobFn)
        throws IOException {
        var stats = OperationStats.initAndGet(purpose, operation);
        try {
            var result = blobFn.get();
            stats.isSuccess = true;
            return result;
        } finally {
            collect(stats);
            OperationStats.clear();

        }
    }

    /**
     * Executes GCS Storage operation in a wrapper that stores metrics in ThreadLocal
     */
    public <T> T runAndCollectUnchecked(OperationPurpose purpose, StorageOperation operation, Supplier<T> supplier) {
        var stats = OperationStats.initAndGet(purpose, operation);
        try {
            var result = supplier.get();
            stats.isSuccess = true;
            return result;
        } finally {
            collect(stats);
            OperationStats.clear();
        }
    }

    void collect(OperationStats stats) {
        var op = stats.operation;
        var opOk = 0;
        var opErr = 0;
        switch (op) {
            case GET, LIST -> {
                opOk = stats.reqAtt - stats.reqErr + stats.reqBillableErr;
                opErr = stats.reqBillableErr;
            }
            case INSERT -> {
                opOk = stats.isSuccess ? 1 : 0;
                opErr = opOk > 0 ? 0 : 1;
            }
        }
        restMetering.get(op).add(opOk);
        var attr = telemetryAttributes.get(stats.purpose).get(stats.operation);
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

    public Map<String, BlobStoreActionStats> operationsStats() {
        return restMetering.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().key, e -> {
            var ops = e.getValue().sum();
            return new BlobStoreActionStats(ops, ops);
        }));
    }
}
