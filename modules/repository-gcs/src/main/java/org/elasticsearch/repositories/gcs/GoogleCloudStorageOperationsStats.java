/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

final class GoogleCloudStorageOperationsStats {

    private final String bucket;

    /**
     * Track operations only, required for REST repository metering API
     */
    private final EnumMap<Operation, LongAdder> operations;

    /**
     * Telemetry (APM)
     */
    private final RepositoriesMetrics telemetry;
    private final EnumMap<OperationPurpose, EnumMap<Operation, Map<String, Object>>> telemetryAttributes;

    GoogleCloudStorageOperationsStats(String bucket, RepositoryMetadata metadata, RepositoriesMetrics repositoriesMetrics) {
        this.bucket = bucket;
        this.telemetry = repositoriesMetrics;
        this.operations = new EnumMap<>(Operation.class);
        for (var op : Operation.values()) {
            operations.put(op, new LongAdder());
        }
        this.telemetryAttributes = new EnumMap<>(OperationPurpose.class);
        for (var purpose : OperationPurpose.values()) {
            var purposeMap = new EnumMap<Operation, Map<String, Object>>(Operation.class);
            telemetryAttributes.put(purpose, purposeMap);
            for (var operation : Operation.values()) {
                var attrMap = RepositoriesMetrics.createAttributesMap(metadata, purpose, operation.key);
            }

        }
    }

    String bucket() {
        return bucket;
    }

    /**
     * Increment counter by 1 for given:
     * @param purpose {@link OperationPurpose}
     * @param operation {@link Operation}
     * @param counter {@link Counter}
     */
    void inc(OperationPurpose purpose, Operation operation, Counter counter) {
        var attr = telemetryAttributes.get(purpose).get(operation);
        switch (counter) {
            case REQUEST -> telemetry.requestCounter().incrementBy(1, attr);
            case REQUEST_EXCEPTION -> {
                telemetry.exceptionCounter().incrementBy(1, attr);
                telemetry.exceptionHistogram().record(1, attr);
            }
            case RANGE_NOT_SATISFIED -> telemetry.requestRangeNotSatisfiedExceptionCounter().incrementBy(1, attr);
            case THROTTLE -> {
                telemetry.throttleCounter().incrementBy(1, attr);
                telemetry.throttleHistogram().record(1, attr);
            }
            case OPERATION -> {
                telemetry.operationCounter().incrementBy(1, attr);
                operations.get(operation).add(1);
            }
            case OPERATION_EXCEPTION -> telemetry.unsuccessfulOperationCounter().incrementBy(1, attr);
        }
    }

    public Map<String, BlobStoreActionStats> toMap() {
        return operations.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().key, e -> {
            var ops = e.getValue().sum();
            return new BlobStoreActionStats(ops, ops);
        }));
    }

    // void recordExceptionHistogram(OperationPurpose purpose, Operation operation, )

    /**
     * enumerates telemetry counters
     */
    enum Counter {
        REQUEST,
        REQUEST_EXCEPTION,
        RANGE_NOT_SATISFIED,
        THROTTLE,
        OPERATION,
        OPERATION_EXCEPTION
    }

    enum Operation {
        GET_OBJECT("GetObject"),
        LIST_OBJECTS("ListObjects"),
        INSERT_OBJECT("InsertObject");

        private final String key;

        Operation(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }
}
