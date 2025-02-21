/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

final class GoogleCloudStorageOperationsStats {

    private static final int OPERATION_PURPOSE_NUM = OperationPurpose.values().length;
    private static final int OPERATION_NUM = Operation.values().length;
    private final OperationCounters[] counters;
    private final String[] counterNames;
    private final String bucketName;

    GoogleCloudStorageOperationsStats(String bucketName) {
        this.bucketName = bucketName;
        counters = new OperationCounters[OPERATION_PURPOSE_NUM * OPERATION_NUM];
        counterNames = new String[counters.length];
        for (int counterIndex = 0; counterIndex < counters.length; counterIndex++) {
            counterNames[counterIndex] = counterName(counterIndex);
            counters[counterIndex] = new OperationCounters(new LongAdder(), new LongAdder());
        }
    }

    private static String counterName(int counterIndex) {
        int purposeOrd = counterIndex / OPERATION_NUM;
        String purpose = OperationPurpose.values()[purposeOrd].name();
        int operationOrd = counterIndex - (purposeOrd * OPERATION_NUM);
        String operation = Operation.values()[operationOrd].name();
        return purpose + "_" + operation;
    }

    private OperationCounters operationCounters(OperationPurpose purpose, Operation operation) {
        return counters[purpose.ordinal() * OPERATION_NUM + operation.ordinal()];
    }

    void trackOperation(OperationPurpose purpose, Operation operation) {
        operationCounters(purpose, operation).operations.add(1);
    }

    void trackRequest(OperationPurpose purpose, Operation operation) {
        operationCounters(purpose, operation).requests.add(1);
    }

    String bucketName() {
        return bucketName;
    }

    Map<String, BlobStoreActionStats> toMap() {
        var results = new HashMap<String, BlobStoreActionStats>(counterNames.length);
        for (var counterIndex = 0; counterIndex < counterNames.length; counterIndex++) {
            var stats = counters[counterIndex];
            var operations = stats.operations.sum();
            var requests = stats.requests.sum();
            results.put(counterNames[counterIndex], new BlobStoreActionStats(operations, requests));
        }
        return results;
    }

    public enum Operation {
        GET("Get"),
        LIST("List"),
        PUT("Put"),
        POST("Post");

        final String name;

        Operation(String name) {
            this.name = name;
        }
    }

    record OperationCounters(LongAdder operations, LongAdder requests) {}
}
