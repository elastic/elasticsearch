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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

final class GoogleCloudStorageOperationsStats {

    /**
     * Every operation purpose and operation has a set of counters.
     * Represented by {@code Map<Purpose,Map<Operation,Counters>>}
     * <pre>
     * {@code
     * | Purpose      | Operation   | OperationsCnt |  RequestCnt |
     * |--------------+-------------+---------------+-------------|
     * | SnapshotData | GetObject   |            10 |          10 |
     * | SnapshotData | ListObjects |            20 | 21(1 retry) |
     * | SnapshotData | ...         |               |             |
     * | Translog     | GetObject   |             5 |           5 |
     * | ...          |             |               |             |
     * }
     * </pre>
     */
    private final EnumMap<OperationPurpose, EnumMap<Operation, Counters>> counters;
    private final String bucketName;

    GoogleCloudStorageOperationsStats(String bucketName) {
        this.bucketName = bucketName;
        this.counters = new EnumMap<>(OperationPurpose.class);
        for (var purpose : OperationPurpose.values()) {
            var operations = new EnumMap<Operation, Counters>(Operation.class);
            for (var operation : Operation.values()) {
                operations.put(operation, new Counters(purpose, operation));
            }
            counters.put(purpose, operations);
        }
    }

    public String bucketName() {
        return bucketName;
    }

    void trackOperation(OperationPurpose purpose, Operation operation) {
        counters.get(purpose).get(operation).operations.add(1);
    }

    void trackRequest(OperationPurpose purpose, Operation operation) {
        counters.get(purpose).get(operation).requests.add(1);
    }

    void trackRequestAndOperation(OperationPurpose purpose, Operation operation) {
        var c = counters.get(purpose).get(operation);
        c.requests.add(1);
        c.operations.add(1);
    }

    Map<String, BlobStoreActionStats> toMap() {
        return counters.values()
            .stream()
            .flatMap(ops -> ops.values().stream())
            .collect(Collectors.toUnmodifiableMap(Counters::name, (c) -> new BlobStoreActionStats(c.operations.sum(), c.requests.sum())));
    }

    public enum Operation {
        GET_METADATA("GetMetadata"),
        GET_OBJECT("GetObject"),
        LIST_OBJECTS("ListObjects"),
        MULTIPART_UPLOAD("MultipartUpload"),
        RESUMABLE_UPLOAD("ResumableUpload");

        final String name;

        Operation(String name) {
            this.name = name;
        }
    }

    private record Counters(String name, LongAdder operations, LongAdder requests) {
        Counters(OperationPurpose purpose, Operation operation) {
            this(purpose.name() + '_' + operation.name(), new LongAdder(), new LongAdder());
        }
    }
}
