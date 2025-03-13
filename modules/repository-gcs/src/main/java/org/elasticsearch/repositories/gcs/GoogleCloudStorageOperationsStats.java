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

    private final String bucketName;
    private final StatsTracker tracker;

    GoogleCloudStorageOperationsStats(String bucketName, boolean isStateless) {
        this.bucketName = bucketName;
        if (isStateless) {
            this.tracker = new ServerlessTracker(bucketName);
        } else {
            this.tracker = new StatefulTracker();
        }
    }

    GoogleCloudStorageOperationsStats(String bucketName) {
        this(bucketName, false);
    }

    public String bucketName() {
        return bucketName;
    }

    public StatsTracker tracker() {
        return tracker;
    }

    public enum Operation {
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

    sealed interface StatsTracker permits ServerlessTracker, StatefulTracker {
        void trackRequest(OperationPurpose purpose, Operation operation);

        void trackOperation(OperationPurpose purpose, Operation operation);

        Map<String, BlobStoreActionStats> toMap();

        default void trackOperationAndRequest(OperationPurpose purpose, Operation operation) {
            trackOperation(purpose, operation);
            trackRequest(purpose, operation);
        }
    }

    /**
     * Stateful tracker is single dimension: Operation only. The OperationPurpose is ignored.
     */
    static final class StatefulTracker implements StatsTracker {

        private final EnumMap<Operation, Counters> counters;

        StatefulTracker() {
            this.counters = new EnumMap<>(Operation.class);
            for (var operation : Operation.values()) {
                counters.put(operation, new Counters(operation.key()));
            }
        }

        @Override
        public void trackRequest(OperationPurpose purpose, Operation operation) {
            // dont track requests, only operations
        }

        @Override
        public void trackOperation(OperationPurpose purpose, Operation operation) {
            counters.get(operation).operations().add(1);
        }

        @Override
        public Map<String, BlobStoreActionStats> toMap() {
            return counters.values().stream().collect(Collectors.toUnmodifiableMap(Counters::name, (c) -> {
                var ops = c.operations().sum();
                return new BlobStoreActionStats(ops, ops);
            }));
        }

    }

    /**
     * Serverless tracker is 2-dimensional: OperationPurpose and Operations.
     * Every combination of these has own set of counters: number of operations and number of http requests.
     * A single operation might have multiple HTTP requests, for example a single ResumableUpload operation
     * can perform a series of HTTP requests with size up to {@link GoogleCloudStorageBlobStore#SDK_DEFAULT_CHUNK_SIZE}.
     * <pre>
     * {@code
     * | Purpose      | Operation       | OperationsCnt | RequestCnt |
     * |--------------+-----------------+---------------+------------|
     * | SnapshotData | GetObject       |             1 |          1 |
     * | SnapshotData | ListObjects     |             2 |          2 |
     * | SnapshotData | ResumableUpload |             1 |         10 |
     * | SnapshotData | ...             |               |            |
     * | Translog     | GetObject       |             5 |          5 |
     * | ...          |                 |               |            |
     * }
     * </pre>
     */
    static final class ServerlessTracker implements StatsTracker {
        private final EnumMap<OperationPurpose, EnumMap<Operation, Counters>> counters;

        ServerlessTracker(String bucketName) {
            this.counters = new EnumMap<>(OperationPurpose.class);
            for (var purpose : OperationPurpose.values()) {
                var operations = new EnumMap<Operation, Counters>(Operation.class);
                for (var operation : Operation.values()) {
                    operations.put(operation, new Counters(purpose.getKey() + "_" + operation.key()));
                }
                counters.put(purpose, operations);
            }
        }

        @Override
        public void trackOperation(OperationPurpose purpose, Operation operation) {
            counters.get(purpose).get(operation).operations.add(1);
        }

        @Override
        public void trackRequest(OperationPurpose purpose, Operation operation) {
            counters.get(purpose).get(operation).requests.add(1);
        }

        @Override
        public void trackOperationAndRequest(OperationPurpose purpose, Operation operation) {
            var c = counters.get(purpose).get(operation);
            c.requests.add(1);
            c.operations.add(1);
        }

        @Override
        public Map<String, BlobStoreActionStats> toMap() {
            return counters.values()
                .stream()
                .flatMap(ops -> ops.values().stream())
                .collect(
                    Collectors.toUnmodifiableMap(Counters::name, (c) -> new BlobStoreActionStats(c.operations.sum(), c.requests.sum()))
                );
        }

    }

    private record Counters(String name, LongAdder operations, LongAdder requests) {
        Counters(String name) {
            this(name, new LongAdder(), new LongAdder());
        }
    }
}
