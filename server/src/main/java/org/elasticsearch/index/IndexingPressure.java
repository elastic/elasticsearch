/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.stats.IndexingPressureStats;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IndexingPressure {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES = Setting.memorySizeSetting(
        "indexing_pressure.memory.limit",
        "10%",
        Setting.Property.NodeScope
    );

    // TODO: Remove once it is no longer needed for BWC
    public static final Setting<ByteSizeValue> SPLIT_BULK_THRESHOLD = Setting.memorySizeSetting(
        "indexing_pressure.memory.split_bulk_threshold",
        "8.5%",
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_COORDINATING_BYTES = Setting.memorySizeSetting(
        "indexing_pressure.memory.coordinating.limit",
        MAX_INDEXING_BYTES,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_PRIMARY_BYTES = Setting.memorySizeSetting(
        "indexing_pressure.memory.primary.limit",
        MAX_INDEXING_BYTES,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_REPLICA_BYTES = Setting.memorySizeSetting(
        "indexing_pressure.memory.replica.limit",
        (s) -> ByteSizeValue.ofBytes((long) (MAX_PRIMARY_BYTES.get(s).getBytes() * 1.5)).getStringRep(),
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SPLIT_BULK_HIGH_WATERMARK = Setting.memorySizeSetting(
        "indexing_pressure.memory.split_bulk.watermark.high",
        "7.5%",
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SPLIT_BULK_HIGH_WATERMARK_SIZE = Setting.byteSizeSetting(
        "indexing_pressure.memory.split_bulk.watermark.high.bulk_size",
        ByteSizeValue.ofMb(1),
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SPLIT_BULK_LOW_WATERMARK = Setting.memorySizeSetting(
        "indexing_pressure.memory.split_bulk.watermark.low",
        "5.0%",
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SPLIT_BULK_LOW_WATERMARK_SIZE = Setting.byteSizeSetting(
        "indexing_pressure.memory.split_bulk.watermark.low.bulk_size",
        ByteSizeValue.ofMb(4),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(IndexingPressure.class);

    private final AtomicLong currentCombinedCoordinatingAndPrimaryBytes = new AtomicLong(0);
    private final AtomicLong currentCoordinatingBytes = new AtomicLong(0);
    private final AtomicLong currentPrimaryBytes = new AtomicLong(0);
    private final AtomicLong currentReplicaBytes = new AtomicLong(0);

    private final AtomicLong currentCoordinatingOps = new AtomicLong(0);
    private final AtomicLong currentPrimaryOps = new AtomicLong(0);
    private final AtomicLong currentReplicaOps = new AtomicLong(0);

    private final AtomicLong totalCombinedCoordinatingAndPrimaryBytes = new AtomicLong(0);
    private final AtomicLong totalCoordinatingBytes = new AtomicLong(0);
    private final AtomicLong totalPrimaryBytes = new AtomicLong(0);
    private final AtomicLong totalReplicaBytes = new AtomicLong(0);

    private final AtomicLong totalCoordinatingOps = new AtomicLong(0);
    private final AtomicLong totalCoordinatingRequests = new AtomicLong(0);
    private final AtomicLong totalPrimaryOps = new AtomicLong(0);
    private final AtomicLong totalReplicaOps = new AtomicLong(0);

    private final AtomicLong coordinatingRejections = new AtomicLong(0);
    private final AtomicLong primaryRejections = new AtomicLong(0);
    private final AtomicLong replicaRejections = new AtomicLong(0);
    private final AtomicLong primaryDocumentRejections = new AtomicLong(0);

    private final AtomicLong lowWaterMarkSplits = new AtomicLong(0);
    private final AtomicLong highWaterMarkSplits = new AtomicLong(0);

    private final long lowWatermark;
    private final long lowWatermarkSize;
    private final long highWatermark;
    private final long highWatermarkSize;
    private final long coordinatingLimit;
    private final long primaryLimit;
    private final long replicaLimit;

    public IndexingPressure(Settings settings) {
        this.lowWatermark = SPLIT_BULK_LOW_WATERMARK.get(settings).getBytes();
        this.lowWatermarkSize = SPLIT_BULK_LOW_WATERMARK_SIZE.get(settings).getBytes();
        this.highWatermark = SPLIT_BULK_HIGH_WATERMARK.get(settings).getBytes();
        this.highWatermarkSize = SPLIT_BULK_HIGH_WATERMARK_SIZE.get(settings).getBytes();
        this.coordinatingLimit = MAX_COORDINATING_BYTES.get(settings).getBytes();
        this.primaryLimit = MAX_PRIMARY_BYTES.get(settings).getBytes();
        this.replicaLimit = MAX_REPLICA_BYTES.get(settings).getBytes();
    }

    private static Releasable wrapReleasable(Releasable releasable) {
        final AtomicBoolean called = new AtomicBoolean();
        return () -> {
            if (called.compareAndSet(false, true)) {
                releasable.close();
            } else {
                logger.error("IndexingPressure memory is adjusted twice", new IllegalStateException("Releasable is called twice"));
                assert false : "IndexingPressure is adjusted twice";
            }
        };
    }

    public Incremental startIncrementalCoordinating(int operations, long bytes, boolean forceExecution) {
        Incremental coordinating = new Incremental(forceExecution);
        coordinating.coordinating.increment(operations, bytes);
        return coordinating;
    }

    public Coordinating markCoordinatingOperationStarted(int operations, long bytes, boolean forceExecution) {
        Coordinating coordinating = createCoordinatingOperation(forceExecution);
        coordinating.increment(operations, bytes);
        return coordinating;
    }

    public Coordinating createCoordinatingOperation(boolean forceExecution) {
        return new Coordinating(forceExecution);
    }

    public class Incremental implements Releasable {

        private final AtomicBoolean closed = new AtomicBoolean();
        private final boolean forceExecution;
        private long currentUnparsedSize = 0;
        private long totalParsedBytes = 0;
        private Coordinating coordinating;

        public Incremental(boolean forceExecution) {
            this.forceExecution = forceExecution;
            this.coordinating = new Coordinating(forceExecution);
        }

        public long totalParsedBytes() {
            return totalParsedBytes;
        }

        public void incrementUnparsedBytes(long bytes) {
            assert closed.get() == false;
            // TODO: Implement integration with IndexingPressure for unparsed bytes
            currentUnparsedSize += bytes;
        }

        public void transferUnparsedBytesToParsed(long bytes) {
            assert closed.get() == false;
            assert currentUnparsedSize >= bytes;
            currentUnparsedSize -= bytes;
            totalParsedBytes += bytes;
        }

        public void increment(int operations, long bytes) {
            // TODO: Eventually most of the memory will already be accounted for in unparsed.
            coordinating.increment(operations, bytes);
        }

        public long currentOperationsSize() {
            return coordinating.currentOperationsSize;
        }

        public Optional<Releasable> maybeSplit() {
            long currentUsage = (currentCombinedCoordinatingAndPrimaryBytes.get() + currentReplicaBytes.get());
            long currentOperationsSize = coordinating.currentOperationsSize;
            if (currentUsage >= highWatermark && currentOperationsSize >= highWatermarkSize) {
                highWaterMarkSplits.getAndIncrement();
                logger.trace(
                    () -> Strings.format(
                        "Split bulk due to high watermark: current bytes [%d] and size [%d]",
                        currentUsage,
                        currentOperationsSize
                    )
                );
                return Optional.of(split());
            }
            if (currentUsage >= lowWatermark && currentOperationsSize >= lowWatermarkSize) {
                lowWaterMarkSplits.getAndIncrement();
                logger.trace(
                    () -> Strings.format(
                        "Split bulk due to low watermark: current bytes [%d] and size [%d]",
                        currentUsage,
                        currentOperationsSize
                    )
                );
                return Optional.of(split());
            }
            return Optional.empty();
        }

        public Releasable split() {
            Coordinating toReturn = coordinating;
            coordinating = new Coordinating(forceExecution);
            return toReturn;
        }

        @Override
        public void close() {
            coordinating.close();
        }
    }

    // TODO: Maybe this should be re-named and used for primary operations too. Eventually we will need to account for: ingest pipeline
    // expansions, reading updates, etc. This could just be a generic OP that could be expanded as appropriate
    public class Coordinating implements Releasable {

        private final AtomicBoolean closed = new AtomicBoolean();
        private final boolean forceExecution;
        private int currentOperations = 0;
        private long currentOperationsSize = 0;

        public Coordinating(boolean forceExecution) {
            this.forceExecution = forceExecution;
        }

        public void increment(int operations, long bytes) {
            assert closed.get() == false;
            long combinedBytes = currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
            long replicaWriteBytes = currentReplicaBytes.get();
            long totalBytes = combinedBytes + replicaWriteBytes;
            if (forceExecution == false && totalBytes > coordinatingLimit) {
                long bytesWithoutOperation = combinedBytes - bytes;
                long totalBytesWithoutOperation = totalBytes - bytes;
                currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
                coordinatingRejections.getAndIncrement();
                throw new EsRejectedExecutionException(
                    "rejected execution of coordinating operation ["
                        + "coordinating_and_primary_bytes="
                        + bytesWithoutOperation
                        + ", "
                        + "replica_bytes="
                        + replicaWriteBytes
                        + ", "
                        + "all_bytes="
                        + totalBytesWithoutOperation
                        + ", "
                        + "coordinating_operation_bytes="
                        + bytes
                        + ", "
                        + "max_coordinating_bytes="
                        + coordinatingLimit
                        + "]",
                    false
                );
            }
            currentOperations += operations;
            currentOperationsSize += bytes;
            logger.trace(() -> Strings.format("adding [%d] coordinating operations and [%d] bytes", operations, bytes));
            currentCoordinatingBytes.getAndAdd(bytes);
            currentCoordinatingOps.getAndAdd(operations);
            totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
            totalCoordinatingBytes.getAndAdd(bytes);
            totalCoordinatingOps.getAndAdd(operations);
            totalCoordinatingRequests.getAndIncrement();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                logger.trace(
                    () -> Strings.format("removing [%d] coordinating operations and [%d] bytes", currentOperations, currentOperationsSize)
                );
                currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-currentOperationsSize);
                currentCoordinatingBytes.getAndAdd(-currentOperationsSize);
                currentCoordinatingOps.getAndAdd(-currentOperations);
                currentOperationsSize = 0;
                currentOperations = 0;
            } else {
                logger.error("IndexingPressure memory is adjusted twice", new IllegalStateException("Releasable is called twice"));
                assert false : "IndexingPressure is adjusted twice";
            }
        }
    }

    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(int operations, long bytes) {
        currentPrimaryBytes.getAndAdd(bytes);
        currentPrimaryOps.getAndAdd(operations);
        totalPrimaryBytes.getAndAdd(bytes);
        totalPrimaryOps.getAndAdd(operations);
        return wrapReleasable(() -> {
            this.currentPrimaryBytes.getAndAdd(-bytes);
            this.currentPrimaryOps.getAndAdd(-operations);
        });
    }

    public Releasable markPrimaryOperationStarted(int operations, long bytes, boolean forceExecution) {
        long combinedBytes = this.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long replicaWriteBytes = this.currentReplicaBytes.get();
        long totalBytes = combinedBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > primaryLimit) {
            long bytesWithoutOperation = combinedBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.primaryRejections.getAndIncrement();
            this.primaryDocumentRejections.addAndGet(operations);
            throw new EsRejectedExecutionException(
                "rejected execution of primary operation ["
                    + "coordinating_and_primary_bytes="
                    + bytesWithoutOperation
                    + ", "
                    + "replica_bytes="
                    + replicaWriteBytes
                    + ", "
                    + "all_bytes="
                    + totalBytesWithoutOperation
                    + ", "
                    + "primary_operation_bytes="
                    + bytes
                    + ", "
                    + "max_primary_bytes="
                    + primaryLimit
                    + "]",
                false
            );
        }
        logger.trace(() -> Strings.format("adding [%d] primary operations and [%d] bytes", operations, bytes));
        currentPrimaryBytes.getAndAdd(bytes);
        currentPrimaryOps.getAndAdd(operations);
        totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
        totalPrimaryBytes.getAndAdd(bytes);
        totalPrimaryOps.getAndAdd(operations);
        return wrapReleasable(() -> {
            logger.trace(() -> Strings.format("removing [%d] primary operations and [%d] bytes", operations, bytes));
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.currentPrimaryBytes.getAndAdd(-bytes);
            this.currentPrimaryOps.getAndAdd(-operations);
        });
    }

    public Releasable markReplicaOperationStarted(int operations, long bytes, boolean forceExecution) {
        long replicaWriteBytes = this.currentReplicaBytes.addAndGet(bytes);
        if (forceExecution == false && replicaWriteBytes > replicaLimit) {
            long replicaBytesWithoutOperation = replicaWriteBytes - bytes;
            this.currentReplicaBytes.getAndAdd(-bytes);
            this.replicaRejections.getAndIncrement();
            throw new EsRejectedExecutionException(
                "rejected execution of replica operation ["
                    + "replica_bytes="
                    + replicaBytesWithoutOperation
                    + ", "
                    + "replica_operation_bytes="
                    + bytes
                    + ", "
                    + "max_replica_bytes="
                    + replicaLimit
                    + "]",
                false
            );
        }
        currentReplicaOps.getAndAdd(operations);
        totalReplicaBytes.getAndAdd(bytes);
        totalReplicaOps.getAndAdd(operations);
        return wrapReleasable(() -> {
            this.currentReplicaBytes.getAndAdd(-bytes);
            this.currentReplicaOps.getAndAdd(-operations);
        });
    }

    public IndexingPressureStats stats() {
        return new IndexingPressureStats(
            totalCombinedCoordinatingAndPrimaryBytes.get(),
            totalCoordinatingBytes.get(),
            totalPrimaryBytes.get(),
            totalReplicaBytes.get(),
            currentCombinedCoordinatingAndPrimaryBytes.get(),
            currentCoordinatingBytes.get(),
            currentPrimaryBytes.get(),
            currentReplicaBytes.get(),
            coordinatingRejections.get(),
            primaryRejections.get(),
            replicaRejections.get(),
            coordinatingLimit,
            totalCoordinatingOps.get(),
            totalPrimaryOps.get(),
            totalReplicaOps.get(),
            currentCoordinatingOps.get(),
            currentPrimaryOps.get(),
            currentReplicaOps.get(),
            primaryDocumentRejections.get(),
            totalCoordinatingRequests.get(),
            lowWaterMarkSplits.get(),
            highWaterMarkSplits.get()
        );
    }
}
