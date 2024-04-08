/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IndexingPressure {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES = Setting.memorySizeSetting(
        "indexing_pressure.memory.limit",
        "10%",
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
    private final AtomicLong totalPrimaryOps = new AtomicLong(0);
    private final AtomicLong totalReplicaOps = new AtomicLong(0);

    private final AtomicLong coordinatingRejections = new AtomicLong(0);
    private final AtomicLong primaryRejections = new AtomicLong(0);
    private final AtomicLong replicaRejections = new AtomicLong(0);
    private final AtomicLong primaryDocumentRejections = new AtomicLong(0);

    private final long primaryAndCoordinatingLimits;
    private final long replicaLimits;

    public IndexingPressure(Settings settings) {
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.replicaLimits = (long) (this.primaryAndCoordinatingLimits * 1.5);
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

    public Releasable markCoordinatingOperationStarted(int operations, long bytes, boolean forceExecution) {
        long combinedBytes = this.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long replicaWriteBytes = this.currentReplicaBytes.get();
        long totalBytes = combinedBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
            long bytesWithoutOperation = combinedBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.coordinatingRejections.getAndIncrement();
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
                    + "max_coordinating_and_primary_bytes="
                    + primaryAndCoordinatingLimits
                    + "]",
                false
            );
        }
        logger.trace(() -> Strings.format("adding [%d] coordinating operations and [%d] bytes", operations, bytes));
        currentCoordinatingBytes.getAndAdd(bytes);
        currentCoordinatingOps.getAndAdd(operations);
        totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
        totalCoordinatingBytes.getAndAdd(bytes);
        totalCoordinatingOps.getAndAdd(operations);
        return wrapReleasable(() -> {
            logger.trace(() -> Strings.format("removing [%d] coordinating operations and [%d] bytes", operations, bytes));
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.currentCoordinatingBytes.getAndAdd(-bytes);
            this.currentCoordinatingOps.getAndAdd(-operations);
        });
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
        if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
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
                    + "max_coordinating_and_primary_bytes="
                    + primaryAndCoordinatingLimits
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
        if (forceExecution == false && replicaWriteBytes > replicaLimits) {
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
                    + replicaLimits
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
            primaryAndCoordinatingLimits,
            totalCoordinatingOps.get(),
            totalPrimaryOps.get(),
            totalReplicaOps.get(),
            currentCoordinatingOps.get(),
            currentPrimaryOps.get(),
            currentReplicaOps.get(),
            primaryDocumentRejections.get()
        );
    }
}
