/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexingPressure.IndexingPressureContributor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the estimated heap retained by compound commits (CCs) pending upload and, as an
 * {@link IndexingPressureContributor}, rejects new indexing operations once it exceeds
 * {@link #PENDING_CC_UPLOAD_MEMORY_LIMIT}. Bounds {@code ShardCommitState#pendingUploadBccGenerations}.
 */
public class PendingCommitUploadPressure implements IndexingPressureContributor {

    /**
     * Estimated per-CC heap, excluding the header bytes that {@link #estimateHeapBytes} counts exactly.
     * From heap-dump analysis a pending CC retained ~135 KB (~95 KB header + ~38 KB metadata); the 38 KB
     * metadata is rounded up to 50 KB for margin.
     */
    public static final long ESTIMATED_METADATA_BYTES_PER_CC = ByteSizeUnit.KB.toBytes(50);

    /**
     * Fraction of JVM heap consumable by pending-upload commits before indexing is rejected. Default 10%:
     * absorbs a transient spike but rejects a sustained overload before it OOMs. Lower if OOMs persist.
     */
    public static final Setting<ByteSizeValue> PENDING_CC_UPLOAD_MEMORY_LIMIT = Setting.memorySizeSetting(
        "stateless.pending_commit_upload.memory.limit",
        "10%",
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(PendingCommitUploadPressure.class);

    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final long pendingBytesLimit;

    public PendingCommitUploadPressure(Settings settings) {
        this.pendingBytesLimit = PENDING_CC_UPLOAD_MEMORY_LIMIT.get(settings).getBytes();
    }

    /**
     * Estimated heap a frozen VBCC pins while pending upload: per-CC metadata ({@code size()} times
     * {@link #ESTIMATED_METADATA_BYTES_PER_CC}) plus its exact materialized header bytes
     * ({@link VirtualBatchedCompoundCommit#getHeaderBytes()}; other readers stream from disk / shared
     * buffers and hold no per-VBCC heap). A frozen VBCC is immutable, so this is identical at queue and
     * upload time, keeping the tracked total balanced.
     */
    static long estimateHeapBytes(VirtualBatchedCompoundCommit vbcc) {
        return (long) vbcc.size() * ESTIMATED_METADATA_BYTES_PER_CC + vbcc.getHeaderBytes();
    }

    /** Adds this frozen VBCC's {@link #estimateHeapBytes} to the pending total; pair with {@link #markVbccUploaded}. */
    public void markVbccQueued(VirtualBatchedCompoundCommit vbcc) {
        assert vbcc.isFrozen() : "VBCC must be frozen before it is queued for upload";
        markBytesQueued(estimateHeapBytes(vbcc));
    }

    /** Releases this VBCC's {@link #estimateHeapBytes} from the pending total. */
    public void markVbccUploaded(VirtualBatchedCompoundCommit vbcc) {
        markBytesUploaded(estimateHeapBytes(vbcc));
    }

    /** Byte-accounting primitive behind {@link #markVbccQueued}; package-private for byte-precise tests. */
    void markBytesQueued(long bytes) {
        long current = pendingBytes.addAndGet(bytes);
        logger.trace(() -> Strings.format("queued [%d] bytes for upload, total pending [%d]", bytes, current));
    }

    /** Byte-accounting primitive behind {@link #markVbccUploaded}; package-private for byte-precise tests. */
    void markBytesUploaded(long bytes) {
        long current = pendingBytes.addAndGet(-bytes);
        logger.trace(() -> Strings.format("uploaded [%d] bytes, total pending [%d]", bytes, current));
    }

    @Override
    public void checkAndMaybeReject() {
        long current = pendingBytes.get();
        if (current > pendingBytesLimit) {
            throw new EsRejectedExecutionException(
                "rejected execution of indexing operation due to pending commit upload pressure ["
                    + "pending_bytes="
                    + current
                    + ", "
                    + "pending_bytes_limit="
                    + pendingBytesLimit
                    + "]",
                false
            );
        }
    }

    // Package-private: exposed only for tests to observe the tracked backlog.
    long getPendingBytes() {
        return pendingBytes.get();
    }

    // Package-private: exposed only so tests can drive inputs relative to the heap-derived limit.
    long getPendingBytesLimit() {
        return pendingBytesLimit;
    }
}
