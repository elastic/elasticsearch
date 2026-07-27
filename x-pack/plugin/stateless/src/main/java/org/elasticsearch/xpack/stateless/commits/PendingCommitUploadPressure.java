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
import org.elasticsearch.index.IndexingPressureMonitor.IndexingPressureContributor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the estimated heap retained by compound commits (CCs) pending upload to the object store
 * and implements {@link IndexingPressureContributor} to reject new indexing operations when too
 * many commits are queued. This prevents {@code ShardCommitState#pendingUploadBccGenerations} from
 * growing without bound.
 *
 * <p>The byte limit is set directly via {@link #PENDING_CC_UPLOAD_MEMORY_LIMIT}, expressed as a
 * fraction of JVM heap.
 */
public class PendingCommitUploadPressure implements IndexingPressureContributor {

    /**
     * Estimated heap retained per compound commit (CC) pending upload for everything <em>except</em>
     * the CC's  header bytes, which {@link #estimateHeapBytes} measures exactly. Covers the
     * per-CC metadata: {@code BlobLocation} maps, {@code PrimaryTermAndGeneration}, internal-file
     * readers, and commit references.
     *
     * <p>From incident heap-dump analysis, a pending VBCC retained ~135 KB per CC in total, of which the
     * compound-commit header {@code byte[]} was ~95 KB (counted separately, see {@link #estimateHeapBytes}).
     * The remaining metadata was ~38 KB per CC; rounded up to 50 KB for a safety margin.
     */
    public static final long ESTIMATED_METADATA_BYTES_PER_CC = ByteSizeUnit.KB.toBytes(50);

    /**
     * Fraction of the JVM heap that may be consumed by commits pending upload before new indexing
     * operations are rejected. Defaults to 10%: sized to absorb a typical transient upload spike (which
     * would otherwise drain on its own) while still rejecting a sustained overload before it can OOM the
     * node. Lower it if OOMs from pending-upload backlog persist.
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
     * Estimates the heap a frozen VBCC pins while pending upload. Two parts:
     * <ol>
     *   <li>per-CC metadata: compound-commit count times {@link #ESTIMATED_METADATA_BYTES_PER_CC}; and</li>
     *   <li>materialized header bytes: {@link VirtualBatchedCompoundCommit#getHeaderBytes()}, the exact
     *       total the VBCC has materialized on the heap for its per-CC header {@code byte[]} arrays
     *       (internal files and replicated ranges stream from the Lucene directory and padding uses a
     *       shared static buffer, so they hold no per-VBCC heap and are not counted).</li>
     * </ol>
     * Because a frozen VBCC is immutable, this yields the same value at queue and upload time, keeping
     * the tracked total balanced.
     */
    static long estimateHeapBytes(VirtualBatchedCompoundCommit vbcc) {
        return (long) vbcc.size() * ESTIMATED_METADATA_BYTES_PER_CC + vbcc.getHeaderBytes();
    }

    /**
     * Records that a frozen VBCC has been queued for upload, contributing {@link #estimateHeapBytes} of
     * estimated heap. Must be paired with a corresponding call to {@link #markVbccUploaded}.
     */
    public void markVbccQueued(VirtualBatchedCompoundCommit vbcc) {
        assert vbcc.isFrozen() : "VBCC must be frozen before it is queued for upload";
        markBytesQueued(estimateHeapBytes(vbcc));
    }

    /**
     * Records that a VBCC has been uploaded and removed from the pending queue, releasing
     * {@link #estimateHeapBytes} of estimated heap.
     */
    public void markVbccUploaded(VirtualBatchedCompoundCommit vbcc) {
        markBytesUploaded(estimateHeapBytes(vbcc));
    }

    /**
     * Internal byte-accounting primitive backing {@link #markVbccQueued}. Package-private so tests can
     * exercise byte-precise behaviour directly.
     */
    void markBytesQueued(long bytes) {
        long current = pendingBytes.addAndGet(bytes);
        logger.trace(() -> Strings.format("queued [%d] bytes for upload, total pending [%d]", bytes, current));
    }

    /**
     * Internal byte-accounting primitive backing {@link #markVbccUploaded}. Package-private so tests can
     * exercise byte-precise behaviour directly.
     */
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

    public long getPendingBytes() {
        return pendingBytes.get();
    }

    public long getPendingBytesLimit() {
        return pendingBytesLimit;
    }
}
