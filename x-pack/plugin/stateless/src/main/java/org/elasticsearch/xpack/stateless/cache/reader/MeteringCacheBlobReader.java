/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;

import java.io.InputStream;
import java.util.function.IntConsumer;

/**
 * Wrapper around {@link CacheBlobReader} which counts how many bytes were read through delegated {@link CacheBlobReader}
 */
public class MeteringCacheBlobReader implements CacheBlobReader {

    private static final Logger logger = LogManager.getLogger(MeteringCacheBlobReader.class);

    private final CacheBlobReader delegate;
    private final ReadCompleteCallback readCompleteCallback;

    public MeteringCacheBlobReader(final CacheBlobReader delegate, final ReadCompleteCallback readCompleteCallback) {
        this.delegate = delegate;
        this.readCompleteCallback = readCompleteCallback;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        return delegate.getRange(position, length, remainingFileLength);
    }

    @Override
    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
        delegate.getRangeInputStream(position, length, listener);
    }

    @Override
    public String executorName() {
        return delegate.executorName();
    }

    /**
     * Returns a consumer that increments the byte counter as each chunk lands in the cache, before the
     * {@link org.elasticsearch.blobcache.common.SparseFileTracker} unblocks any waiting reader threads.
     * Exceptions thrown by the callback are caught and logged at DEBUG to prevent a metrics failure from
     * aborting the cache-fill operation.
     */
    @Override
    public IntConsumer newBytesCopiedConsumer() {
        return bytes -> {
            try {
                readCompleteCallback.onBytesRead(bytes);
            } catch (Exception e) {
                logger.debug("Error calling call-back", e);
            }
        };
    }

    /**
     * Records the elapsed time of the full range copy. Called once per range, after all chunks have landed
     * and after the {@link org.elasticsearch.blobcache.common.SparseFileTracker} has advanced. Safe to call
     * after the reader has been unblocked because this only affects throughput telemetry, not byte counters.
     */
    @Override
    public void onCopyCompleted(int totalBytesRead, long timeNanos) {
        try {
            readCompleteCallback.onCopyCompleted(totalBytesRead, timeNanos);
        } catch (Exception e) {
            logger.debug("Error calling timing call-back", e);
        }
    }

    /**
     * Notified as bytes land in the cache (per-chunk) and once when the full range copy completes.
     * The two methods are called from different points in the copy pipeline; see
     * {@link MeteringCacheBlobReader#newBytesCopiedConsumer()} and
     * {@link MeteringCacheBlobReader#onCopyCompleted(int, long)} for the happens-before guarantees.
     */
    public interface ReadCompleteCallback {
        /**
         * Called once per chunk, before the SparseFileTracker advances. Used for byte-counter updates
         * that must be visible to reader threads before they are unblocked.
         *
         * @param bytesRead The number of bytes in this chunk
         */
        default void onBytesRead(int bytesRead) {};

        /**
         * Called once after the full range copy completes, with the total bytes and wall-clock duration
         * of the copy. Used for throughput-metric recording.
         *
         * @param totalBytesRead Total bytes copied for this range
         * @param timeNanos      Wall-clock duration of the copy in nanoseconds
         */
        default void onCopyCompleted(int totalBytesRead, long timeNanos) {};
    }
}
