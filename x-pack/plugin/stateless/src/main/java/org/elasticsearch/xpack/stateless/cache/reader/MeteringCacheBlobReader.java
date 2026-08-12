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
     * This replaces the previous close()-based accounting to eliminate the race between cache-fill completion
     * and metric collection in {@link org.elasticsearch.xpack.stateless.recovery.metering.StatelessRecoveryMetricsCollector}.
     * Exceptions thrown by the callback are caught and logged at DEBUG to prevent a metrics failure from
     * aborting the cache-fill operation.
     */
    @Override
    public IntConsumer newBytesCopiedConsumer() {
        return bytes -> {
            try {
                readCompleteCallback.onReadCompleted(bytes, 0);
            } catch (Exception e) {
                logger.debug("Error calling readCompleteCallback", e);
            }
        };
    }

    /**
     * Notified as bytes are copied into the shared blob cache, once per chunk, via
     * {@link #newBytesCopiedConsumer()}.
     */
    public interface ReadCompleteCallback {
        /**
         * Notify that a chunk of bytes was copied into the cache.
         *
         * @param bytesRead The number of bytes in the chunk
         * @param timeToReadNanos Reserved for future timing use; currently always 0
         */
        void onReadCompleted(int bytesRead, long timeToReadNanos);
    }
}
