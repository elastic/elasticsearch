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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

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
        delegate.getRangeInputStream(position, length, listener.map(MeteringInputStream::new));
    }

    @Override
    public String executorName() {
        return delegate.executorName();
    }

    /**
     * Notified as bytes are read from the source stream (per-chunk) and once when the read is completed.
     */
    public interface ReadCompleteCallback {
        /**
         * Called as bytes are read from the metered stream, before they are written to the cache and before the
         * SparseFileTracker advances. Used for byte-counter updates that must be visible to reader threads before
         * they are unblocked.
         *
         * @param bytesRead The number of bytes in this chunk
         */
        default void onBytesRead(int bytesRead) {}

        /**
         * Notify that a stream was consumed.
         * <p>
         * Not called when no bytes were copied (totalBytesRead == 0)
         *
         * @param totalBytesRead Total bytes read
         * @param timeToReadNanos The time between the first byte being read and the stream being closed (in nanoseconds)
         */
        default void onReadCompleted(int totalBytesRead, long timeToReadNanos) {}
    }

    /**
     * Counts bytes per-read, notifies {@link ReadCompleteCallback#onBytesRead} immediately on each chunk
     * (before the SparseFileTracker advances), and fires {@link ReadCompleteCallback#onReadCompleted} with
     * elapsed timing on close.
     */
    private class MeteringInputStream extends FilterInputStream {

        private final long streamCreatedTimeNs;
        private int totalBytesRead;
        private boolean closed;

        private MeteringInputStream(InputStream delegateInputStream) {
            super(delegateInputStream);
            streamCreatedTimeNs = System.nanoTime();
        }

        @Override
        public int read() throws IOException {
            final int byteOfData = super.read();
            if (byteOfData != -1) {
                notifyBytesRead(1);
            }
            return byteOfData;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int bytesRead = super.read(b, off, len);
            if (bytesRead > 0) {
                notifyBytesRead(bytesRead);
            }
            return bytesRead;
        }

        private void notifyBytesRead(int bytesRead) {
            totalBytesRead += bytesRead;
            try {
                readCompleteCallback.onBytesRead(bytesRead);
            } catch (Exception e) {
                logger.debug("Error calling call-back", e);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed == false) {
                try {
                    if (totalBytesRead > 0) {
                        long readTimeNanos = System.nanoTime() - streamCreatedTimeNs;
                        readCompleteCallback.onReadCompleted(totalBytesRead, readTimeNanos);
                    }
                } catch (Exception e) {
                    logger.debug("Error calling call-back", e);
                } finally {
                    closed = true;
                }
            }
            super.close();
        }
    }
}
