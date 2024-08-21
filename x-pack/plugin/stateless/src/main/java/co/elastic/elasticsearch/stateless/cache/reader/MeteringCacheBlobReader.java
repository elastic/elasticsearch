/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache.reader;

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

    /**
     * Notified when a {@link MeteringInputStream} is closed, providing information
     * about its consumption.
     */
    public interface ReadCompleteCallback {
        /**
         * Notify that a stream was consumed
         *
         * @param bytesRead The number of bytes read
         * @param timeToReadNanos The time between the first byte being read and the stream being closed (in nanoseconds)
         */
        void onReadCompleted(int bytesRead, long timeToReadNanos);
    }

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
                totalBytesRead += 1;
            }
            return byteOfData;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int bytesRead = super.read(b, off, len);
            if (bytesRead != -1) {
                totalBytesRead += bytesRead;
            }
            return bytesRead;
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
