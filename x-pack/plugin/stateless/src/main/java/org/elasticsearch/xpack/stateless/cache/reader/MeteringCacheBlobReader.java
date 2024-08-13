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

import org.elasticsearch.blobcache.common.ByteRange;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.LongConsumer;

/**
 * Wrapper around {@link CacheBlobReader} which counts how many bytes were read through delegated {@link CacheBlobReader}
 */
public class MeteringCacheBlobReader implements CacheBlobReader {

    private final CacheBlobReader delegate;
    private final LongConsumer totalBytesReadConsumer;

    public MeteringCacheBlobReader(final CacheBlobReader delegate, final LongConsumer totalBytesReadConsumer) {
        this.delegate = delegate;
        this.totalBytesReadConsumer = totalBytesReadConsumer;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        return delegate.getRange(position, length, remainingFileLength);
    }

    @Override
    public InputStream getRangeInputStream(long position, int length) throws IOException {
        return new MeteringInputStream(delegate.getRangeInputStream(position, length));
    }

    private class MeteringInputStream extends FilterInputStream {

        private long totalBytesRead;
        private boolean closed;

        private MeteringInputStream(InputStream delegateInputStream) {
            super(delegateInputStream);
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
                totalBytesReadConsumer.accept(totalBytesRead);
                closed = true;
            }
            super.close();
        }
    }
}
