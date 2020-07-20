/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.ByteArray;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachedBlobContainer extends FilterBlobContainer {

    protected static final int DEFAULT_BYTE_ARRAY_SIZE = 1 << 14;

    public CachedBlobContainer(BlobContainer delegate) {
        super(delegate);
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new CachedBlobContainer(child);
    }

    /**
     * A {@link FilterInputStream} that copies over all the bytes read from the original input stream to a given {@link ByteArray}. The
     * number of bytes copied cannot exceed the size of the {@link ByteArray}.
     */
    static class CopyOnReadInputStream extends FilterInputStream {

        private final AtomicBoolean closed;
        private final ByteArray bytes;

        private long count;
        private long mark;

        protected CopyOnReadInputStream(InputStream in, ByteArray byteArray) {
            super(in);
            this.bytes = Objects.requireNonNull(byteArray);
            this.closed = new AtomicBoolean(false);
        }

        long getCount() {
            return count;
        }

        public int read() throws IOException {
            final int result = super.read();
            if (result != -1) {
                if (count < bytes.size()) {
                    bytes.set(count, (byte) result);
                }
                count++;
            }
            return result;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            final int result = super.read(b, off, len);
            if (result != -1) {
                if (count < bytes.size()) {
                    bytes.set(count, b, off, Math.toIntExact(Math.min(bytes.size() - count, result)));
                }
                count += result;
            }
            return result;
        }

        @Override
        public long skip(long n) throws IOException {
            final long skip = super.skip(n);
            if (skip > 0L) {
                count += skip;
            }
            return skip;
        }

        @Override
        public synchronized void mark(int readlimit) {
            super.mark(readlimit);
            mark = count;
        }

        @Override
        public synchronized void reset() throws IOException {
            super.reset();
            count = mark;
        }

        protected void closeInternal(final ReleasableBytesReference releasable) {
            releasable.close();
        }

        @Override
        public final void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                boolean success = false;
                try {
                    super.close();
                    final PagedBytesReference reference = new PagedBytesReference(bytes, Math.toIntExact(Math.min(count, bytes.size())));
                    closeInternal(new ReleasableBytesReference(reference, bytes));
                    success = true;
                } finally {
                    if (success == false) {
                        bytes.close();
                    }
                }
            }
        }
    }
}
