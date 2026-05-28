/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FilterInputStream} that throws {@link IOException} once a configured byte count is exceeded.
 * Useful for bounding the size of HTTP response bodies read into memory.
 *
 * <p>Near-duplicates of this primitive already exist in {@code x-pack/plugin/inference} and
 * {@code x-pack/plugin/watcher}; the three should be consolidated into a shared component.
 *
 * <p>{@code mark}/{@code reset} are unsupported to avoid recalculating the byte counter on rewind.
 */
public final class SizeLimitInputStream extends FilterInputStream {

    private final long maxByteSize;
    private final AtomicLong byteCounter = new AtomicLong(0);

    public SizeLimitInputStream(long maxByteSize, InputStream in) {
        super(in);
        if (maxByteSize < 0L) {
            throw new IllegalArgumentException("maxByteSize must be non-negative, got [" + maxByteSize + "]");
        }
        this.maxByteSize = maxByteSize;
    }

    @Override
    public int read() throws IOException {
        int bytesRead = super.read();
        if (bytesRead != -1) {
            byteCounter.incrementAndGet();
            checkMaximumLengthReached();
        }
        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = super.read(b, off, len);
        if (bytesRead != -1) {
            byteCounter.addAndGet(bytesRead);
            checkMaximumLengthReached();
        }
        return bytesRead;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark not supported");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private void checkMaximumLengthReached() throws IOException {
        if (byteCounter.get() > maxByteSize) {
            throw new IOException("Maximum limit of [" + maxByteSize + "] bytes reached");
        }
    }
}
