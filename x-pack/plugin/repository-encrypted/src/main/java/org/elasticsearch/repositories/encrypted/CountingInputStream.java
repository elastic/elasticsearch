/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A {@code CountingInputStream} wraps another input stream and counts the number of bytes
 * that have been read or skipped.
 * Bytes replayed following a {@code reset} call are not counted multiple times, i.e. only
 * the bytes that are produced in a single pass, without resets, by the wrapped stream are counted.
 * This input stream does no buffering on its own and only supports {@code mark} and
 * {@code reset} if the wrapped stream supports it.
 * <p>
 * If the {@code closeSource} constructor argument is {@code true}, closing this
 * stream will also close the wrapped input stream. Apart from closing the wrapped
 * stream in this case, the {@code close} method does nothing else.
 */
public final class CountingInputStream extends FilterInputStream {

    protected long count; // protected for tests
    protected long mark; // protected for tests
    protected boolean closed; // protected for tests
    private final boolean closeSource;

    /**
     * Wraps another input stream, counting the number of bytes read.
     *
     * @param in the input stream to be wrapped
     * @param closeSource {@code true} if closing this stream will also close the wrapped stream
     */
    public CountingInputStream(InputStream in, boolean closeSource) {
        super(Objects.requireNonNull(in));
        this.count = 0L;
        this.mark = -1L;
        this.closed = false;
        this.closeSource = closeSource;
    }

    /** Returns the number of bytes read. */
    public long getCount() {
        return count;
    }

    @Override
    public int read() throws IOException {
        int result = in.read();
        if (result != -1) {
            count++;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = in.read(b, off, len);
        if (result != -1) {
            count += result;
        }
        return result;
    }

    @Override
    public long skip(long n) throws IOException {
        long result = in.skip(n);
        count += result;
        return result;
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
        mark = count;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (false == in.markSupported()) {
            throw new IOException("Mark not supported");
        }
        if (mark == -1L) {
            throw new IOException("Mark not set");
        }
        count = mark;
        in.reset();
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            if (closeSource) {
                in.close();
            }
        }
    }
}
