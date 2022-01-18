/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.encrypted;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A {@code CountingInputStream} wraps another input stream and counts the number of bytes
 * that have been read or skipped.
 * <p>
 * This input stream does no buffering on its own and only supports {@code mark} and
 * {@code reset} if the underlying wrapped stream supports it.
 * <p>
 * If the stream supports {@code mark} and {@code reset} the byte count is also reset to the
 * value that it had on the last {@code mark} call, thereby not counting the same bytes twice.
 * <p>
 * If the {@code closeSource} constructor argument is {@code true}, closing this
 * stream will also close the wrapped input stream. Apart from closing the wrapped
 * stream in this case, the {@code close} method does nothing else.
 */
public final class CountingInputStream extends InputStream {

    private final InputStream source;
    private final boolean closeSource;
    long count; // package-protected for tests
    long mark; // package-protected for tests
    boolean closed; // package-protected for tests

    /**
     * Wraps another input stream, counting the number of bytes read.
     *
     * @param source the input stream to be wrapped
     * @param closeSource {@code true} if closing this stream will also close the wrapped stream
     */
    public CountingInputStream(InputStream source, boolean closeSource) {
        this.source = Objects.requireNonNull(source);
        this.closeSource = closeSource;
        this.count = 0L;
        this.mark = -1L;
        this.closed = false;
    }

    /** Returns the number of bytes read. */
    public long getCount() {
        return count;
    }

    @Override
    public int read() throws IOException {
        int result = source.read();
        if (result != -1) {
            count++;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = source.read(b, off, len);
        if (result != -1) {
            count += result;
        }
        return result;
    }

    @Override
    public long skip(long n) throws IOException {
        long result = source.skip(n);
        count += result;
        return result;
    }

    @Override
    public int available() throws IOException {
        return source.available();
    }

    @Override
    public boolean markSupported() {
        return source.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
        source.mark(readlimit);
        mark = count;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (false == source.markSupported()) {
            throw new IOException("Mark not supported");
        }
        if (mark == -1L) {
            throw new IOException("Mark not set");
        }
        count = mark;
        source.reset();
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            if (closeSource) {
                source.close();
            }
        }
    }
}
