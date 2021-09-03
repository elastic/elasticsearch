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
 * A {@code PrefixInputStream} wraps another input stream and exposes
 * only the first bytes of it. Reading from the wrapping
 * {@code PrefixInputStream} consumes the underlying stream. The stream
 * is exhausted when {@code prefixLength} bytes have been read, or the underlying
 * stream is exhausted before that.
 * <p>
 * Only if the {@code closeSource} constructor argument is {@code true}, the
 * closing of this stream will also close the underlying input stream.
 * Any subsequent {@code read}, {@code skip} and {@code available} calls
 * will throw {@code IOException}s.
 */
public final class PrefixInputStream extends InputStream {

    /**
     * The underlying stream of which only a prefix is returned
     */
    private final InputStream source;
    /**
     * The length in bytes of the prefix.
     * This is the maximum number of bytes that can be read from this stream,
     * but fewer bytes can be read if the wrapped source stream itself contains fewer bytes
     */
    private final int prefixLength;
    /**
     * The current count of bytes read from this stream.
     * This starts of as {@code 0} and is always smaller or equal to {@code prefixLength}.
     */
    private int count;
    /**
     * whether closing this stream must also close the underlying stream
     */
    private boolean closeSource;
    /**
     * flag signalling if this stream has been closed
     */
    private boolean closed;

    public PrefixInputStream(InputStream source, int prefixLength, boolean closeSource) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("The prefixLength constructor argument must be a positive integer");
        }
        this.source = source;
        this.prefixLength = prefixLength;
        this.count = 0;
        this.closeSource = closeSource;
        this.closed = false;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        if (remainingPrefixByteCount() <= 0) {
            return -1;
        }
        int byteVal = source.read();
        if (byteVal == -1) {
            return -1;
        }
        count++;
        return byteVal;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        if (remainingPrefixByteCount() <= 0) {
            return -1;
        }
        int readSize = Math.min(len, remainingPrefixByteCount());
        int bytesRead = source.read(b, off, readSize);
        if (bytesRead == -1) {
            return -1;
        }
        count += bytesRead;
        return bytesRead;
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0 || remainingPrefixByteCount() <= 0) {
            return 0;
        }
        long bytesToSkip = Math.min(n, remainingPrefixByteCount());
        assert bytesToSkip > 0;
        long bytesSkipped = source.skip(bytesToSkip);
        count += bytesSkipped;
        return bytesSkipped;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return Math.min(remainingPrefixByteCount(), source.available());
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {
        // mark and reset are not supported
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (closeSource) {
            source.close();
        }
    }

    private int remainingPrefixByteCount() {
        return prefixLength - count;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

}
