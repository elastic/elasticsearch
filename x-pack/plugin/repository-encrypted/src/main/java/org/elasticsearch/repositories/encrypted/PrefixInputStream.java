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
 * A {@code PrefixInputStream} wraps another input stream and exposes
 * only the first bytes of it. Reading from the wrapping
 * {@code PrefixInputStream} consumes the underlying stream. The stream
 * is exhausted when {@code length} bytes have been read or the underlying
 * stream is exhausted.
 * <p>
 * If the {@code closeSource} constructor argument is {@code true}, closing this
 * stream will also close the underlying input stream. Any subsequent {@code read},
 * {@code skip} and {@code available} calls will throw {@code IOException}s.
 */
public final class PrefixInputStream extends FilterInputStream {

    private final int length;
    private int position;
    private boolean closeSource;
    private boolean closed;

    public PrefixInputStream(InputStream in, int length, boolean closeSource) {
        super(Objects.requireNonNull(in));
        if (length < 0) {
            throw new IllegalArgumentException("The length constructor argument must be a positive value");
        }
        this.length = length;
        this.position = 0;
        this.closeSource = closeSource;
        this.closed = false;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        if (position >= length) {
            return -1;
        }
        int byteVal = in.read();
        if (byteVal == -1) {
            return -1;
        }
        position++;
        return byteVal;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        if (position >= length) {
            return -1;
        }
        int readSize = Math.min(len, length - position);
        int bytesRead = in.read(b, off, readSize);
        if (bytesRead == -1) {
            return -1;
        }
        position += bytesRead;
        return bytesRead;
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0 || position >= length) {
            return 0;
        }
        long bytesToSkip = Math.min(n, length - position);
        assert bytesToSkip > 0;
        long bytesSkipped = in.skip(bytesToSkip);
        position += bytesSkipped;
        return bytesSkipped;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return Math.min(length - position, in.available());
    }

    @Override
    public void mark(int readlimit) {
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (closeSource) {
            in.close();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

}
