package org.elasticsearch.repositories.encrypted;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class PrefixInputStream extends FilterInputStream {

    private final int length;
    private int position;
    private boolean closeSource;
    private boolean closed;

    public PrefixInputStream(InputStream in, int length, boolean closeSource) {
        super(Objects.requireNonNull(in));
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
        return Math.min(length - position, super.available());
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

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
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

}
