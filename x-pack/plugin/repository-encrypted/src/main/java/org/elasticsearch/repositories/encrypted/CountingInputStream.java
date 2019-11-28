package org.elasticsearch.repositories.encrypted;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class CountingInputStream extends FilterInputStream {

    private long count;
    private long mark;
    private boolean closed;
    private final boolean closeSource;

    /**
     * Wraps another input stream, counting the number of bytes read.
     *
     * @param in the input stream to be wrapped
     * @param closeSource if closing this stream will propagate to the wrapped stream
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
