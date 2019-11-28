package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class ChainInputStream extends InputStream {

    private InputStream in;
    private InputStream markIn;
    private boolean closed;

    public ChainInputStream() {
        this.in = null;
        this.markIn = null;
        this.closed = false;
    }

    private void nextIn() throws IOException {
        if (in != null) {
            in.close();
        }
        in = next(in);
        if (in == null) {
            throw new NullPointerException();
        }
        if (markSupported() && false == in.markSupported()) {
            throw new IllegalStateException("chain input stream element must support mark");
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        do {
            int byteVal = in == null ? -1 : in.read();
            if (byteVal != -1) {
                return byteVal;
            }
            if (false == hasNext(in)) {
                return -1;
            }
            nextIn();
        } while (true);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        do {
            int bytesRead = in == null ? -1 : in.read(b, off, len);
            if (bytesRead != -1) {
                return bytesRead;
            }
            if (false == hasNext(in)) {
                return -1;
            }
            nextIn();
        } while (true);
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        long bytesRemaining = n;
        while (bytesRemaining > 0) {
            long bytesSkipped = in == null ? 0 : in.skip(bytesRemaining);
            if (bytesSkipped == 0) {
                int byteRead = read();
                if (byteRead == -1) {
                    break;
                } else {
                    bytesRemaining--;
                }
            } else {
                bytesRemaining -= bytesSkipped;
            }
        }
        return n - bytesRemaining;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return in == null ? 0 : in.available();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        if (markSupported()) {
            markIn = in;
            if (markIn != null) {
                markIn.mark(readlimit);
            }
        }
    }

    @Override
    public void reset() throws IOException {
        if (false == markSupported()) {
            throw new IOException("Mark/reset not supported");
        }
        in = markIn;
        if (in != null) {
            in.reset();
        }
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            if (in != null) {
                in.close();
            }
            while (hasNext(in)) {
                nextIn();
            }
        }
    }

    abstract boolean hasNext(@Nullable InputStream in);

    abstract InputStream next(@Nullable InputStream in) throws IOException;

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

}
