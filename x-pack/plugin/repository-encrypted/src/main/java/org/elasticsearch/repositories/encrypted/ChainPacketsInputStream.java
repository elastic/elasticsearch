package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class ChainPacketsInputStream extends InputStream {

    private InputStream packetIn;
    private InputStream markIn;
    private boolean closed;

    public ChainPacketsInputStream() {
        this.packetIn = null;
        this.markIn = null;
        this.closed = false;
    }

    abstract boolean hasNextPacket(@Nullable InputStream currentPacketIn);

    abstract InputStream nextPacket(@Nullable InputStream currentPacketIn) throws IOException;

    @Override
    public int read() throws IOException {
        ensureOpen();
        do {
            int byteVal = packetIn == null ? -1 : packetIn.read();
            if (byteVal != -1) {
                return byteVal;
            }
            if (false == hasNextPacket(packetIn)) {
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
            int bytesRead = packetIn == null ? -1 : packetIn.read(b, off, len);
            if (bytesRead != -1) {
                return bytesRead;
            }
            if (false == hasNextPacket(packetIn)) {
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
            long bytesSkipped = packetIn == null ? 0 : packetIn.skip(bytesRemaining);
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
        return packetIn == null ? 0 : packetIn.available();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        if (markSupported()) {
            markIn = packetIn;
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
        packetIn = markIn;
        if (packetIn != null) {
            packetIn.reset();
        }
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            if (packetIn != null) {
                packetIn.close();
            }
            while (hasNextPacket(packetIn)) {
                nextIn();
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    private void nextIn() throws IOException {
        if (packetIn != null) {
            packetIn.close();
        }
        packetIn = nextPacket(packetIn);
        if (packetIn == null) {
            throw new NullPointerException();
        }
        if (markSupported() && false == packetIn.markSupported()) {
            throw new IllegalStateException("Packet input stream must support mark");
        }
    }

}
