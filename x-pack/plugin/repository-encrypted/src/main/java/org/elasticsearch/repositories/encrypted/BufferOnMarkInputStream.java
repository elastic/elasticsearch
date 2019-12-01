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


public final class BufferOnMarkInputStream extends FilterInputStream {

    private final int bufferSize;
    private byte[] ringBuffer;
    private int head;
    private int tail;
    private int position;
    private boolean markCalled;
    private boolean resetCalled;
    private boolean closed;

    public BufferOnMarkInputStream(InputStream in, int bufferSize) {
        super(Objects.requireNonNull(in));
        this.bufferSize = bufferSize;
        this.ringBuffer = null;
        this.head = this.tail = this.position = -1;
        this.markCalled = this.resetCalled = false;
        this.closed = false;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        if (resetCalled) {
            int bytesRead = readFromBuffer(b, off, len);
            if (bytesRead == 0) {
                resetCalled = false;
            } else {
                return bytesRead;
            }
        }
        int bytesRead = in.read(b, off, len);
        if (bytesRead <= 0) {
            return bytesRead;
        }
        if (markCalled) {
            if (false == writeToBuffer(b, off, len)) {
                // could not fully write to buffer, invalidate mark
                markCalled = false;
            }
        }
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        byte[] arr = new byte[1];
        int readResult = read(arr, 0, arr.length);
        if (readResult == -1) {
            return -1;
        }
        return arr[0];
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        if (false == markCalled) {
            return in.skip(n);
        }
        long remaining = n;
        int size = (int)Math.min(2048, remaining);
        byte[] skipBuffer = new byte[size];
        while (remaining > 0) {
            int bytesRead = read(skipBuffer, 0, (int)Math.min(size, remaining));
            if (bytesRead < 0) {
                break;
            }
            remaining -= bytesRead;
        }
        return n - remaining;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        int bytesAvailable = 0;
        if (resetCalled) {
            if (position <= tail) {
                bytesAvailable += tail - position;
            } else {
                bytesAvailable += ringBuffer.length - position + tail;
            }
        }
        bytesAvailable += in.available();
        return bytesAvailable;
    }

    @Override
    public void mark(int readlimit) {
        if (readlimit > bufferSize) {
            throw new IllegalArgumentException("Readlimit value [" + readlimit + "] exceeds the maximum value of [" + bufferSize + "]");
        }
        markCalled = true;
        if (ringBuffer == null) {
            // "+ 1" for the full-buffer sentinel free element
            ringBuffer = new byte[bufferSize + 1];
            head = tail = position = 0;
        } else {
            head = position;
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        if (false == markCalled) {
            throw new IOException("Mark not called or has been invalidated");
        }
        resetCalled = true;
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            in.close();
        }
    }

    private int readFromBuffer(byte[] b, int off, int len) {
        if (position == tail) {
            return 0;
        }
        final int readLength;
        if (position <= tail) {
            readLength = Math.min(len, tail - position);
        } else {
            readLength = Math.min(len, ringBuffer.length - position);
        }
        System.arraycopy(ringBuffer, position, b, off, readLength);
        position += readLength;
        if (position == ringBuffer.length) {
            position = 0;
        }
        return readLength;
    }

    private int getRemainingBufferCapacity() {
        if (head == tail) {
            return ringBuffer.length - 1;
        } else if (head < tail) {
            return ringBuffer.length - tail + head - 1;
        } else {
            return head - tail - 1;
        }
    }

    private boolean writeToBuffer(byte[] b, int off, int len) {
        if (len > getRemainingBufferCapacity()) {
            return false;
        }
        while (len > 0) {
            final int writeLength;
            if (head <= tail) {
                writeLength = Math.min(len, ringBuffer.length - tail);
            } else {
                writeLength = Math.min(len, head - tail);
            }
            System.arraycopy(b, off, ringBuffer, tail, writeLength);
            tail += writeLength;
            off += writeLength;
            len -= writeLength;
            if (tail == ringBuffer.length) {
                tail = 0;
            }
        }
        return true;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

}
