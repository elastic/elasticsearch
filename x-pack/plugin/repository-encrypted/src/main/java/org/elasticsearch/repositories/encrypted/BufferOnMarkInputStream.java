/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;


public final class BufferOnMarkInputStream extends FilterInputStream {

    // protected for tests
    protected final int bufferSize;
    protected byte[] ringBuffer;
    protected int head;
    protected int tail;
    protected int position;
    protected boolean markCalled;
    protected boolean resetCalled;
    protected boolean closed;

    public BufferOnMarkInputStream(InputStream in, int bufferSize) {
        super(Objects.requireNonNull(in));
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("The buffersize constructor argument must be a strictly positive value");
        }
        this.bufferSize = bufferSize;
        // the ring buffer is lazily allocated upon the first mark call
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
        // firstly try reading any buffered bytes in case this read call is part of rewind following a reset call
        if (resetCalled) {
            int bytesRead = readFromBuffer(b, off, len);
            if (bytesRead == 0) {
                // rewinding is complete, no more bytes to replay
                resetCalled = false;
            } else {
                return bytesRead;
            }
        }
        int bytesRead = in.read(b, off, len);
        if (bytesRead <= 0) {
            return bytesRead;
        }
        // if mark has been previously called, buffer all the read bytes
        if (markCalled) {
            if (bytesRead > getRemainingBufferCapacity()) {
                // could not fully write to buffer, invalidate mark
                markCalled = false;
                head = tail = position = 0;
            } else {
                writeToBuffer(b, off, bytesRead);
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
            if (resetCalled) {
                throw new IllegalStateException("Reset cannot be called without a preceding mark invocation");
            }
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
        // readlimit is otherwise ignored but this defensively fails if the caller is expecting to be able to mark/reset more than this
        // stream can accommodate
        if (readlimit > bufferSize) {
            throw new IllegalArgumentException("Readlimit value [" + readlimit + "] exceeds the maximum value of [" + bufferSize + "]");
        } else if (readlimit < 0) {
            throw new IllegalArgumentException("Readlimit value [" + readlimit + "] cannot be negative");
        }
        markCalled = true;
        // lazily allocate the mark ring buffer
        if (ringBuffer == null) {
            // "+ 1" for the full-buffer sentinel free element
            ringBuffer = new byte[bufferSize + 1];
            head = tail = position = 0;
        } else {
            if (resetCalled) {
                // mark after reset
                head = position;
            } else {
                // discard any leftovers in buffer
                head = tail = position = 0;
            }
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
        position = head;
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

    private void writeToBuffer(byte[] b, int off, int len) {
        while (len > 0) {
            final int writeLength;
            if (head <= tail) {
                writeLength = Math.min(len, ringBuffer.length - tail - (head == 0 ? 1 : 0));
            } else {
                writeLength = Math.min(len, head - tail - 1);
            }
            if (writeLength <= 0) {
                throw new IllegalStateException("No space left in the mark buffer");
            }
            System.arraycopy(b, off, ringBuffer, tail, writeLength);
            tail += writeLength;
            off += writeLength;
            len -= writeLength;
            if (tail == ringBuffer.length) {
                tail = 0;
                // tail wrap-around overwrites head
                if (head == 0) {
                    throw new IllegalStateException("Possible overflow of the mark buffer");
                }
            }
        }
    }

    // protected for tests
    protected int getRemainingBufferCapacity() {
        if (ringBuffer == null) {
            return bufferSize;
        }
        if (head == tail) {
            return ringBuffer.length - 1;
        } else if (head < tail) {
            return ringBuffer.length - tail + head - 1;
        } else {
            return head - tail - 1;
        }
    }

    //protected for tests
    protected int getRemainingBufferToRead() {
        if (ringBuffer == null) {
            return 0;
        }
        if (head <= tail) {
            return tail - position;
        } else if (position >= head) {
            return ringBuffer.length - position + tail;
        } else {
            return tail - position;
        }
    }

    // protected for tests
    protected int getCurrentBufferCount() {
        if (ringBuffer == null) {
            return 0;
        }
        if (head <= tail) {
            return tail - head;
        } else {
            return ringBuffer.length - head + tail;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

    // only for tests
    protected InputStream getWrapped() {
        return in;
    }

}
