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
 * A {@code BufferOnMarkInputStream} adds the mark and reset functionality to another input stream.
 * All the bytes read or skipped following a {@link #mark(int)} call are also stored in a fixed-size internal array
 * so they can be replayed following a {@link #reset()} call. The size of the internal buffer is specified at construction
 * time. It is an error (throws {@code IllegalArgumentException}) to specify a larger {@code readlimit} value as an argument
 * to a mark call.
 * <p>
 * Unlike the {@link java.io.BufferedInputStream} this only buffers upon a {@link #mark(int)} call,
 * i.e. if {@code mark} is never called this is equivalent to a bare pass-through {@link FilterInputStream}.
 * Moreover, this does not buffer in advance, so the amount of bytes read from this input stream, at any time, is equal to the amount
 * read from the underlying stream (provided that reset has not been called, in which case bytes are replayed from the internal buffer
 * and no bytes are read from the underlying stream).
 * <p>
 * Close will also close the underlying stream and any subsequent {@code read}, {@code skip}, {@code available} and
 * {@code reset} calls will throw {@code IOException}s.
 * <p>
 * This is NOT thread-safe, multiple threads sharing a single instance must synchronize access.
 */
public final class BufferOnMarkInputStream extends FilterInputStream {

    // all protected for tests
    protected final int bufferSize;
    /**
     * The array used to store the bytes to be replayed upon a reset call.
     * The buffer portion that stores valid bytes, which must be returned by the read calls after a reset call,
     * is demarcated by a {@code head} (inclusive) and a {@code tail} offset (exclusive). The offsets wrap around,
     * i.e. if the {@code tail} offset is smaller than the {@code head} offset, then the portion of valid bytes
     * is that from the {@code head} offset until the end of the buffer array and from the start of the array
     * until the {@code tail} offset. The buffer is empty when both the {@code head} and the {@code tail} offsets
     * are equal. The buffer is full if it stores {@code bufferSize} elements.
     * To avoid mixing up the two states, the actual allocated size of the array is {@code bufferSize + 1}.
     */
    protected byte[] ringBuffer;
    /**
     * The inclusive start offset of the bytes that must be replayed after a reset call.
     */
    protected int head;
    /**
     * The exclusive end offset of the bytes that must be replayed after a reset call.
     */
    protected int tail;
    /**
     * The current offset of the next byte to be returned from the buffer for the reads following a reset.
     * This is defined only when {@code resetCalled} is {@code true}.
     */
    protected int position;
    /**
     * {@code true} when the result of a read or skip from the underlying stream must also be stored in the buffer
     */
    protected boolean markCalled;
    /**
     * {@code true} when the returned bytes must come from the buffer and not from the underlying stream
     */
    protected boolean resetCalled;
    protected boolean closed;

    /**
     * Creates a {@code BufferOnMarkInputStream} that buffers a maximum of {@code bufferSize} elements.
     * The {@code bufferSize} is the maximum value for the mark readlimit.
     *
     * @param in the underlying input buffer
     * @param bufferSize the number of bytes that can be stored after a call to mark
     */
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

    /**
     * Reads up to {@code len} bytes of data into an array of bytes from this
     * input stream. If {@code len} is zero, then no bytes are read and {@cpde 0}
     * is returned; otherwise, there is an attempt to read at least one byte.
     * The read will return buffered bytes, which have been returned in a previous
     * call as well, if the contents of the stream must be replayed following a
     * reset call; otherwise it forwards the call to the underlying stream.
     * If no byte is available because there are no more bytes to replay following
     * a reset (if a reset was called) and the underlying stream is exhausted, the
     * value {@code -1} is returned; otherwise, at least one byte is read and stored
     * into {@code b}, starting at offset {@code off}.
     *
     * @param   b     the buffer into which the data is read.
     * @param   off   the start offset in the destination array {@code b}
     * @param   len   the maximum number of bytes read.
     * @return  the total number of bytes read into the buffer, or
     *          {@code -1} if there is no more data because the end of
     *          the stream has been reached.
     * @throws  NullPointerException If {@code b} is {@code null}.
     * @throws  IndexOutOfBoundsException If {@code off} is negative,
     * {@code len} is negative, or {@code len} is greater than
     * {@code b.length - off}
     * @throws  IOException if this stream has been closed or an I/O error occurs on the underlying stream.
     * @see     java.io.InputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        // firstly try reading any buffered bytes in case this read call is part of a rewind following a reset call
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

    /**
     * Reads the next byte of data from this input stream. The value
     * byte is returned as an {@code int} in the range
     * {@code 0} to {@code 255}. If no byte is available
     * because the end of the stream has been reached, the value
     * {@code -1} is returned. The end of the stream is reached if the
     * end of the underlying stream is reached, and reset has not been
     * called or there are no more bytes to replay following a reset.
     * This method blocks until input data is available, the end of
     * the stream is detected, or an exception is thrown.
     *
     * @return     the next byte of data, or {@code -1} if the end of the
     *             stream is reached.
     * @exception  IOException  if this stream has been closed or an I/O error occurs on the underlying stream.
     * @see        BufferOnMarkInputStream#read(byte[], int, int)
     */
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

    /**
     * Skips over and discards {@code n} bytes of data from the
     * input stream. The {@code skip} method may, for a variety of
     * reasons, end up skipping over some smaller number of bytes,
     * possibly {@code 0}. The actual number of bytes skipped is
     * returned.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @throws     IOException if this stream is closed, or if {@code in.skip(n)} throws an IOException or,
     * in the case that {@code mark} is called, if BufferOnMarkInputStream#read(byte[], int, int) throws an IOException
     */
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

    /**
     * Returns an estimate of the number of bytes that can be read (or
     * skipped over) from this input stream without blocking by the next
     * caller of a method for this input stream. The next caller might be
     * the same thread or another thread. A single read or skip of this
     * many bytes will not block, but may read or skip fewer bytes.
     *
     * @return     an estimate of the number of bytes that can be read (or skipped
     *             over) from this input stream without blocking.
     * @exception  IOException  if this stream is closed or if {@code in.available()} throws an IOException
     */
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

    /**
     * Marks the current position in this input stream. A subsequent call to
     * the {@code reset} method repositions this stream at the last marked
     * position so that subsequent reads re-read the same bytes.
     * <p>
     * The {@code readlimit} arguments tells this input stream to
     * allow that many bytes to be read before the mark position can be
     * invalidated. The {@code readlimit} argument value must be smaller than
     * the {@code bufferSize} constructor argument value, as returned by
     * {@link #getMaxMarkReadlimit()}.
     * <p>
     * The invalidation of the mark position when the read count exceeds the read
     * limit is not currently enforced. A mark position is invalidated when the
     * read count exceeds the maximum read limit, as returned by
     * {@link #getMaxMarkReadlimit()}.
     *
     * @param   readlimit   the maximum limit of bytes that can be read before
     *                      the mark position can be invalidated.
     * @see     BufferOnMarkInputStream#reset()
     * @see     java.io.InputStream#mark(int)
     */
    @Override
    public void mark(int readlimit) {
        // readlimit is otherwise ignored but this defensively fails if the caller is expecting to be able to mark/reset more than this
        // instance can accommodate in the ring mark buffer
        if (readlimit > bufferSize) {
            throw new IllegalArgumentException("Readlimit value [" + readlimit + "] exceeds the maximum value of [" + bufferSize + "]");
        } else if (readlimit < 0) {
            throw new IllegalArgumentException("Readlimit value [" + readlimit + "] cannot be negative");
        }
        if (closed) {
            return;
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

    /**
     * Tests if this input stream supports the {@code mark} and
     * {@code reset} methods. This always returns {@code true}.
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Repositions this stream to the position at the time the
     * {@code mark} method was last called on this input stream.
     * Subsequent read calls will return the same bytes in the same
     * order since the point of the {@code mark} call. Naturally,
     * {@code mark} can be invoked at any moment, even after a
     * {@code reset}.
     *
     * @throws IOException  if the stream has been closed or the number of bytes
     * read since the last mark call exceeded {@link #getMaxMarkReadlimit()}
     * @see     java.io.InputStream#mark(int)
     */
    @Override
    public void reset() throws IOException {
        ensureOpen();
        if (false == markCalled) {
            throw new IOException("Mark not called or has been invalidated");
        }
        resetCalled = true;
        position = head;
    }

    /**
     * Closes this input stream as well as the underlying stream.
     *
     * @exception  IOException  if an I/O error occurs while closing the underlying stream.
     */
    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            ringBuffer = null;
            in.close();
        }
    }

    /**
     * Returns the maximum value for the {@code readlimit} argument of the {@link #mark(int)} method.
     * This is the same as the {@code bufferSize} constructor argument.
     */
    public int getMaxMarkReadlimit() {
        return bufferSize;
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

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
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

    // only for tests
    protected InputStream getWrapped() {
        return in;
    }

}
