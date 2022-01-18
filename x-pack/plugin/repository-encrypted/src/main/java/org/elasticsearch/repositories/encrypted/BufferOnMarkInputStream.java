/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.encrypted;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A {@code BufferOnMarkInputStream} adds the {@code mark} and {@code reset} functionality to another input stream.
 * All the bytes read or skipped following a {@link #mark(int)} call are also stored in a fixed-size internal array
 * so they can be replayed following a {@link #reset()} call. The size of the internal buffer is specified at construction
 * time. It is an error (throws {@code IllegalArgumentException}) to specify a larger {@code readlimit} value as an argument
 * to a {@code mark} call.
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
public final class BufferOnMarkInputStream extends InputStream {

    /**
     * the underlying input stream supplying the actual bytes to read
     */
    final InputStream source;
    /**
     * The fixed capacity buffer used to store the bytes following a {@code mark} call on the input stream,
     * and which are then replayed after the {@code reset} call.
     * The buffer permits appending bytes which can then be read, possibly multiple times, by also
     * supporting the mark and reset operations on its own.
     * Reading will <b>not</b> discard the bytes just read. Subsequent reads will return the
     * next bytes, but the bytes can be replayed by reading after calling {@code reset}.
     * The {@code mark} operation is used to adjust the position of the reset return position to the current
     * read position and also discard the bytes read before.
     */
    final RingBuffer ringBuffer; // package-protected for tests
    /**
     * {@code true} when the result of a read or a skip from the underlying source stream must also be stored in the buffer
     */
    boolean storeToBuffer; // package-protected for tests
    /**
     * {@code true} when the returned bytes must come from the buffer and not from the underlying source stream
     */
    boolean replayFromBuffer; // package-protected for tests
    /**
     * {@code true} when this stream is closed and any further calls throw IOExceptions
     */
    boolean closed; // package-protected for tests

    /**
     * Creates a {@code BufferOnMarkInputStream} that buffers a maximum of {@code bufferSize} elements
     * from the wrapped input stream {@code source} in order to support {@code mark} and {@code reset}.
     * The {@code bufferSize} is the maximum value for the {@code mark} readlimit argument.
     *
     * @param source the underlying input buffer
     * @param bufferSize the number of bytes that can be stored after a call to mark
     */
    public BufferOnMarkInputStream(InputStream source, int bufferSize) {
        this.source = source;
        this.ringBuffer = new RingBuffer(bufferSize);
        this.storeToBuffer = this.replayFromBuffer = false;
        this.closed = false;
    }

    /**
     * Reads up to {@code len} bytes of data into an array of bytes from this
     * input stream. If {@code len} is zero, then no bytes are read and {@code 0}
     * is returned; otherwise, there is an attempt to read at least one byte.
     * If the contents of the stream must be replayed following a {@code reset}
     * call, the call will return buffered bytes which have been returned in a previous
     * call. Otherwise it forwards the read call to the underlying source input stream.
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
        if (replayFromBuffer) {
            int bytesRead = ringBuffer.read(b, off, len);
            if (bytesRead == 0) {
                // rewinding is complete, no more bytes to replay
                replayFromBuffer = false;
            } else {
                return bytesRead;
            }
        }
        int bytesRead = source.read(b, off, len);
        if (bytesRead <= 0) {
            return bytesRead;
        }
        // if mark has been previously called, buffer all the read bytes
        if (storeToBuffer) {
            if (bytesRead > ringBuffer.getAvailableToWriteByteCount()) {
                // can not fully write to buffer
                // invalidate mark
                storeToBuffer = false;
                // empty buffer
                ringBuffer.clear();
            } else {
                ringBuffer.write(b, off, bytesRead);
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
        if (false == storeToBuffer) {
            // integrity check of the replayFromBuffer state variable
            if (replayFromBuffer) {
                throw new IllegalStateException("Reset cannot be called without a preceding mark invocation");
            }
            // if mark has not been called, no storing to the buffer is required
            return source.skip(n);
        }
        long remaining = n;
        int size = (int) Math.min(2048, remaining);
        byte[] skipBuffer = new byte[size];
        while (remaining > 0) {
            // skipping translates to a read so that the skipped bytes are stored in the buffer,
            // so they can possibly be replayed after a reset
            int bytesRead = read(skipBuffer, 0, (int) Math.min(size, remaining));
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
        if (replayFromBuffer) {
            bytesAvailable += ringBuffer.getAvailableToReadByteCount();
        }
        bytesAvailable += source.available();
        return bytesAvailable;
    }

    /**
     * Tests if this input stream supports the {@code mark} and {@code reset} methods.
     * This always returns {@code true}.
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Marks the current position in this input stream. A subsequent call to
     * the {@code reset} method repositions this stream at the last marked
     * position so that subsequent reads re-read the same bytes. The bytes
     * read or skipped following a {@code mark} call will be buffered internally
     * and any previously buffered bytes are discarded.
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
        // instance can accommodate in the fixed ring buffer
        if (readlimit > ringBuffer.getBufferSize()) {
            throw new IllegalArgumentException(
                "Readlimit value [" + readlimit + "] exceeds the maximum value of [" + ringBuffer.getBufferSize() + "]"
            );
        } else if (readlimit < 0) {
            throw new IllegalArgumentException("Readlimit value [" + readlimit + "] cannot be negative");
        }
        if (closed) {
            return;
        }
        // signal that further read or skipped bytes must be stored to the buffer
        storeToBuffer = true;
        if (replayFromBuffer) {
            // the mark operation while replaying after a reset
            // this only discards the previously buffered bytes before the current position
            // as well as updates the mark position in the buffer
            ringBuffer.mark();
        } else {
            // any previously stored bytes are discarded because mark only has to retain bytes from this position on
            ringBuffer.clear();
        }
    }

    /**
     * Repositions this stream to the position at the time the {@code mark} method was last called on this input stream.
     * It throws an {@code IOException} if {@code mark} has not yet been called on this instance.
     * Internally, this resets the buffer to the last mark position and signals that further reads (and skips)
     * on this input stream must return bytes from the buffer and not from the underlying source stream.
     *
     * @throws IOException if the stream has been closed or the number of bytes
     *                     read since the last mark call exceeded {@link #getMaxMarkReadlimit()}
     * @see java.io.InputStream#mark(int)
     */
    @Override
    public void reset() throws IOException {
        ensureOpen();
        if (false == storeToBuffer) {
            throw new IOException("Mark not called or has been invalidated");
        }
        // signal that further reads/skips must be satisfied from the buffer and not from the underlying source stream
        replayFromBuffer = true;
        // position the buffer's read pointer back to the last mark position
        ringBuffer.reset();
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
            source.close();
        }
    }

    /**
     * Returns the maximum value for the {@code readlimit} argument of the {@link #mark(int)} method.
     * This is the value of the {@code bufferSize} constructor argument and represents the maximum number
     * of bytes that can be internally buffered (so they can be replayed after the reset call).
     */
    public int getMaxMarkReadlimit() {
        return ringBuffer.getBufferSize();
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

    /**
     * This buffer is used to store all the bytes read or skipped after the last {@link BufferOnMarkInputStream#mark(int)}
     * invocation.
     * <p>
     * The latest bytes written to the ring buffer are appended following the previous ones.
     * Reading back the bytes advances an internal pointer so that subsequent read calls return subsequent bytes.
     * However, read bytes are not discarded. The same bytes can be re-read following the {@link #reset()} invocation.
     * {@link #reset()} permits re-reading the bytes since the last {@link #mark()}} call, or since the buffer instance
     * has been created or the {@link #clear()} method has been invoked.
     * Calling {@link #mark()} will discard all bytes read before, and calling {@link #clear()} will discard all the
     * bytes (new bytes must be written otherwise reading will return {@code 0} bytes).
     */
    static class RingBuffer {

        /**
         * This holds the size of the buffer which is lazily allocated on the first {@link #write(byte[], int, int)} invocation
         */
        private final int bufferSize;
        /**
         * The array used to store the bytes to be replayed upon a reset call.
         */
        byte[] buffer; // package-protected for tests
        /**
         * The start offset (inclusive) for the bytes that must be re-read after a reset call. This offset is advanced
         * by invoking {@link #mark()}
         */
        int head; // package-protected for tests
        /**
         * The end offset (exclusive) for the bytes that must be re-read after a reset call. This offset is advanced
         * by writing to the ring buffer.
         */
        int tail; // package-protected for tests
        /**
         * The offset of the bytes to return on the next read call. This offset is advanced by reading from the ring buffer.
         */
        int position; // package-protected for tests

        /**
         * Creates a new ring buffer instance that can store a maximum of {@code bufferSize} bytes.
         * More bytes are stored by writing to the ring buffer, and bytes are discarded from the buffer by the
         * {@code mark} and {@code reset} method invocations.
         */
        RingBuffer(int bufferSize) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("The buffersize constructor argument must be a strictly positive value");
            }
            this.bufferSize = bufferSize;
        }

        /**
         * Returns the maximum number of bytes that this buffer can store.
         */
        int getBufferSize() {
            return bufferSize;
        }

        /**
         * Rewind back to the read position of the last {@link #mark()} or {@link #reset()}. The next
         * {@link RingBuffer#read(byte[], int, int)} call will return the same bytes that the read
         * call after the last {@link #mark()} did.
         */
        void reset() {
            position = head;
        }

        /**
         * Mark the current read position. Any previously read bytes are discarded from the ring buffer,
         * i.e. they cannot be re-read, but this frees up space for writing other bytes.
         * All the following {@link RingBuffer#read(byte[], int, int)} calls will revert back to this position.
         */
        void mark() {
            head = position;
        }

        /**
         * Empties out the ring buffer, discarding all the bytes written to it, i.e. any following read calls don't
         * return any bytes.
         */
        void clear() {
            head = position = tail = 0;
        }

        /**
         * Copies up to {@code len} bytes from the ring buffer and places them in the {@code b} array starting at offset {@code off}.
         * This advances the internal pointer of the ring buffer so that a subsequent call will return the following bytes, not the
         * same ones (see {@link #reset()}).
         * Exactly {@code len} bytes are copied from the ring buffer, but no more than {@link #getAvailableToReadByteCount()}; i.e.
         * if {@code len} is greater than the value returned by {@link #getAvailableToReadByteCount()} this reads all the remaining
         * available bytes (which could be {@code 0}).
         * This returns the exact count of bytes read (the minimum of {@code len} and the value of {@code #getAvailableToReadByteCount}).
         *
         * @param b   the array where to place the bytes read
         * @param off the offset in the array where to start placing the bytes read (i.e. first byte is stored at b[off])
         * @param len the maximum number of bytes to read
         * @return the number of bytes actually read
         */
        int read(byte[] b, int off, int len) {
            Objects.requireNonNull(b);
            Objects.checkFromIndexSize(off, len, b.length);
            if (position == tail || len == 0) {
                return 0;
            }
            // the number of bytes to read
            final int readLength;
            if (position <= tail) {
                readLength = Math.min(len, tail - position);
            } else {
                // the ring buffer contains elements that wrap around the end of the array
                readLength = Math.min(len, buffer.length - position);
            }
            System.arraycopy(buffer, position, b, off, readLength);
            // update the internal pointer with the bytes read
            position += readLength;
            if (position == buffer.length) {
                // pointer wrap around
                position = 0;
                // also read the remaining bytes after the wrap around
                return readLength + read(b, off + readLength, len - readLength);
            }
            return readLength;
        }

        /**
         * Copies <b>exactly</b> {@code len} bytes from the array {@code b}, starting at offset {@code off}, into the ring buffer.
         * The bytes are appended after the ones written in the same way by a previous call, and are available to
         * {@link #read(byte[], int, int)} immediately.
         * This throws {@code IllegalArgumentException} if the ring buffer does not have enough space left.
         * To get the available capacity left call {@link #getAvailableToWriteByteCount()}.
         *
         * @param b the array from which to copy the bytes into the ring buffer
         * @param off the offset of the first element to copy
         * @param len the number of elements to copy
         */
        void write(byte[] b, int off, int len) {
            Objects.requireNonNull(b);
            Objects.checkFromIndexSize(off, len, b.length);
            // allocate internal buffer lazily
            if (buffer == null && len > 0) {
                // "+ 1" for the full-buffer sentinel element
                buffer = new byte[bufferSize + 1];
                head = position = tail = 0;
            }
            if (len > getAvailableToWriteByteCount()) {
                throw new IllegalArgumentException("Not enough remaining space in the ring buffer");
            }
            while (len > 0) {
                final int writeLength;
                if (head <= tail) {
                    writeLength = Math.min(len, buffer.length - tail - (head == 0 ? 1 : 0));
                } else {
                    writeLength = Math.min(len, head - tail - 1);
                }
                if (writeLength <= 0) {
                    throw new IllegalStateException("No space left in the ring buffer");
                }
                System.arraycopy(b, off, buffer, tail, writeLength);
                tail += writeLength;
                off += writeLength;
                len -= writeLength;
                if (tail == buffer.length) {
                    tail = 0;
                    // tail wrap-around overwrites head
                    if (head == 0) {
                        throw new IllegalStateException("Possible overflow of the ring buffer");
                    }
                }
            }
        }

        /**
         * Returns the number of bytes that can be written to this ring buffer before it becomes full
         * and will not accept further writes. Be advised that reading (see {@link #read(byte[], int, int)})
         * does not free up space because bytes can be re-read multiple times (see {@link #reset()});
         * ring buffer space can be reclaimed by calling {@link #mark()} or {@link #clear()}
         */
        int getAvailableToWriteByteCount() {
            if (buffer == null) {
                return bufferSize;
            }
            if (head == tail) {
                return buffer.length - 1;
            } else if (head < tail) {
                return buffer.length - tail + head - 1;
            } else {
                return head - tail - 1;
            }
        }

        /**
         * Returns the number of bytes that can be read from this ring buffer before it becomes empty
         * and all subsequent {@link #read(byte[], int, int)} calls will return {@code 0}. Writing
         * more bytes (see {@link #write(byte[], int, int)}) will obviously increase the number of
         * bytes available to read. Calling {@link #reset()} will also increase the available byte
         * count because the following reads will go over again the same bytes since the last
         * {@code mark} call.
         */
        int getAvailableToReadByteCount() {
            if (buffer == null) {
                return 0;
            }
            if (head <= tail) {
                return tail - position;
            } else if (position >= head) {
                return buffer.length - position + tail;
            } else {
                return tail - position;
            }
        }

    }

}
