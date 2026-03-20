/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A buffered {@link IndexInput} implementation that wraps another {@link IndexInput}.
 * This combines the functionality of Lucene's {@code FilterIndexInput} (wrapping another input)
 * with {@code BufferedIndexInput} (providing buffering for efficient reads).
 * <p>
 * This is useful when you need to add buffering on top of an existing {@link IndexInput}
 * that may not be efficiently buffered, such as when reading from remote storage.
 */
public class BufferedFilterIndexInput extends IndexInput implements RandomAccessInput {

    private static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    /** Default buffer size */
    public static final int BUFFER_SIZE = 1024;

    /** Minimum buffer size allowed */
    public static final int MIN_BUFFER_SIZE = 8;

    /** The wrapped input */
    protected final IndexInput in;

    private final long length;
    private final int bufferSize;

    protected ByteBuffer buffer = EMPTY_BYTEBUFFER;
    private long bufferStart = 0; // position in file of buffer

    /**
     * Creates a buffered wrapper around the given {@link IndexInput} with the default buffer size.
     *
     * @param in the underlying input to wrap
     */
    public BufferedFilterIndexInput(IndexInput in) {
        this(in, BUFFER_SIZE);
    }

    /**
     * Creates a buffered wrapper around the given {@link IndexInput} with a specific buffer size.
     *
     * @param in the underlying input to wrap
     * @param bufferSize the buffer size to use
     */
    public BufferedFilterIndexInput(IndexInput in, int bufferSize) {
        super(in.toString());
        this.in = in;
        this.length = in.length();
        int bufSize = Math.max(MIN_BUFFER_SIZE, (int) Math.min(bufferSize, length));
        checkBufferSize(bufSize);
        this.bufferSize = bufSize;
    }

    /**
     * Private constructor for slicing.
     */
    private BufferedFilterIndexInput(IndexInput in, int bufferSize, long length) {
        super(in.toString());
        this.in = in;
        this.length = length;
        this.bufferSize = bufferSize;
    }

    private void checkBufferSize(int bufferSize) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException("bufferSize must be at least MIN_BUFFER_SIZE (got " + bufferSize + ")");
        }
    }

    /**
     * Returns the underlying {@link IndexInput}.
     */
    public IndexInput getDelegate() {
        return in;
    }

    @Override
    public final long length() {
        return length;
    }

    /**
     * Returns the buffer size.
     */
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public final byte readByte() throws IOException {
        if (buffer.hasRemaining() == false) {
            refill();
        }
        return buffer.get();
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        readBytes(b, offset, len, true);
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        int available = buffer.remaining();
        if (len <= available) {
            // the buffer contains enough data to satisfy this request
            if (len > 0) { // to allow b to be null if len is 0...
                buffer.get(b, offset, len);
            }
        } else {
            // the buffer does not have enough data. First serve all we've got.
            if (available > 0) {
                buffer.get(b, offset, available);
                offset += available;
                len -= available;
            }
            // and now, read the remaining 'len' bytes:
            if (useBuffer && len < bufferSize) {
                // If the amount left to read is small enough, and
                // we are allowed to use our buffer, do it in the usual
                // buffered way: fill the buffer and copy from it:
                refill();
                if (buffer.remaining() < len) {
                    // Throw an exception when refill() could not read len bytes:
                    buffer.get(b, offset, buffer.remaining());
                    throw new EOFException("read past EOF: " + this);
                } else {
                    buffer.get(b, offset, len);
                }
            } else {
                // The amount left to read is larger than the buffer or we've been asked to not use our buffer -
                // there's no performance reason not to read it all at once.
                long currentPos = bufferStart + buffer.position();
                long after = currentPos + len;
                if (after > length()) {
                    throw new EOFException("read past EOF: " + this);
                }
                in.seek(currentPos);
                in.readBytes(b, offset, len);
                bufferStart = after;
                buffer.limit(0); // trigger refill() on read
            }
        }
    }

    @Override
    public final short readShort() throws IOException {
        if (Short.BYTES <= buffer.remaining()) {
            return buffer.getShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public final int readInt() throws IOException {
        if (Integer.BYTES <= buffer.remaining()) {
            return buffer.getInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public final long readLong() throws IOException {
        if (Long.BYTES <= buffer.remaining()) {
            return buffer.getLong();
        } else {
            return super.readLong();
        }
    }

    @Override
    public final int readVInt() throws IOException {
        if (5 <= buffer.remaining()) {
            return ByteBufferStreamInput.readVInt(buffer);
        } else {
            return super.readVInt();
        }
    }

    @Override
    public final long readVLong() throws IOException {
        if (10 <= buffer.remaining()) {
            return ByteBufferStreamInput.readVLong(buffer);
        } else {
            return super.readVLong();
        }
    }

    // RandomAccessInput methods

    private long resolvePositionInBuffer(long pos, int width) throws IOException {
        long index = pos - bufferStart;
        if (index >= 0 && index <= buffer.limit() - width) {
            return index;
        }
        if (index < 0) {
            // if we're moving backwards, then try and fill up the previous page rather than
            // starting again at the current pos, to avoid successive backwards reads reloading
            // the same data over and over again. We also check that we can read `width`
            // bytes without going over the end of the buffer
            bufferStart = Math.max(bufferStart - bufferSize, pos + width - bufferSize);
            bufferStart = Math.max(bufferStart, 0);
            bufferStart = Math.min(bufferStart, pos);
        } else {
            // we're moving forwards, reset the buffer to start at pos
            bufferStart = pos;
        }
        buffer.limit(0); // trigger refill() on read
        in.seek(bufferStart);
        refill();
        return pos - bufferStart;
    }

    @Override
    public final byte readByte(long pos) throws IOException {
        long index = resolvePositionInBuffer(pos, Byte.BYTES);
        return buffer.get((int) index);
    }

    @Override
    public final short readShort(long pos) throws IOException {
        long index = resolvePositionInBuffer(pos, Short.BYTES);
        return buffer.getShort((int) index);
    }

    @Override
    public final int readInt(long pos) throws IOException {
        long index = resolvePositionInBuffer(pos, Integer.BYTES);
        return buffer.getInt((int) index);
    }

    @Override
    public final long readLong(long pos) throws IOException {
        long index = resolvePositionInBuffer(pos, Long.BYTES);
        return buffer.getLong((int) index);
    }

    // Bulk read methods

    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Float.BYTES, remainingDst);
            buffer.asFloatBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Float.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Long.BYTES, remainingDst);
            buffer.asLongBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Long.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readLong();
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Integer.BYTES, remainingDst);
            buffer.asIntBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Integer.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readInt();
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    protected void refill() throws IOException {
        long start = bufferStart + buffer.position();
        long end = start + bufferSize;
        if (end > length()) { // don't read past EOF
            end = length();
        }
        int newLength = (int) (end - start);
        if (newLength <= 0) {
            throw new EOFException("read past EOF: " + this);
        }

        if (buffer == EMPTY_BYTEBUFFER) {
            buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN); // allocate buffer lazily
            in.seek(bufferStart);
        }
        buffer.position(0);
        buffer.limit(newLength);
        bufferStart = start;
        in.seek(bufferStart);
        in.readBytes(buffer.array(), buffer.arrayOffset(), newLength);
        buffer.position(newLength);
        // Make sure buffer is in correct state
        assert buffer.order() == ByteOrder.LITTLE_ENDIAN : buffer.order();
        assert buffer.position() == newLength;
        buffer.flip();
    }

    @Override
    public final long getFilePointer() {
        return bufferStart + buffer.position();
    }

    @Override
    public final void seek(long pos) throws IOException {
        if (pos >= bufferStart && pos < (bufferStart + buffer.limit())) {
            buffer.position((int) (pos - bufferStart)); // seek within buffer
        } else {
            bufferStart = pos;
            buffer.limit(0); // trigger refill() on read
            in.seek(pos);
        }
    }

    @Override
    public IndexInput clone() {
        BufferedFilterIndexInput clone = (BufferedFilterIndexInput) super.clone();
        clone.buffer = EMPTY_BYTEBUFFER;
        clone.bufferStart = getFilePointer();
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length
                    + ": "
                    + this
            );
        }
        return new SlicedBufferedFilterIndexInput(in.slice(sliceDescription, offset, length), bufferSize);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    /**
     * A sliced view of a {@link BufferedFilterIndexInput}.
     */
    private static final class SlicedBufferedFilterIndexInput extends BufferedFilterIndexInput {

        SlicedBufferedFilterIndexInput(IndexInput slice, int bufferSize) {
            super(slice, Math.max(MIN_BUFFER_SIZE, (int) Math.min(bufferSize, slice.length())), slice.length());
        }

        @Override
        public IndexInput clone() {
            BufferedFilterIndexInput clone = (BufferedFilterIndexInput) super.clone();
            clone.buffer = EMPTY_BYTEBUFFER;
            clone.bufferStart = getFilePointer();
            return clone;
        }
    }
}
