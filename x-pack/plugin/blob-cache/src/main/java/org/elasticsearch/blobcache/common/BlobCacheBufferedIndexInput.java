/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.core.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Copy of {@link org.apache.lucene.store.BufferedIndexInput} that contains optimizations that haven't made it to the Lucene version used
 * by Elasticsearch yet or that are only applicable to Elasticsearch.
 * <p>
 * Deviates from Lucene's implementation slightly to fix a bug - see [NOTE: Missing Seek] below, and #98970 for more details.
 */
public abstract class BlobCacheBufferedIndexInput extends IndexInput implements RandomAccessInput {

    private static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    public static final int BUFFER_SIZE = 1024;

    /** Minimum buffer size allowed */
    public static final int MIN_BUFFER_SIZE = 8;

    // The normal read buffer size defaults to 1024, but
    // increasing this during merging seems to yield
    // performance gains. However we don't want to increase
    // it too much because there are quite a few
    // BufferedIndexInputs created during merging. See
    // LUCENE-888 for details.
    public static final int MERGE_BUFFER_SIZE = 4096;

    private final long length;

    private final int bufferSize;

    protected ByteBuffer buffer = EMPTY_BYTEBUFFER;

    private long bufferStart = 0; // position in file of buffer

    @Override
    public final byte readByte() throws IOException {
        if (buffer.hasRemaining() == false) {
            refill();
        }
        return buffer.get();
    }

    public BlobCacheBufferedIndexInput(String resourceDesc, IOContext context, long length) {
        this(resourceDesc, bufferSize(context), length);
    }

    /** Inits BufferedIndexInput with a specific bufferSize */
    public BlobCacheBufferedIndexInput(String resourceDesc, int bufferSize, long length) {
        super(resourceDesc);
        int bufSize = Math.max(MIN_BUFFER_SIZE, (int) Math.min(bufferSize, length));
        checkBufferSize(bufSize);
        this.bufferSize = bufSize;
        this.length = length;
    }

    @Override
    public final long length() {
        return length;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    private void checkBufferSize(int bufferSize) {
        if (bufferSize < MIN_BUFFER_SIZE) throw new IllegalArgumentException(
            "bufferSize must be at least MIN_BUFFER_SIZE (got " + bufferSize + ")"
        );
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
            if (len > 0) // to allow b to be null if len is 0...
                buffer.get(b, offset, len);
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
                // The amount left to read is larger than the buffer or we've been asked to not use our buffer - there's no performance
                // reason not to read it all at once.
                if (buffer == EMPTY_BYTEBUFFER) {
                    // fresh clone, must seek
                    // [NOTE: Missing Seek] This deviates from Lucene's BufferedIndexInput implementation - see #98970
                    seekInternal(bufferStart);
                } // else there's no need to do a seek here because we are already positioned correctly

                long after = bufferStart + buffer.position() + len;
                if (after > length()) throw new EOFException("read past EOF: " + this);
                readInternal(ByteBuffer.wrap(b, offset, len));
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
        seekInternal(bufferStart);
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
        if (end > length()) // don't read past EOF
            end = length();
        int newLength = (int) (end - start);
        if (newLength <= 0) throw new EOFException("read past EOF: " + this);

        if (buffer == EMPTY_BYTEBUFFER) {
            buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN); // allocate buffer lazily
            seekInternal(bufferStart);
        }
        buffer.position(0);
        buffer.limit(newLength);
        bufferStart = start;
        readInternal(buffer);
        // Make sure sub classes don't mess up with the buffer.
        assert buffer.order() == ByteOrder.LITTLE_ENDIAN : buffer.order();
        assert buffer.remaining() == 0 : "should have thrown EOFException";
        assert buffer.position() == newLength;
        buffer.flip();
    }

    /**
     * Expert: implements buffer refill. Reads bytes from the current position in the input.
     *
     * @param b the buffer to read bytes into
     */
    protected abstract void readInternal(ByteBuffer b) throws IOException;

    @Override
    public final long getFilePointer() {
        return bufferStart + buffer.position();
    }

    @Override
    public final void seek(long pos) throws IOException {
        if (pos >= bufferStart && pos < (bufferStart + buffer.limit())) buffer.position((int) (pos - bufferStart)); // seek within buffer
        else {
            bufferStart = pos;
            buffer.limit(0); // trigger refill() on read
            seekInternal(pos);
        }
    }

    /**
     * Try slicing {@code sliceLength} bytes from the given {@code sliceOffset} from the currently buffered.
     * If this input's buffer currently contains the sliced range fully, then it is copied to a newly allocated byte array and an array
     * backed index input is returned. Using this method will never allocate a byte array larger than the buffer size and will result in
     * a potentially  more memory efficient {@link IndexInput} than slicing to a new {@link BlobCacheBufferedIndexInput} and will prevent
     * any further reads from input that is wrapped by this instance.
     *
     * @param name slice name
     * @param sliceOffset slice offset
     * @param sliceLength slice length
     * @return a byte array backed index input if slicing directly from the buffer worked or {@code null} otherwise
     */
    @Nullable
    protected final ByteArrayIndexInput trySliceBuffer(String name, long sliceOffset, long sliceLength) {
        if (ByteRange.of(bufferStart, bufferStart + buffer.limit()).contains(sliceOffset, sliceOffset + sliceLength)) {
            final byte[] bytes = new byte[(int) sliceLength];
            buffer.get(Math.toIntExact(sliceOffset - bufferStart), bytes, 0, bytes.length);
            return new ByteArrayIndexInput(name, bytes);
        }
        return null;
    }

    @Nullable
    protected final IndexInput tryCloneBuffer() {
        if (buffer.limit() == length && bufferStart == 0) {
            var clone = trySliceBuffer(super.toString(), 0, length);
            if (clone != null) {
                try {
                    clone.seek(buffer.position());
                } catch (IOException ioe) {
                    assert false : ioe;
                    throw new UncheckedIOException(ioe);
                }
                return clone;
            }
        }
        return null;
    }

    /**
     * Expert: implements seek. Sets current position in this file, where the next {@link
     * #readInternal(ByteBuffer)} will occur.
     *
     * @see #readInternal(ByteBuffer)
     */
    protected abstract void seekInternal(long pos) throws IOException;

    @Override
    public IndexInput clone() {
        BlobCacheBufferedIndexInput clone = (BlobCacheBufferedIndexInput) super.clone();

        clone.buffer = EMPTY_BYTEBUFFER;
        clone.bufferStart = getFilePointer();

        return clone;
    }

    /** Returns default buffer sizes for the given {@link IOContext} */
    public static int bufferSize(IOContext context) {
        switch (context.context()) {
            case MERGE:
                return MERGE_BUFFER_SIZE;
            case DEFAULT:
            case FLUSH:
            default:
                return BUFFER_SIZE;
        }
    }
}
