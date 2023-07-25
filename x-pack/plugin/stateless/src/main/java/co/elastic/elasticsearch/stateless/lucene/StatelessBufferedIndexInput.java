/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.lucene;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Copy of {@link org.apache.lucene.store.BufferedIndexInput} that contains optimizations that haven't made it to the Lucene version used
 * by Elasticsearch yet. TODO: Remove this once a Lucene release that contains https://github.com/apache/lucene/pull/12453 makes it into ES
 */
public abstract class StatelessBufferedIndexInput extends IndexInput implements RandomAccessInput {

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

    public StatelessBufferedIndexInput(String resourceDesc) {
        this(resourceDesc, BUFFER_SIZE);
    }

    public StatelessBufferedIndexInput(String resourceDesc, IOContext context) {
        this(resourceDesc, bufferSize(context));
    }

    /** Inits BufferedIndexInput with a specific bufferSize */
    public StatelessBufferedIndexInput(String resourceDesc, int bufferSize) {
        super(resourceDesc);
        checkBufferSize(bufferSize);
        this.bufferSize = bufferSize;
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
                // The amount left to read is larger than the buffer
                // or we've been asked to not use our buffer -
                // there's no performance reason not to read it all
                // at once. Note that unlike the previous code of
                // this function, there is no need to do a seek
                // here, because there's no need to reread what we
                // had in the buffer.
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
            byte b = buffer.get();
            if (b >= 0) return b;
            int i = b & 0x7F;
            b = buffer.get();
            i |= (b & 0x7F) << 7;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7F) << 14;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7F) << 21;
            if (b >= 0) return i;
            b = buffer.get();
            // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
            i |= (b & 0x0F) << 28;
            if ((b & 0xF0) == 0) return i;
            throw new IOException("Invalid vInt detected (too many bits)");
        } else {
            return super.readVInt();
        }
    }

    @Override
    public final long readVLong() throws IOException {
        if (9 <= buffer.remaining()) {
            byte b = buffer.get();
            if (b >= 0) return b;
            long i = b & 0x7FL;
            b = buffer.get();
            i |= (b & 0x7FL) << 7;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 14;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 21;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 28;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 35;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 42;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 49;
            if (b >= 0) return i;
            b = buffer.get();
            i |= (b & 0x7FL) << 56;
            if (b >= 0) return i;
            throw new IOException("Invalid vLong detected (negative values disallowed)");
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
     * Expert: implements seek. Sets current position in this file, where the next {@link
     * #readInternal(ByteBuffer)} will occur.
     *
     * @see #readInternal(ByteBuffer)
     */
    protected abstract void seekInternal(long pos) throws IOException;

    @Override
    public StatelessBufferedIndexInput clone() {
        StatelessBufferedIndexInput clone = (StatelessBufferedIndexInput) super.clone();

        clone.buffer = EMPTY_BYTEBUFFER;
        clone.bufferStart = getFilePointer();

        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return wrap(sliceDescription, this, offset, length);
    }

    /** Returns default buffer sizes for the given {@link IOContext} */
    public static int bufferSize(IOContext context) {
        switch (context.context) {
            case MERGE:
                return MERGE_BUFFER_SIZE;
            case DEFAULT:
            case FLUSH:
            case READ:
            default:
                return BUFFER_SIZE;
        }
    }

    /**
     * Wraps a portion of another IndexInput with buffering.
     *
     * <p><b>Please note:</b> This is in most cases ineffective, because it may double buffer!
     */
    public static StatelessBufferedIndexInput wrap(String sliceDescription, IndexInput other, long offset, long length) {
        return new SlicedIndexInput(sliceDescription, other, offset, length);
    }

    /** Implementation of an IndexInput that reads from a portion of a file. */
    private static final class SlicedIndexInput extends StatelessBufferedIndexInput {
        IndexInput base;
        long fileOffset;
        long length;

        SlicedIndexInput(String sliceDescription, IndexInput base, long offset, long length) {
            super(
                (sliceDescription == null) ? base.toString() : (base.toString() + " [slice=" + sliceDescription + "]"),
                StatelessBufferedIndexInput.BUFFER_SIZE
            );
            if (offset < 0 || length < 0 || offset + length > base.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + base);
            }
            this.base = base.clone();
            this.fileOffset = offset;
            this.length = length;
        }

        @Override
        public SlicedIndexInput clone() {
            SlicedIndexInput clone = (SlicedIndexInput) super.clone();
            clone.base = base.clone();
            clone.fileOffset = fileOffset;
            clone.length = length;
            return clone;
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            long start = getFilePointer();
            if (start + b.remaining() > length) {
                throw new EOFException("read past EOF: " + this);
            }
            base.seek(fileOffset + start);
            base.readBytes(b.array(), b.position(), b.remaining());
            b.position(b.position() + b.remaining());
        }

        @Override
        protected void seekInternal(long pos) {}

        @Override
        public void close() throws IOException {
            base.close();
        }

        @Override
        public long length() {
            return length;
        }
    }
}
