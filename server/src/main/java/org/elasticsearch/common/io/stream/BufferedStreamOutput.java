/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.common.io.stream.StreamOutputHelper.putMultiByteVInt;
import static org.elasticsearch.common.io.stream.StreamOutputHelper.putVInt;

/**
 * Adapts a raw {@link OutputStream} into a rich {@link StreamOutput} for use with {@link Writeable} instances, using a buffer.
 * <p>
 * Similar to {@link OutputStreamStreamOutput} in function, but with different performance characteristics because it requires a buffer to
 * be acquired or allocated up-front. Apart from the costs of the buffer creation &amp; release a {@link BufferedStreamOutput} is likely
 * more performant than an {@link OutputStreamStreamOutput} because it writes all fields directly to its local buffer and only copies data
 * to the underlying stream when the buffer fills up.
 *
 * <hr>
 *
 * <h2>Wrapping a {@linkplain java.io.ByteArrayOutputStream}</h2>
 *
 * A {@link java.io.ByteArrayOutputStream} collects data in an underlying {@code byte[]} collector which typically doubles in size each time
 * it receives a write operation which exhausts the remaining space in the existing collector. This works well if wrapped with a
 * {@link BufferedStreamOutput} especially if the object is expected to be small enough to fit entirely into the buffer, because then
 * there's only one slightly-oversized {@code byte[]} allocation to create the collector, plus another right-sized {@code byte[]} allocation
 * to extract the result, assuming the buffer is not allocated afresh each time. However, subsequent flushes may need to allocate a larger
 * collector, copying over the existing data to the new collector, so this can perform badly for larger objects. For objects larger than
 * half the G1 region size (8MiB on a 32GiB heap) each reallocation will require a
 * <a href="https://www.oracle.com/technical-resources/articles/java/g1gc.html">humongous allocation</a> which can be stressful for the
 * garbage collector, so it's better to split the bytes into several smaller pages using utilities such as {@linkplain BytesStreamOutput},
 * {@linkplain ReleasableBytesStreamOutput} or {@linkplain RecyclerBytesStreamOutput} in those cases.
 * <p>
 * A {@link BufferedStreamOutput} around a {@link java.io.ByteArrayOutputStream} is also good if the in-memory serialized representation
 * will have a long lifetime, because the resulting {@code byte[]} is exactly the correct size. When using other approaches such as a
 * {@linkplain BytesStreamOutput}, {@linkplain ReleasableBytesStreamOutput} or {@linkplain RecyclerBytesStreamOutput} there will be some
 * amount of unused overhead bytes which may be particularly undesirable for long-lived objects.
 * <p>
 * An {@link OutputStreamStreamOutput} wrapper is almost certainly worse than a {@link BufferedStreamOutput} because it will make the
 * {@link java.io.ByteArrayOutputStream} perform significantly more allocations and copies until the collecting buffer gets large enough.
 * Most writes to a {@link OutputStreamStreamOutput} use a thread-local intermediate buffer (itself somewhat expensive) and then copy that
 * intermediate buffer directly to the output.
 * <p>
 * Any memory allocated in this way is untracked by the {@link org.elasticsearch.common.breaker} subsystem unless the caller takes steps to
 * add this tracking themselves.
 */
public class BufferedStreamOutput extends StreamOutput {

    private final OutputStream delegate;
    private final byte[] buffer;
    private final int startPosition;
    private final int endPosition;
    private int position;
    private long flushedBytes;
    private boolean isClosed;

    /**
     * Wrap the given stream, using the given {@link BytesRef} for the buffer. It is the caller's responsibility to make sure that nothing
     * else modifies the buffer's contents while this object is in use.
     */
    public BufferedStreamOutput(OutputStream delegate, BytesRef buffer) {
        this.delegate = Objects.requireNonNull(delegate);
        this.buffer = Objects.requireNonNull(buffer.bytes);
        this.startPosition = buffer.offset;
        this.endPosition = buffer.offset + buffer.length;
        this.position = startPosition;
        assert buffer.length >= 1 : buffer.length + " is too short";
    }

    @Override
    public long position() {
        return flushedBytes + position - startPosition;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        int position = this.position;
        if (endPosition <= position) {
            flush();
            position = startPosition;
        }
        buffer[position] = b;
        this.position = position + 1;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        int position = this.position;
        if (length <= endPosition - position) {
            System.arraycopy(b, offset, buffer, position, length);
            this.position = position + length;
        } else {
            writeBytesOverflow(b, offset, length, startPosition, position, endPosition);
        }
    }

    // only called if the write operation exceeds the available buffer space by at least one byte
    private void writeBytesOverflow(byte[] b, int offset, int length, int startPosition, int position, int endPosition) throws IOException {
        // extracted so that writeBytes() inlines better; using args instead of fields is noticeably faster too
        int initialCopyLength = endPosition - position;
        assert 0 <= initialCopyLength && initialCopyLength < length;
        System.arraycopy(b, offset, buffer, position, initialCopyLength);
        offset += initialCopyLength;
        length -= initialCopyLength;
        final var bufferLength = endPosition - startPosition;
        delegate.write(buffer, startPosition, bufferLength);
        if (bufferLength <= length) {
            delegate.write(b, offset, length);
            this.flushedBytes += bufferLength + length;
            this.position = startPosition;
        } else {
            System.arraycopy(b, offset, buffer, startPosition, length);
            this.flushedBytes += bufferLength;
            this.position = startPosition + length;
        }
    }

    private int capacity() {
        // Probably faster to copy position into a local variable rather than using this method - TODO investigate further
        return endPosition - position;
    }

    @Override
    public void flush() throws IOException {
        innerFlush(startPosition, position);
        delegate.flush();
        assert assertTrashBuffer(); // ensure nobody else cares about the buffer contents by trashing its contents if assertions enabled
    }

    private void innerFlush(int startPosition, int position) throws IOException {
        // extracted so that flush inlines better; using args instead of field accesses is noticeably faster too
        int buffered = position - startPosition;
        if (0 < buffered) {
            delegate.write(buffer, startPosition, buffered);
            flushedBytes += buffered;
            this.position = startPosition;
        }
    }

    private boolean assertTrashBuffer() {
        // sequence of 0xa5 == 0b10100101 is not valid as a bool/vInt/vLong/... and unlikely to arise otherwise so might aid debugging
        Arrays.fill(buffer, startPosition, endPosition, (byte) 0xa5);
        return true;
    }

    @Override
    public void close() throws IOException {
        if (isClosed == false) {
            isClosed = true;
            flush();
            delegate.close();
        }
    }

    @Override
    public void writeShort(short i) throws IOException {
        if (Short.BYTES <= capacity()) {
            ByteUtils.writeShortBE(i, buffer, position);
            position += Short.BYTES;
        } else {
            writeShortBigEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeShortBigEndianWithBoundsChecks(short i) throws IOException {
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    @Override
    public void writeInt(int i) throws IOException {
        if (Integer.BYTES <= capacity()) {
            ByteUtils.writeIntBE(i, buffer, position);
            position += Integer.BYTES;
        } else {
            writeIntBigEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeIntBigEndianWithBoundsChecks(int i) throws IOException {
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    @Override
    public void writeIntLE(int i) throws IOException {
        if (Integer.BYTES <= capacity()) {
            ByteUtils.writeIntLE(i, buffer, position);
            position += Integer.BYTES;
        } else {
            writeIntLittleEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeIntLittleEndianWithBoundsChecks(int i) throws IOException {
        writeByte((byte) i);
        writeByte((byte) (i >> 8));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 24));
    }

    private static final int MAX_VINT_BYTES = 5;
    private static final int MAX_VLONG_BYTES = 9;
    private static final int MAX_ZLONG_BYTES = 10;
    private static final int MAX_CHAR_BYTES = 3;

    @Override
    public void writeVInt(int i) throws IOException {
        if (25 <= Integer.numberOfLeadingZeros(i)) {
            writeByte((byte) i);
        } else if (MAX_VINT_BYTES <= capacity()) {
            position = putMultiByteVInt(buffer, i, position);
        } else {
            writeVIntWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeVIntWithBoundsChecks(int i) throws IOException {
        while ((i & 0xFFFF_FF80) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    @Override
    public void writeVIntArray(int[] values) throws IOException {
        int position = this.position;
        if ((values.length + 1) * MAX_VINT_BYTES <= endPosition - position) {
            position = putVInt(buffer, values.length, position);
            for (var value : values) {
                position = putVInt(buffer, value, position);
            }
            this.position = position;
        } else {
            writeVIntArrayWithBoundsChecks(values);
        }
    }

    // slower (cold) path extracted to its own method to allow fast & hot path to be inlined
    private void writeVIntArrayWithBoundsChecks(int[] values) throws IOException {
        writeVInt(values.length);
        int i = 0;
        int position = this.position;
        int lastSafePosition = this.endPosition - MAX_VINT_BYTES;
        while (i < values.length) {
            while (i < values.length && position <= lastSafePosition) {
                position = putVInt(buffer, values[i++], position);
            }
            this.position = position;
            while (capacity() < MAX_VINT_BYTES && i < values.length) {
                writeVInt(values[i++]);
            }
            position = this.position;
        }
    }

    @Override
    void writeVLongNoCheck(long i) throws IOException {
        if (MAX_VLONG_BYTES <= capacity()) {
            int position = this.position;
            while ((i & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
                buffer[position++] = ((byte) ((i & 0x7F) | 0x80));
                i >>>= 7;
            }
            buffer[position++] = ((byte) i);
            this.position = position;
        } else {
            writeVLongWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeVLongWithBoundsChecks(long i) throws IOException {
        while ((i & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    @Override
    public void writeZLong(long i) throws IOException {
        long value = BitUtil.zigZagEncode(i);
        // NB not quite the same as writeVLongNoCheck because MAX_ZLONG_BYTES != MAX_VLONG_BYTES
        if (MAX_ZLONG_BYTES <= capacity()) {
            int position = this.position;
            while ((value & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
                buffer[position++] = ((byte) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
            buffer[position++] = ((byte) value);
            this.position = position;
        } else {
            writeVLongWithBoundsChecks(value);
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        if (Long.BYTES <= capacity()) {
            ByteUtils.writeLongBE(i, buffer, position);
            position += Long.BYTES;
        } else {
            writeLongBigEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeLongBigEndianWithBoundsChecks(long i) throws IOException {
        writeByte((byte) (i >> 56));
        writeByte((byte) (i >> 48));
        writeByte((byte) (i >> 40));
        writeByte((byte) (i >> 32));
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    @Override
    public void writeLongLE(long i) throws IOException {
        if (Long.BYTES <= capacity()) {
            ByteUtils.writeLongLE(i, buffer, position);
            position += Long.BYTES;
        } else {
            writeLongLittleEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeLongLittleEndianWithBoundsChecks(long i) throws IOException {
        writeByte((byte) i);
        writeByte((byte) (i >> 8));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 32));
        writeByte((byte) (i >> 40));
        writeByte((byte) (i >> 48));
        writeByte((byte) (i >> 56));
    }

    @Override
    public void writeString(String str) throws IOException {
        final int charCount = str.length();
        int position = this.position;
        if (MAX_VINT_BYTES + charCount * MAX_CHAR_BYTES <= endPosition - position) {
            position = putVInt(buffer, charCount, position);
            for (int i = 0; i < charCount; i++) {
                position = putCharUtf8(buffer, str.charAt(i), position);
            }
            this.position = position;
        } else {
            writeStringWithBoundsChecks(charCount, str);
        }
    }

    @Override
    public void writeOptionalString(String str) throws IOException {
        if (str == null) {
            writeByte((byte) 0);
        } else {
            final int charCount = str.length();
            int position = this.position;
            if (1 + MAX_VINT_BYTES + charCount * MAX_CHAR_BYTES <= endPosition - position) {
                buffer[position++] = (byte) 1;
                position = putVInt(buffer, charCount, position);
                for (int i = 0; i < charCount; i++) {
                    position = putCharUtf8(buffer, str.charAt(i), position);
                }
                this.position = position;
            } else {
                writeByte((byte) 1);
                writeStringWithBoundsChecks(charCount, str);
            }
        }
    }

    @Override
    public void writeGenericString(String str) throws IOException {
        final int charCount = str.length();
        int position = this.position;
        if (1 + MAX_VINT_BYTES + charCount * MAX_CHAR_BYTES <= endPosition - position) {
            buffer[position++] = (byte) 0;
            position = putVInt(buffer, charCount, position);
            for (int i = 0; i < charCount; i++) {
                position = putCharUtf8(buffer, str.charAt(i), position);
            }
            this.position = position;
        } else {
            writeByte((byte) 0);
            writeStringWithBoundsChecks(charCount, str);
        }
    }

    // slower (cold) path extracted to its own method to allow fast & hot path to be inlined
    private void writeStringWithBoundsChecks(int charCount, String str) throws IOException {
        writeVInt(charCount);
        int i = 0;
        int position = this.position;
        int lastSafePosition = this.endPosition - MAX_CHAR_BYTES;
        while (i < charCount) {
            while (i < charCount && position <= lastSafePosition) {
                position = putCharUtf8(buffer, str.charAt(i++), position);
            }
            this.position = position;
            while (capacity() < MAX_CHAR_BYTES && i < charCount) {
                writeCharUtf8(str.charAt(i++));
            }
            position = this.position;
        }
    }

    private static int putCharUtf8(byte[] buffer, int c, int position) {
        if (c <= 0x7F) {
            buffer[position++] = ((byte) c);
        } else if (c > 0x07FF) {
            buffer[position++] = ((byte) (0xE0 | c >> 12 & 0x0F));
            buffer[position++] = ((byte) (0x80 | c >> 6 & 0x3F));
            buffer[position++] = ((byte) (0x80 | c >> 0 & 0x3F));
        } else {
            buffer[position++] = ((byte) (0xC0 | c >> 6 & 0x1F));
            buffer[position++] = ((byte) (0x80 | c >> 0 & 0x3F));
        }
        return position;
    }

    private void writeCharUtf8(int c) throws IOException {
        if (c <= 0x7F) {
            writeByte((byte) c);
        } else if (c > 0x07FF) {
            writeByte((byte) (0xE0 | c >> 12 & 0x0F));
            writeByte((byte) (0x80 | c >> 6 & 0x3F));
            writeByte((byte) (0x80 | c >> 0 & 0x3F));
        } else {
            writeByte((byte) (0xC0 | c >> 6 & 0x1F));
            writeByte((byte) (0x80 | c >> 0 & 0x3F));
        }
    }
}
