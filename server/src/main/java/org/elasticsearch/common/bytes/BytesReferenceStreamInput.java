/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A StreamInput that reads off a {@link BytesRefIterator}. This is used to provide
 * generic stream access to {@link BytesReference} instances without materializing the
 * underlying bytes.
 */
class BytesReferenceStreamInput extends StreamInput {

    protected final BytesReference bytesReference;
    private BytesRefIterator iterator;
    private ByteBuffer slice;
    private int sliceOffset;
    private int totalOffset; // the offset on the stream at which the current slice starts
    private int mark = 0;

    BytesReferenceStreamInput(BytesReference bytesReference) throws IOException {
        this.bytesReference = bytesReference;
        this.iterator = bytesReference.iterator();
        BytesRef bytesRef = iterator.next();
        this.slice = convertToByteBuffer(bytesRef);
        this.sliceOffset = bytesRef.offset;
        this.totalOffset = 0;
    }

    static ByteBuffer convertToByteBuffer(BytesRef bytesRef) {
        if (bytesRef == null) {
            return null;
        }
        return ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    @Override
    public byte readByte() throws IOException {
        maybeNextSlice();
        return slice.get();
    }

    @Override
    public short readShort() throws IOException {
        if (slice.remaining() >= 2) {
            return slice.getShort();
        } else {
            // slow path
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (slice.remaining() >= 4) {
            return slice.getInt();
        } else {
            // slow path
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (slice.remaining() >= 8) {
            return slice.getLong();
        } else {
            // slow path
            return super.readLong();
        }
    }

    @Override
    public int readVInt() throws IOException {
        if (slice.remaining() >= 5) {
            final byte[] buf = slice.array();
            int offset = slice.position();
            byte b = buf[offset++];
            if (b >= 0) {
                slice.position(offset);
                return b;
            }
            int i = b & 0x7F;
            b = buf[offset++];
            i |= (b & 0x7F) << 7;
            if (b >= 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7F) << 14;
            if (b >= 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7F) << 21;
            if (b >= 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x0F) << 28;
            if ((b & 0xF0) == 0) {
                slice.position(offset);
                return i;
            }
            throwOnBrokenVInt(b, i);
        }
        return super.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        if (slice.remaining() >= 10) {
            final byte[] buf = slice.array();
            int offset = slice.position();
            byte b = buf[offset++];
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                slice.position(offset);
                return i;
            }
            b = buf[offset++];
            if (b != 0 && b != 1) {
                throwOnBrokenVLong(b, i);
            }
            slice.position(offset);
            i |= ((long) b) << 63;
            return i;
        } else {
            return super.readVLong();
        }
    }

    protected int offset() {
        return totalOffset + slice.position() - sliceOffset;
    }

    private void maybeNextSlice() throws IOException {
        if (slice.hasRemaining() == false) {
            // moveToNextSlice is intentionally extracted to another method since it's the assumed cold-path
            moveToNextSlice();
        }
    }

    private void moveToNextSlice() throws IOException {
        totalOffset += slice.limit() - sliceOffset;
        BytesRef bytesRef = iterator.next();
        while (bytesRef != null && bytesRef.length == 0) {
            // rare corner case of a bytes reference that has a 0-length component
            bytesRef = iterator.next();
        }
        if (bytesRef == null) {
            throw new EOFException();
        }
        sliceOffset = bytesRef.offset;
        slice = convertToByteBuffer(bytesRef);
    }

    @Override
    public void readBytes(byte[] b, int bOffset, int len) throws IOException {
        final int length = bytesReference.length();
        final int offset = offset();
        if (offset + len > length) {
            throw new IndexOutOfBoundsException("Cannot read " + len + " bytes from stream with length " + length + " at offset " + offset);
        }
        final int bytesRead = read(b, bOffset, len);
        assert bytesRead == len : bytesRead + " vs " + len;
    }

    @Override
    public int read() throws IOException {
        if (offset() >= bytesReference.length()) {
            return -1;
        }
        return Byte.toUnsignedInt(readByte());
    }

    @Override
    public int read(final byte[] b, final int bOffset, final int len) throws IOException {
        final int length = bytesReference.length();
        final int offset = offset();
        if (offset >= length) {
            return -1;
        }
        final int numBytesToCopy = Math.min(len, length - offset);
        int remaining = numBytesToCopy; // copy the full length or the remaining part
        int destOffset = bOffset;
        while (remaining > 0) {
            maybeNextSlice();
            final int currentLen = Math.min(remaining, slice.remaining());
            assert currentLen > 0 : "length has to be > 0 to make progress but was: " + currentLen;
            slice.get(b, destOffset, currentLen);
            destOffset += currentLen;
            remaining -= currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesToCopy;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int available() {
        return bytesReference.length() - offset();
    }

    @Override
    protected void ensureCanReadBytes(int bytesToRead) throws EOFException {
        int bytesAvailable = bytesReference.length() - offset();
        if (bytesAvailable < bytesToRead) {
            throw new EOFException("tried to read: " + bytesToRead + " bytes but only " + bytesAvailable + " remaining");
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0L) {
            return 0L;
        }
        assert offset() <= bytesReference.length() : offset() + " vs " + bytesReference.length();
        // definitely >= 0 and <= Integer.MAX_VALUE so casting is ok
        final int numBytesSkipped = (int) Math.min(n, bytesReference.length() - offset());
        int remaining = numBytesSkipped;
        while (remaining > 0) {
            maybeNextSlice();
            int currentLen = Math.min(remaining, slice.remaining());
            remaining -= currentLen;
            slice.position(slice.position() + currentLen);
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesSkipped;
    }

    @Override
    public void reset() throws IOException {
        if (totalOffset <= mark) {
            slice.position(sliceOffset + (mark - totalOffset));
        } else {
            iterator = bytesReference.iterator();
            BytesRef bytesRef = iterator.next();
            sliceOffset = bytesRef.offset;
            slice = convertToByteBuffer(bytesRef);
            totalOffset = 0;
            final long skipped = skip(mark);
            assert skipped == mark : skipped + " vs " + mark;
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        // We ignore readLimit since the data is all in-memory and therefore we can reset the mark no matter how far we advance.
        this.mark = offset();
    }
}
