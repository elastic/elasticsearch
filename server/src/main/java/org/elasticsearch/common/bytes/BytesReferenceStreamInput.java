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
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * A StreamInput that reads off a {@link BytesRefIterator}. This is used to provide
 * generic stream access to {@link BytesReference} instances without materializing the
 * underlying bytes.
 */
class BytesReferenceStreamInput extends StreamInput {

    protected final BytesReference bytesReference;
    private BytesRefIterator iterator;
    private int sliceIndex;
    private BytesRef slice;
    private int sliceStartOffset; // the offset on the stream at which the current slice starts

    private int mark = 0;

    BytesReferenceStreamInput(BytesReference bytesReference) throws IOException {
        this.bytesReference = bytesReference;
        this.iterator = bytesReference.iterator();
        this.slice = iterator.next();
        this.sliceStartOffset = 0;
        this.sliceIndex = 0;
    }

    @Override
    public byte readByte() throws IOException {
        maybeNextSlice();
        return slice.bytes[slice.offset + (sliceIndex++)];
    }

    @Override
    public short readShort() throws IOException {
        if (slice.length - sliceIndex >= 2) {
            sliceIndex += 2;
            return Numbers.bytesToShort(slice.bytes, slice.offset + sliceIndex - 2);
        } else {
            // slow path
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (slice.length - sliceIndex >= 4) {
            sliceIndex += 4;
            return Numbers.bytesToInt(slice.bytes, slice.offset + sliceIndex - 4);
        } else {
            // slow path
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (slice.length - sliceIndex >= 8) {
            sliceIndex += 8;
            return Numbers.bytesToLong(slice.bytes, slice.offset + sliceIndex - 8);
        } else {
            // slow path
            return super.readLong();
        }
    }

    @Override
    public int readVInt() throws IOException {
        if (slice.length - sliceIndex >= 5) {
            final byte[] buf = slice.bytes;
            final int offset = slice.offset;
            byte b = buf[offset + sliceIndex++];
            if (b >= 0) {
                return b;
            }
            int i = b & 0x7F;
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7F) << 7;
            if (b >= 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7F) << 14;
            if (b >= 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7F) << 21;
            if (b >= 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x0F) << 28;
            if ((b & 0xF0) == 0) {
                return i;
            }
            throwOnBrokenVInt(b, i);
        }
        return super.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        if (slice.length - sliceIndex >= 10) {
            final byte[] buf = slice.bytes;
            final int offset = slice.offset;
            byte b = buf[offset + sliceIndex++];
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[offset + sliceIndex++];
            if (b != 0 && b != 1) {
                throwOnBrokenVLong(b, i);
            }
            i |= ((long) b) << 63;
            return i;
        } else {
            return super.readVLong();
        }
    }

    protected int offset() {
        return sliceStartOffset + sliceIndex;
    }

    private void maybeNextSlice() throws IOException {
        if (sliceIndex == slice.length) {
            // moveToNextSlice is intentionally extracted to another method since it's the assumed cold-path
            moveToNextSlice();
        }
    }

    private void moveToNextSlice() throws IOException {
        slice = iterator.next();
        while (slice != null && slice.length == 0) {
            // rare corner case of a bytes reference that has a 0-length component
            slice = iterator.next();
        }
        if (slice == null) {
            throw new EOFException();
        }
        sliceStartOffset += sliceIndex;
        sliceIndex = 0;
    }

    @Override
    public void readBytes(byte[] b, int bOffset, int len) throws IOException {
        final int length = bytesReference.length();
        final int offset = offset();
        if (offset + len > length) {
            throw new IndexOutOfBoundsException(
                    "Cannot read " + len + " bytes from stream with length " + length + " at offset " + offset);
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
            final int currentLen = Math.min(remaining, slice.length - sliceIndex);
            assert currentLen > 0 : "length has to be > 0 to make progress but was: " + currentLen;
            System.arraycopy(slice.bytes, slice.offset + sliceIndex, b, destOffset, currentLen);
            destOffset += currentLen;
            remaining -= currentLen;
            sliceIndex += currentLen;
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
            int currentLen = Math.min(remaining, slice.length - sliceIndex);
            remaining -= currentLen;
            sliceIndex += currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesSkipped;
    }

    @Override
    public void reset() throws IOException {
        if (sliceStartOffset <= mark) {
            sliceIndex = mark - sliceStartOffset;
        } else {
            iterator = bytesReference.iterator();
            slice = iterator.next();
            sliceStartOffset = 0;
            sliceIndex = 0;
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
