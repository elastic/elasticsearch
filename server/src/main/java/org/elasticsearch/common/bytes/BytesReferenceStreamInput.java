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
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A StreamInput that reads off a {@link BytesRefIterator}. This is used to provide
 * generic stream access to {@link BytesReference} instances without materializing the
 * underlying bytes.
 */
class BytesReferenceStreamInput extends StreamInput {

    private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

    protected final BytesReference bytesReference;
    private BytesRefIterator iterator;
    private ByteBuffer slice;
    private int totalOffset; // the offset on the stream at which the current slice starts
    private int mark = 0;

    BytesReferenceStreamInput(BytesReference bytesReference) throws IOException {
        this.bytesReference = bytesReference;
        this.iterator = bytesReference.iterator();
        this.slice = convertToByteBuffer(iterator.next());
        this.totalOffset = 0;
    }

    static ByteBuffer convertToByteBuffer(BytesRef bytesRef) {
        if (bytesRef == null) {
            return EMPTY;
        }
        // slice here forces the buffer to have a sliced view, keeping track of the original offset
        return ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).slice();
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
    public String readString() throws IOException {
        final int chars = readArraySize();
        if (slice.hasArray()) {
            // attempt reading bytes directly into a string to minimize copying
            final byte[] bytes = slice.array();
            final int start = slice.position() + slice.arrayOffset();
            final int limit = slice.limit() + slice.arrayOffset();

            if (limit - start < chars) {
                return doReadString(chars); // not enough bytes to read at once
            }

            // calculate number of bytes matching the requested chars
            int pos = start;
            int remaining = chars;
            while (remaining-- > 0 && pos < limit) {
                int c = bytes[pos] & 0xff;
                switch (c >> 4) {
                    case 0, 1, 2, 3, 4, 5, 6, 7 -> pos++;
                    case 12, 13 -> pos += 2;
                    case 14 -> {
                        if (maybeHighSurrogate(bytes, pos, limit)) {
                            return doReadString(chars); // surrogate pairs are incorrectly encoded :/
                        }
                        pos += 3;
                    }
                    default -> throwOnBrokenChar(c);
                }
            }

            if (remaining > 0 || pos >= limit) {
                return doReadString(chars); // not enough bytes to read at once
            }

            slice.position(pos - slice.arrayOffset());
            return new String(bytes, start, pos - start, StandardCharsets.UTF_8);
        } else {
            return super.doReadString(chars);
        }
    }

    private static boolean maybeHighSurrogate(byte[] bytes, int pos, int limit) {
        if (pos + 2 >= limit) {
            return true; // beyond limit, we can't tell
        }
        int c1 = bytes[pos] & 0xff;
        int c2 = bytes[pos + 1] & 0xff;
        int c3 = bytes[pos + 2] & 0xff;
        int surrogateCandidate = ((c1 & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
        // check if in the high surrogate range
        return surrogateCandidate >= 0xD800 && surrogateCandidate <= 0xDBFF;
    }

    @Override
    public int readVInt() throws IOException {
        if (slice.remaining() >= 5) {
            return ByteBufferStreamInput.readVInt(slice);
        }
        return super.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        if (slice.remaining() >= 10) {
            return ByteBufferStreamInput.readVLong(slice);
        } else {
            return super.readVLong();
        }
    }

    protected int offset() {
        return totalOffset + slice.position();
    }

    private void maybeNextSlice() throws IOException {
        if (slice.hasRemaining() == false) {
            // moveToNextSlice is intentionally extracted to another method since it's the assumed cold-path
            moveToNextSlice();
        }
    }

    private void moveToNextSlice() throws IOException {
        totalOffset += slice.limit();
        BytesRef bytesRef = iterator.next();
        while (bytesRef != null && bytesRef.length == 0) {
            // rare corner case of a bytes reference that has a 0-length component
            bytesRef = iterator.next();
        }
        if (bytesRef == null) {
            throw new EOFException();
        }
        slice = convertToByteBuffer(bytesRef);
        assert slice.position() == 0;
    }

    @Override
    public void readBytes(byte[] b, int bOffset, int len) throws IOException {
        Objects.checkFromIndexSize(offset(), len, bytesReference.length());
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
            throwEOF(bytesToRead, bytesAvailable);
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
            slice.position(mark - totalOffset);
        } else {
            iterator = bytesReference.iterator();
            slice = convertToByteBuffer(iterator.next());
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
