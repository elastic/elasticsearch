/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * A StreamInput that reads off a {@link BytesRefIterator}. This is used to provide
 * generic stream access to {@link BytesReference} instances without materializing the
 * underlying bytes reference.
 */
final class BytesReferenceStreamInput extends StreamInput {
    private final BytesRefIterator iterator;
    private int sliceIndex;
    private BytesRef slice;
    private final int length; // the total size of the stream
    private int offset; // the current position of the stream

    BytesReferenceStreamInput(BytesRefIterator iterator, final int length) throws IOException {
        this.iterator = iterator;
        this.slice = iterator.next();
        this.length = length;
        this.offset = 0;
        this.sliceIndex = 0;
    }

    @Override
    public byte readByte() throws IOException {
        if (offset >= length) {
            throw new EOFException();
        }
        maybeNextSlice();
        byte b = slice.bytes[slice.offset + (sliceIndex++)];
        offset++;
        return b;
    }

    private void maybeNextSlice() throws IOException {
        while (sliceIndex == slice.length) {
            slice = iterator.next();
            sliceIndex = 0;
            if (slice == null) {
                throw new EOFException();
            }
        }
    }

    @Override
    public void readBytes(byte[] b, int bOffset, int len) throws IOException {
        if (offset + len > length) {
            throw new IndexOutOfBoundsException("Cannot read " + len + " bytes from stream with length " + length + " at offset " + offset);
        }
        read(b, bOffset, len);
    }

    @Override
    public int read() throws IOException {
        if (offset >= length) {
            return -1;
        }
        return Byte.toUnsignedInt(readByte());
    }

    @Override
    public int read(final byte[] b, final int bOffset, final int len) throws IOException {
        if (offset >= length) {
            return -1;
        }
        final int numBytesToCopy =  Math.min(len, length - offset);
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
            offset += currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesToCopy;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public int available() throws IOException {
        return length - offset;
    }

    @Override
    protected void ensureCanReadBytes(int bytesToRead) throws EOFException {
        int bytesAvailable = length - offset;
        if (bytesAvailable < bytesToRead) {
            throw new EOFException("tried to read: " + bytesToRead + " bytes but only " + bytesAvailable + " remaining");
        }
    }

    @Override
    public long skip(long n) throws IOException {
        final int skip = (int) Math.min(Integer.MAX_VALUE, n);
        final int numBytesSkipped =  Math.min(skip, length - offset);
        int remaining = numBytesSkipped;
        while (remaining > 0) {
            maybeNextSlice();
            int currentLen = Math.min(remaining, slice.length - sliceIndex);
            remaining -= currentLen;
            sliceIndex += currentLen;
            offset += currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesSkipped;
    }

    int getOffset() {
        return offset;
    }
}
