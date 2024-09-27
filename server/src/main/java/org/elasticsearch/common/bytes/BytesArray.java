/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

public final class BytesArray extends AbstractBytesReference {

    public static final BytesArray EMPTY = new BytesArray(BytesRef.EMPTY_BYTES, 0, 0);
    private final byte[] bytes;
    private final int offset;

    public BytesArray(String bytes) {
        this(new BytesRef(bytes));
    }

    public BytesArray(BytesRef bytesRef) {
        this(bytesRef, false);
    }

    public BytesArray(BytesRef bytesRef, boolean deepCopy) {
        super(bytesRef.length);
        if (deepCopy) {
            bytesRef = BytesRef.deepCopyOf(bytesRef);
        }
        bytes = bytesRef.bytes;
        offset = bytesRef.offset;
    }

    public BytesArray(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public BytesArray(byte[] bytes, int offset, int length) {
        super(length);
        this.bytes = bytes;
        this.offset = offset;
    }

    @Override
    public byte get(int index) {
        return bytes[offset + index];
    }

    @Override
    public int indexOf(byte marker, int from) {
        final int len = length - from;
        // cache object fields (even when final this is a valid optimization, see https://openjdk.org/jeps/8132243)
        final int offsetAsLocal = offset;
        int off = offsetAsLocal + from;
        final int toIndex = offsetAsLocal + length;
        final byte[] bytesAsLocal = bytes;
        // First, try to find the marker in the first few bytes, so we can enter the faster 8-byte aligned loop below.
        // The idea for this logic is taken from Netty's io.netty.buffer.ByteBufUtil.firstIndexOf and optimized for little endian hardware.
        // See e.g. https://richardstartin.github.io/posts/finding-bytes for the idea behind this optimization.
        final int byteCount = len & 7;
        if (byteCount > 0) {
            final int index = unrolledFirstIndexOf(bytesAsLocal, off, byteCount, marker);
            if (index != -1) {
                return index - offsetAsLocal;
            }
            off += byteCount;
            if (off == toIndex) {
                return -1;
            }
        }
        final int longCount = len >>> 3;
        // faster SWAR (SIMD Within A Register) loop
        final long pattern = compilePattern(marker);
        for (int i = 0; i < longCount; i++) {
            int index = findInLong(ByteUtils.readLongLE(bytesAsLocal, off), pattern);
            if (index < Long.BYTES) {
                return off + index - offsetAsLocal;
            }
            off += Long.BYTES;
        }
        return -1;
    }

    private static long compilePattern(byte byteToFind) {
        return (byteToFind & 0xFFL) * 0x101010101010101L;
    }

    private static int findInLong(long word, long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
        final int binaryPosition = Long.numberOfTrailingZeros(tmp);
        return binaryPosition >>> 3;
    }

    private static int unrolledFirstIndexOf(byte[] buffer, int fromIndex, int byteCount, byte value) {
        if (buffer[fromIndex] == value) {
            return fromIndex;
        }
        if (byteCount == 1) {
            return -1;
        }
        if (buffer[fromIndex + 1] == value) {
            return fromIndex + 1;
        }
        if (byteCount == 2) {
            return -1;
        }
        if (buffer[fromIndex + 2] == value) {
            return fromIndex + 2;
        }
        if (byteCount == 3) {
            return -1;
        }
        if (buffer[fromIndex + 3] == value) {
            return fromIndex + 3;
        }
        if (byteCount == 4) {
            return -1;
        }
        if (buffer[fromIndex + 4] == value) {
            return fromIndex + 4;
        }
        if (byteCount == 5) {
            return -1;
        }
        if (buffer[fromIndex + 5] == value) {
            return fromIndex + 5;
        }
        if (byteCount == 6) {
            return -1;
        }
        if (buffer[fromIndex + 6] == value) {
            return fromIndex + 6;
        }
        return -1;
    }

    @Override
    public int hashCode() {
        // NOOP override to satisfy Checkstyle's EqualsHashCode
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof final BytesArray that) {
            return Arrays.equals(bytes, offset, offset + length, that.bytes, that.offset, that.offset + that.length);
        }
        return super.equals(other);
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from == 0 && this.length == length) {
            return this;
        }
        Objects.checkFromIndexSize(from, length, this.length);
        return new BytesArray(bytes, offset + from, length);
    }

    @Override
    public boolean hasArray() {
        return true;
    }

    @Override
    public byte[] array() {
        return bytes;
    }

    @Override
    public int arrayOffset() {
        return offset;
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(bytes, offset, length);
    }

    @Override
    public BytesRefIterator iterator() {
        if (length == 0) {
            return BytesRefIterator.EMPTY;
        }
        return new BytesRefIterator() {
            BytesRef ref = toBytesRef();

            @Override
            public BytesRef next() {
                BytesRef r = ref;
                ref = null; // only return it once...
                return r;
            }
        };
    }

    @Override
    public long ramBytesUsed() {
        return bytes.length;
    }

    @Override
    public StreamInput streamInput() {
        return StreamInput.wrap(bytes, offset, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(bytes, offset, length);
    }

    @Override
    public int getIntLE(int index) {
        return ByteUtils.readIntLE(bytes, offset + index);
    }

    @Override
    public long getLongLE(int index) {
        return ByteUtils.readLongLE(bytes, offset + index);
    }

    @Override
    public double getDoubleLE(int index) {
        return ByteUtils.readDoubleLE(bytes, offset + index);
    }
}
