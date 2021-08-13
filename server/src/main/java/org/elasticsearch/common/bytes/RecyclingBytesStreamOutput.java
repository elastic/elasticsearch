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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * An in-memory {@link StreamOutput} which first fills the given {@code byte[]} and then allocates more space from the given
 * {@link BigArrays} if needed. The idea is that you can use this for passing data to an API that requires a single {@code byte[]} (or a
 * {@link org.apache.lucene.util.BytesRef}) which you'd prefer to re-use if possible, avoiding excessive allocations, but which may not
 * always be large enough.
 */
public class RecyclingBytesStreamOutput extends BytesStream {

    private final byte[] buffer;
    private final BigArrays bigArrays;

    private int position;

    @Nullable // if buffer is large enough
    private ByteArray overflow;

    public RecyclingBytesStreamOutput(byte[] buffer, BigArrays bigArrays) {
        this.buffer = Objects.requireNonNull(buffer);
        this.bigArrays = Objects.requireNonNull(bigArrays);
    }

    @Override
    public void writeByte(byte b) {
        if (position < buffer.length) {
            buffer[position++] = b;
        } else {
            ensureCapacity(position + 1);
            overflow.set(position++ - buffer.length, b);
        }
    }

    private void ensureCapacity(int size) {
        final int overflowSize = size - buffer.length;
        assert overflowSize > 0 : "no need to ensureCapacity(" + size + ") with buffer of size [" + buffer.length + "]";
        assert position >= buffer.length
                : "no need to ensureCapacity(" + size + ") with buffer of size [" + buffer.length + "] at position [" + position + "]";
        if (overflow == null) {
            overflow = bigArrays.newByteArray(overflowSize, false);
        } else if (overflowSize > overflow.size()) {
            overflow = bigArrays.resize(overflow, overflowSize);
        }
        assert overflow.size() >= overflowSize;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        if (position < buffer.length) {
            final int lengthForBuffer = Math.min(length, buffer.length - position);
            System.arraycopy(b, offset, buffer, position, lengthForBuffer);
            position += lengthForBuffer;
            offset += lengthForBuffer;
            length -= lengthForBuffer;
        }

        if (length > 0) {
            ensureCapacity(position + length);
            overflow.set(position - buffer.length, b, offset, length);
            position += length;
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(overflow);
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Return the written bytes in a {@link BytesRef}, avoiding allocating a new {@code byte[]} if the original buffer was already large
     * enough. If we allocate a new (larger) buffer here then callers should typically re-use it for subsequent streams.
     */
    public BytesRef toBytesRef() {
        if (position <= buffer.length) {
            assert overflow == null;
            return new BytesRef(buffer, 0, position);
        }

        final byte[] newBuffer = new byte[position];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        int copyPos = buffer.length;
        final BytesRefIterator iterator = BytesReference.fromByteArray(overflow, position - buffer.length).iterator();
        BytesRef bytesRef;
        try {
            while ((bytesRef = iterator.next()) != null) {
                assert copyPos + bytesRef.length <= position;
                System.arraycopy(bytesRef.bytes, bytesRef.offset, newBuffer, copyPos, bytesRef.length);
                copyPos += bytesRef.length;
            }
        } catch (IOException e) {
            throw new AssertionError("impossible", e);
        }

        return new BytesRef(newBuffer, 0, position);
    }

    @Override
    public BytesReference bytes() {
        if (position <= buffer.length) {
            assert overflow == null;
            return new BytesArray(buffer, 0, position);
        } else {
            return CompositeBytesReference.of(
                    new BytesArray(buffer, 0, buffer.length),
                    BytesReference.fromByteArray(overflow, position - buffer.length));
        }
    }
}
