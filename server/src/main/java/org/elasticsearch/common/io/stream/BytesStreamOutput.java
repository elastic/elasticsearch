/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * A @link {@link StreamOutput} that uses {@link BigArrays} to acquire pages of
 * bytes, which avoids frequent reallocation &amp; copying of the internal data.
 */
public class BytesStreamOutput extends BytesStream {

    protected final BigArrays bigArrays;

    @Nullable
    protected ByteArray bytes;
    protected int count;

    /**
     * Create a non recycling {@link BytesStreamOutput} with an initial capacity of 0.
     */
    public BytesStreamOutput() {
        // since this impl is not recycling anyway, don't bother aligning to
        // the page size, this will even save memory
        this(0);
    }

    /**
     * Create a non recycling {@link BytesStreamOutput} with enough initial pages acquired
     * to satisfy the capacity given by expected size.
     *
     * @param expectedSize the expected maximum size of the stream in bytes.
     */
    public BytesStreamOutput(int expectedSize) {
        this(expectedSize, BigArrays.NON_RECYCLING_INSTANCE);
    }

    protected BytesStreamOutput(int expectedSize, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        if (expectedSize != 0) {
            this.bytes = bigArrays.newByteArray(expectedSize, false);
        }
    }

    @Override
    public long position() {
        return count;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(count + 1L);
        bytes.set(count, b);
        count++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        Objects.checkFromIndexSize(offset, length, b.length);

        // get enough pages for new size
        ensureCapacity(((long) count) + length);

        // bulk copy
        bytes.set(count, b, offset, length);

        // advance
        count += length;
    }

    public void reset() {
        // shrink list of pages
        if (bytes != null && bytes.size() > PageCacheRecycler.PAGE_SIZE_IN_BYTES) {
            bytes = bigArrays.resize(bytes, PageCacheRecycler.PAGE_SIZE_IN_BYTES);
        }

        // go back to start
        count = 0;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        ensureCapacity(position);
        count = (int) position;
    }

    public void skip(int length) {
        seek(((long) count) + length);
    }

    @Override
    public void close() {
        // empty for now.
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid
     *         bytes in this output stream.
     * @see ByteArrayOutputStream#size()
     */
    public int size() {
        return count;
    }

    @Override
    public BytesReference bytes() {
        if (bytes == null) {
            return BytesArray.EMPTY;
        }
        return BytesReference.fromByteArray(bytes, count);
    }

    /**
     * Like {@link #bytes()} but copies the bytes to a freshly allocated buffer.
     *
     * @return copy of the bytes in this instances
     */
    public BytesReference copyBytes() {
        final BytesReference bytesReference = bytes();
        final byte[] arr = new byte[count];
        if (bytesReference.hasArray()) {
            System.arraycopy(bytesReference.array(), bytesReference.arrayOffset(), arr, 0, bytesReference.length());
        } else {
            copyToArray(bytesReference, arr);
        }
        return new BytesArray(arr);
    }

    private static void copyToArray(BytesReference bytesReference, byte[] arr) {
        int offset = 0;
        final BytesRefIterator iterator = bytesReference.iterator();
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                System.arraycopy(slice.bytes, slice.offset, arr, offset, slice.length);
                offset += slice.length;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    protected void ensureCapacity(long offset) {
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
        }
        if (bytes == null) {
            this.bytes = bigArrays.newByteArray(BigArrays.overSize(offset, PageCacheRecycler.PAGE_SIZE_IN_BYTES, 1), false);
        } else {
            bytes = bigArrays.grow(bytes, offset);
        }
    }

}
