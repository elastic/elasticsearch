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
 * A @link {@link StreamOutput} that accumulates the resulting data in memory, using {@link BigArrays} to avoids frequent reallocation &amp;
 * copying of the internal data once the resulting data grows large enough whilst avoiding excessive overhead in the final result for small
 * objects.
 * <p>
 * A {@link BytesStreamOutput} accumulates data using a non-recycling {@link BigArrays} and, as with an {@link OutputStreamStreamOutput}, it
 * uses a thread-locally-cached buffer for some of its writes and pushes data to the underlying array in small chunks, causing frequent
 * calls to {@link BigArrays#resize}. If the array is large enough (≥16kiB) then the resize operations happen in-place, allocating a new
 * 16kiB {@code byte[]} and appending it to the array, but for smaller arrays these resize operations allocate a completely fresh
 * {@code byte[]} into which they copy the entire contents of the old one.
 * <p>
 * {@link BigArrays#resize} grows smaller arrays more slowly than a {@link ByteArrayOutputStream}, with a target of 12.5% overhead rather
 * than 100%, which means that a sequence of smaller writes causes more allocations and copying overall. It may be worth adding a
 * {@link BufferedStreamOutput} wrapper to reduce the frequency of the resize operations, especially if a suitable buffer is already
 * allocated and available.
 * <p>
 * The resulting {@link BytesReference} is a view over the underlying {@code byte[]} pages and involves no significant extra allocation to
 * obtain. It is oversized: The worst case for overhead is when the data is one byte more than a 16kiB page and therefore the result must
 * retain two pages even though all but one byte of the second page is unused. For smaller objects the overhead will be 12.5%.
 * <p>
 * Any memory allocated in this way is untracked by the {@link org.elasticsearch.common.breaker} subsystem unless the caller takes steps to
 * add this tracking themselves.
 *
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

    @Override
    public void writeString(String str) throws IOException {
        StreamOutputHelper.writeString(str, this);
    }

    @Override
    public void writeOptionalString(@Nullable String str) throws IOException {
        StreamOutputHelper.writeOptionalString(str, this);
    }

    @Override
    public void writeGenericString(String value) throws IOException {
        StreamOutputHelper.writeGenericString(value, this);
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
