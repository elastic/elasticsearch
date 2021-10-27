/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;

/**
 * A @link {@link StreamOutput} that uses {@link BigArrays} to acquire pages of
 * bytes, which avoids frequent reallocation &amp; copying of the internal data.
 */
public class NetworkStreamOutput extends BytesStream {

    static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    static final VarHandle VH_BE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final ArrayList<Recycler.V<BytesRef>> pages = new ArrayList<>();
    private final Recycler<BytesRef> recycler;
    private int currentCapacity = 0;
    private int currentPageOffset = 0;
    private int count = 0;

    protected NetworkStreamOutput(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    @Override
    public long position() {
        return count;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(count + 1L);
        BytesRef currentPage = pages.get(pages.size() - 1).v();
        currentPage.bytes[currentPage.offset + currentPageOffset] = b;
        count++;
        currentPageOffset++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        // illegal args: offset and/or length exceed array size
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }

        int currentPageIndex = pages.size();

        // get enough pages for new size
        ensureCapacity(((long) count) + length);

        // bulk copy
        int bytesToCopy = length;
        while (bytesToCopy > 0) {
            BytesRef currentPage = pages.get(currentPageIndex).v();

        }


        // advance
        count += length;
    }

    @Override
    public void writeInt(int i) throws IOException {
        if (4 > (currentCapacity - count)) {
            super.writeInt(i);
        } else {
            BytesRef currentPage = pages.get(pages.size() - 1).v();
            VH_BE_INT.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        if (8 > (currentCapacity - count)) {
            super.writeLong(i);
        } else {
            BytesRef currentPage = pages.get(pages.size() - 1).v();
            VH_BE_LONG.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
        }
    }

    @Override
    public void reset() {
        for (Recycler.V<BytesRef> page : pages) {
            page.close();
        }
        pages.clear();
        count = 0;
        currentPageOffset = 0;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
//        ensureCapacity(position);
//        count = (int) position;
    }

    public void skip(int length) {
//        seek(((long) count) + length);
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
        if (count == 0) {
            return BytesArray.EMPTY;
        }
        return null;
//        return BytesReference.fromByteArray(bytes, count);
    }

    protected void ensureCapacity(long offset) {
        if (offset > currentCapacity) {
            if (offset > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
            }
            Recycler.V<BytesRef> newPage = recycler.obtain();
            pages.add(newPage);
            currentCapacity += newPage.v().length;
        }
    }
}
