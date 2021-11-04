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
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.BytesRefRecycler;

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
public class BytesStreamOutput extends BytesStream {

    private static final BytesRefRecycler NON_RECYCLING_INSTANCE = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);
    private static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle VH_BE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final int MIN_SIZE = 64;

    protected final ArrayList<Recycler.V<BytesRef>> pages = new ArrayList<>(4);
    private final Recycler<BytesRef> recycler;
    private final int recyclerPageSize;
    private int pageSize;
    private int pageIndex = -1;
    private int currentCapacity = 0;
    private int currentPageOffset;

    public BytesStreamOutput() {
        this(MIN_SIZE);
    }

    public BytesStreamOutput(int expectedSize) {
        this(expectedSize, NON_RECYCLING_INSTANCE);
    }

    protected BytesStreamOutput(int expectedSize, Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        try (Recycler.V<BytesRef> obtain = recycler.obtain()) {
            recyclerPageSize = obtain.v().length;
            pageSize = Math.min(nextPowerOfTwo(Math.max(expectedSize, MIN_SIZE)), recyclerPageSize);
        }
        this.currentPageOffset = pageSize;
        ensureCapacityFromPosition(expectedSize);
    }

    @Override
    public long position() {
        return ((long) pageSize * pageIndex) + currentPageOffset;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(1);
        BytesRef currentPage = pages.get(pageIndex).v();
        currentPage.bytes[currentPage.offset + currentPageOffset] = b;
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

        // get enough pages for new size
        ensureCapacity(length);

        // bulk copy
        int bytesToCopy = length;
        int srcOff = offset;
        int j = 0;
        while (true) {
            BytesRef currentPage = pages.get(pageIndex + j).v();
            int toCopyThisLoop = Math.min(pageSize - currentPageOffset, bytesToCopy);
            System.arraycopy(b, srcOff, currentPage.bytes, currentPage.offset + currentPageOffset, toCopyThisLoop);
            srcOff += toCopyThisLoop;
            bytesToCopy -= toCopyThisLoop;
            if (bytesToCopy > 0) {
                currentPageOffset = 0;
            } else {
                currentPageOffset += toCopyThisLoop;
                break;
            }
            j++;
        }

        // advance
        pageIndex += j;
    }

    @Override
    public void writeInt(int i) throws IOException {
        if (4 > (pageSize - currentPageOffset)) {
            super.writeInt(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_BE_INT.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 4;
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLong(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_BE_LONG.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 8;
        }
    }

    @Override
    public void reset() {
        Releasables.close(pages);
        pages.clear();
        pageIndex = -1;
        currentPageOffset = pageSize;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        ensureCapacityFromPosition(position);
        this.pageIndex = (int) position / pageSize;
        this.currentPageOffset = (int) position % pageSize;
    }

    public void skip(int length) {
        seek(position() + length);
    }

    @Override
    public void close() {}

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid
     *         bytes in this output stream.
     * @see ByteArrayOutputStream#size()
     */
    public int size() {
        return Math.toIntExact(position());
    }

    @Override
    public BytesReference bytes() {
        int position = (int) position();
        if (position == 0) {
            return BytesArray.EMPTY;
        } else {
            final int adjustment;
            final int bytesInLastPage;
            final int remainder = position % pageSize;
            if (remainder != 0) {
                adjustment = 1;
                bytesInLastPage = remainder;
            } else {
                adjustment = 0;
                bytesInLastPage = pageSize;
            }
            final int pageCount = (position / pageSize) + adjustment;
            if (pageCount == 1) {
                BytesRef page = pages.get(0).v();
                return new BytesArray(page.bytes, page.offset, bytesInLastPage);
            } else {
                BytesReference[] references = new BytesReference[pageCount];
                for (int i = 0; i < pageCount - 1; ++i) {
                    references[i] = new BytesArray(this.pages.get(i).v());
                }
                BytesRef last = this.pages.get(pageCount - 1).v();
                references[pageCount - 1] = new BytesArray(last.bytes, last.offset, bytesInLastPage);
                return CompositeBytesReference.of(references);
            }
        }
    }

    private static int nextPowerOfTwo(int i) {
        int highestOneBit = Integer.highestOneBit(i);
        if (i == highestOneBit) {
            return i;
        } else {
            return highestOneBit << 1;
        }
    }

    private void ensureCapacity(int bytesNeeded) {
        if (bytesNeeded > pageSize - currentPageOffset) {
            ensureCapacityFromPosition(position() + bytesNeeded);
        }
    }

    protected void ensureCapacityFromPosition(long newPosition) {
        while (newPosition > currentCapacity) {
            if (newPosition > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
            }
            if (pageSize != recyclerPageSize) {
                assert pages.size() <= 1;
                assert pageIndex <= 0;
                pageSize = Math.min(nextPowerOfTwo((int) newPosition), recyclerPageSize);
                Recycler.V<BytesRef> maybeOldPage = pageIndex == -1 ? null : pages.get(0);
                pages.clear();
                handleSinglePageResize(maybeOldPage);
                currentCapacity = pageSize;
            } else {
                Recycler.V<BytesRef> newPage = recycler.obtain();
                assert pageSize == newPage.v().length;
                pages.add(newPage);
                // We are at the end of the current page, increment page index
                if (currentPageOffset == pageSize) {
                    pageIndex++;
                    currentPageOffset = 0;
                }
                currentCapacity += pageSize;
            }
        }
    }

    private void handleSinglePageResize(Recycler.V<BytesRef> maybeOldPage) {
        final BytesRef newBytePage;
        if (pageSize == recyclerPageSize) {
            Recycler.V<BytesRef> newPage = recycler.obtain();
            assert pageSize == newPage.v().length;
            pages.add(newPage);
            newBytePage = newPage.v();
        } else {
            newBytePage = new BytesRef(pageSize);
            pages.add(new NonRecycled(newBytePage));
        }
        if (maybeOldPage != null) {
            BytesRef oldBytePage = maybeOldPage.v();
            System.arraycopy(oldBytePage.bytes, oldBytePage.offset, newBytePage.bytes, newBytePage.offset, currentPageOffset);
        } else {
            pageIndex++;
            currentPageOffset = 0;
        }
    }

    private static class NonRecycled implements Recycler.V<BytesRef> {

        private final BytesRef bytePage;

        private NonRecycled(BytesRef bytePage) {
            this.bytePage = bytePage;
        }

        @Override
        public void close() {}

        @Override
        public BytesRef v() {
            return bytePage;
        }

        @Override
        public boolean isRecycled() {
            return false;
        }
    }

    /**
     * Like {@link #bytes()} but copies the bytes to a freshly allocated buffer.
     *
     * @return copy of the bytes in this instances
     */
    public BytesReference copyBytes() {
        final byte[] keyBytes = new byte[size()];
        int offset = 0;
        final BytesRefIterator iterator = bytes().iterator();
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                System.arraycopy(slice.bytes, slice.offset, keyBytes, offset, slice.length);
                offset += slice.length;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return new BytesArray(keyBytes);
    }
}
