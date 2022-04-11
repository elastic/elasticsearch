/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasables;

import java.lang.reflect.Array;
import java.util.Arrays;

/** Common implementation for array lists that slice data into fixed-size blocks. */
abstract class AbstractBigArray extends AbstractArray {

    private final PageCacheRecycler recycler;
    private Recycler.V<?>[] cache;

    private final int pageShift;
    private final int pageMask;
    protected long size;

    protected AbstractBigArray(int pageSize, BigArrays bigArrays, boolean clearOnResize) {
        super(bigArrays, clearOnResize);
        this.recycler = bigArrays.recycler;
        if (pageSize < 128) {
            throw new IllegalArgumentException("pageSize must be >= 128");
        }
        if ((pageSize & (pageSize - 1)) != 0) {
            throw new IllegalArgumentException("pageSize must be a power of two");
        }
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = pageSize - 1;
        size = 0;
        if (this.recycler != null) {
            cache = new Recycler.V<?>[16];
        } else {
            cache = null;
        }
    }

    final int numPages(long capacity) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity);
        }
        return (int) numPages;
    }

    final int pageSize() {
        return pageMask + 1;
    }

    final int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    final int indexInPage(long index) {
        return (int) (index & pageMask);
    }

    @Override
    public final long size() {
        return size;
    }

    public abstract void resize(long newSize);

    protected abstract int numBytesPerElement();

    @Override
    public final long ramBytesUsed() {
        return ramBytesEstimated(size);
    }

    /** Given the size of the array, estimate the number of bytes it will use. */
    public final long ramBytesEstimated(final long size) {
        // rough approximate, we only take into account the size of the values, not the overhead of the array objects
        return ((long) pageIndex(size - 1) + 1) * pageSize() * numBytesPerElement();
    }

    private static <T> T[] grow(T[] array, int minSize) {
        if (array.length < minSize) {
            final int newLen = ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            array = Arrays.copyOf(array, newLen);
        }
        return array;
    }

    private <T> T registerNewPage(Recycler.V<T> v, int page, int expectedSize) {
        cache = grow(cache, page + 1);
        assert cache[page] == null;
        cache[page] = v;
        assert Array.getLength(v.v()) == expectedSize;
        return v.v();
    }

    protected final byte[] newBytePage(int page) {
        if (recycler != null) {
            final Recycler.V<byte[]> v = recycler.bytePage(clearOnResize);
            return registerNewPage(v, page, PageCacheRecycler.BYTE_PAGE_SIZE);
        } else {
            return new byte[PageCacheRecycler.BYTE_PAGE_SIZE];
        }
    }

    protected final Object[] newObjectPage(int page) {
        if (recycler != null) {
            final Recycler.V<Object[]> v = recycler.objectPage();
            return registerNewPage(v, page, PageCacheRecycler.OBJECT_PAGE_SIZE);
        } else {
            return new Object[PageCacheRecycler.OBJECT_PAGE_SIZE];
        }
    }

    protected final void releasePage(int page) {
        if (recycler != null) {
            cache[page].close();
            cache[page] = null;
        }
    }

    @Override
    protected final void doClose() {
        if (recycler != null) {
            Releasables.close(cache);
            cache = null;
        }
    }

    /**
     * Fills an array with a value by copying it to itself, increasing copy ranges in each iteration
     */
    protected static final void fillBySelfCopy(byte[] page, int fromBytes, int toBytes, int initialCopyBytes) {
        for (int pos = fromBytes + initialCopyBytes; pos < toBytes;) {
            int sourceBytesLength = pos - fromBytes; // source bytes available to be copied
            int copyBytesLength = Math.min(sourceBytesLength, toBytes - pos); // number of bytes to actually copy
            System.arraycopy(page, fromBytes, page, pos, copyBytesLength);
            pos += copyBytesLength;
        }
    }

    /**
     * Bulk copies array to paged array
     */
    public void set(long index, byte[] buf, int offset, int len, byte[][] pages, int shift) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            System.arraycopy(buf, offset << shift, pages[pageIndex], indexInPage << shift, len << shift);
        } else {
            int copyLen = pageSize() - indexInPage;
            System.arraycopy(buf, offset << shift, pages[pageIndex], indexInPage, copyLen << shift);
            do {
                ++pageIndex;
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, pageSize());
                System.arraycopy(buf, offset << shift, pages[pageIndex], 0, copyLen << shift);
            } while (len > copyLen);
        }
    }

}
