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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.elasticsearch.common.util.PageCacheRecycler.INT_PAGE_SIZE;

/**
 * Int array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigIntArray extends AbstractBigArray implements IntArray {

    private static final BigIntArray ESTIMATOR = new BigIntArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    static final byte[] ZERO_VALUE_FILL = new byte[BYTE_PAGE_SIZE];
    static final byte[] MIN_VALUE_FILL = new byte[BYTE_PAGE_SIZE];
    static final byte[] MAX_VALUE_FILL = new byte[BYTE_PAGE_SIZE];

    static {
        for (int i = 0; i < INT_PAGE_SIZE; i++) {
            VH_PLATFORM_NATIVE_INT.set(ZERO_VALUE_FILL, i << 2, 0);
            VH_PLATFORM_NATIVE_INT.set(MIN_VALUE_FILL, i << 2, Integer.MIN_VALUE);
            VH_PLATFORM_NATIVE_INT.set(MAX_VALUE_FILL, i << 2, Integer.MAX_VALUE);
        }
    }

    private byte[][] pages;

    /** Constructor. */
    BigIntArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(INT_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public int get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (int) VH_PLATFORM_NATIVE_INT.get(pages[pageIndex], indexInPage << 2);
    }

    @Override
    public int set(long index, int value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final int ret = (int) VH_PLATFORM_NATIVE_INT.get(page, indexInPage << 2);
        VH_PLATFORM_NATIVE_INT.set(page, indexInPage << 2, value);
        return ret;
    }

    @Override
    public int increment(long index, int inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final int newVal = (int) VH_PLATFORM_NATIVE_INT.get(page, indexInPage << 2) + inc;
        VH_PLATFORM_NATIVE_INT.set(page, indexInPage << 2, newVal);
        return newVal;
    }

    @Override
    public void fill(long fromIndex, long toIndex, int value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            fill(pages[fromPage], indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, value);
        } else {
            fill(pages[fromPage], indexInPage(fromIndex), pageSize(), value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                fill(pages[i], 0, pageSize(), value);
            }
            fill(pages[toPage], 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    public static void fill(byte[] page, int from, int to, int value) {
        if (value == 0) {
            System.arraycopy(ZERO_VALUE_FILL, 0, page, from << 2, (to - from) << 2);
        } else if (value == Integer.MIN_VALUE) {
            System.arraycopy(MIN_VALUE_FILL, 0, page, from << 2, (to - from) << 2);
        } else if (value == Integer.MAX_VALUE) {
            System.arraycopy(MAX_VALUE_FILL, 0, page, from << 2, (to - from) << 2);
        } else {
            for (int i = from; i < to; i++) {
                VH_PLATFORM_NATIVE_INT.set(page, i << 2, value);
            }
        }
    }

    @Override
    protected int numBytesPerElement() {
        return Integer.BYTES;
    }

    /** Change the size of this array. Content between indexes <code>0</code> and <code>min(size(), newSize)</code> will be preserved. */
    @Override
    public void resize(long newSize) {
        final int numPages = numPages(newSize);
        if (numPages > pages.length) {
            pages = Arrays.copyOf(pages, ArrayUtil.oversize(numPages, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        for (int i = numPages - 1; i >= 0 && pages[i] == null; --i) {
            pages[i] = newBytePage(i);
        }
        for (int i = numPages; i < pages.length && pages[i] != null; ++i) {
            pages[i] = null;
            releasePage(i);
        }
        this.size = newSize;
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            System.arraycopy(buf, offset << 2, pages[pageIndex], indexInPage << 2, len << 2);
        } else {
            int copyLen = pageSize() - indexInPage;
            System.arraycopy(buf, offset << 2, pages[pageIndex], indexInPage, copyLen << 2);
            do {
                ++pageIndex;
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, pageSize());
                System.arraycopy(buf, offset << 2, pages[pageIndex], 0, copyLen << 2);
            } while (len > copyLen);
        }
    }
}
