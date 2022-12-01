/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

final class BigLongDoubleDoubleArray extends AbstractBigArray implements LongDoubleDoubleArray {

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int ELEMENT_SHIFT = 5;
    public static final int ELEMENT_SIZE_IN_BYTES = 1 << 5;   // 32 (8 + 8 + 8 = 24bytes + 8 pad = 32 byte)
    public static final int ELEMENTS_PER_PAGE = PAGE_SIZE_IN_BYTES / ELEMENT_SIZE_IN_BYTES;

    private static final BigLongDoubleDoubleArray ESTIMATOR = new BigLongDoubleDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    static final VarHandle VH_PLATFORM_NATIVE_DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    private byte[][] pages;

    // 512 elements per page (accounting for the extra 8 bytes padding)

    /** Constructor. */
    BigLongDoubleDoubleArray(long numberOfElements, BigArrays bigArrays, boolean clearOnResize) {
        super(ELEMENTS_PER_PAGE, bigArrays, clearOnResize);
        this.size = numberOfElements;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public long getLong0(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (long) VH_PLATFORM_NATIVE_LONG.get(pages[pageIndex], indexInPage << ELEMENT_SHIFT);
    }

    @Override
    public double getDouble0(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        // + prev all elements, which is one long
        var d = (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], (indexInPage << ELEMENT_SHIFT) + 8);
        return d;
    }

    @Override
    public double getDouble1(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        // + prev all elements, which is one long and one double
        return (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], (indexInPage << ELEMENT_SHIFT) + 16);
    }

    @Override
    public void set(long index, long lValue0, double dValue0, double dValue1) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final int offset = indexInPage << ELEMENT_SHIFT;
        VH_PLATFORM_NATIVE_LONG.set(page, offset, lValue0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 8, dValue0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 16, dValue1);
        // ignore padding - should be 0
    }

    @Override
    public void increment(long index, long lValue0Inc, double dValue0Inc, double dValue1Inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final int offset = indexInPage << ELEMENT_SHIFT;
        var newLong0 = (long) VH_PLATFORM_NATIVE_LONG.get(pages[pageIndex], offset) + +lValue0Inc;
        var newDouble0 = (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], offset + 8) + dValue0Inc;
        var newDouble1 = (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], offset + 16) + dValue1Inc;
        VH_PLATFORM_NATIVE_LONG.set(page, offset, newLong0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 8, newDouble0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 16, newDouble1);
    }

    /** Changes the size of this array. Content between indexes <code>0</code> and <code>min(size(), newSize)</code> will be preserved. */
    @Override
    public void resize(long newSize) {
        final int numPages = numPages(newSize);
        if (numPages > pages.length) {
            pages = Arrays.copyOf(pages, ArrayUtil.oversize(numPages, ELEMENT_SIZE_IN_BYTES));
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

    @Override
    protected int numBytesPerElement() {
        return ELEMENT_SIZE_IN_BYTES;
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }
}
