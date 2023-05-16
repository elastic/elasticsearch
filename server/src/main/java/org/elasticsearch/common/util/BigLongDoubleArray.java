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

final class BigLongDoubleArray extends AbstractBigArray.PowerOfTwoAbstractBigArray implements LongDoubleArray {

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int ELEMENT_SHIFT = 4;
    public static final int ELEMENT_SIZE_IN_BYTES = 1 << 4;   // 16 (8 + 8 )
    public static final int ELEMENTS_PER_PAGE = PAGE_SIZE_IN_BYTES / ELEMENT_SIZE_IN_BYTES;

    private static final BigLongDoubleArray ESTIMATOR = new BigLongDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    static final VarHandle VH_PLATFORM_NATIVE_DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    private byte[][] pages;

    // 1024 elements per page

    /** Constructor. */
    BigLongDoubleArray(long numberOfElements, BigArrays bigArrays, boolean clearOnResize) {
        super(ELEMENTS_PER_PAGE, bigArrays, clearOnResize);
        this.size = numberOfElements;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public long getLong(long index) {
        final int pageIndex = (int) (index >>> 10);
        final int indexInPage = (int) (index & 0x3FF);
        return (long) VH_PLATFORM_NATIVE_LONG.get(pages[pageIndex], indexInPage << 4);
    }

    @Override
    public double getDouble(long index) {
        final int pageIndex = (int) (index >>> 10);
        final int indexInPage = (int) (index & 0x3FF);
        // + prev all elements, which is one long
        return (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], (indexInPage << 4) + Long.BYTES);
    }

    @Override
    public void set(long index, long lValue, double dValue) {
        final int pageIndex = (int) (index >>> 10);
        final int indexInPage = (int) (index & 0x3FF);
        final byte[] page = pages[pageIndex];
        final int offset = indexInPage << ELEMENT_SHIFT;
        VH_PLATFORM_NATIVE_LONG.set(page, offset, lValue);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 8, dValue);
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        set(index, buf, offset, len, pages, ELEMENT_SHIFT);
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
