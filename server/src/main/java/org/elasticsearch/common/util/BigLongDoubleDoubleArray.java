/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public final class BigLongDoubleDoubleArray extends AbstractBigArray implements LongDoubleDoubleArray {

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int ELEMENT_SHIFT = 5;
    public static final int ELEMENT_SIZE_IN_BYTES = 1 << 5;
    public static final int ELEMENTS_PER_PAGE = PAGE_SIZE_IN_BYTES / ELEMENT_SIZE_IN_BYTES;

    private static final BigLongDoubleDoubleArray ESTIMATOR = new BigLongDoubleDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    static final VarHandle VH_PLATFORM_NATIVE_DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    private byte[][] pages;

    // 512 elements per page (accounting for the extra 8 bytes padding)

    /** Constructor. */
    public BigLongDoubleDoubleArray(long numberOfElements, BigArrays bigArrays, boolean clearOnResize) {
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
        var d = (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], (indexInPage << ELEMENT_SHIFT) + Long.BYTES);
        return d;
    }

    @Override
    public double getDouble1(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        // + prev all elements, which is one long and one double
        return (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], (indexInPage << ELEMENT_SHIFT) + Long.BYTES + Double.BYTES);
    }

    @Override
    public void set(long index, long lValue0, double dValue0, double dValue1) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        VH_PLATFORM_NATIVE_LONG.set(page, indexInPage << ELEMENT_SHIFT, lValue0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, (indexInPage << ELEMENT_SHIFT) + Long.BYTES, dValue0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, (indexInPage << ELEMENT_SHIFT) + Long.BYTES + Double.BYTES, dValue1);
        // ignore padding - should be 0
    }

    @Override
    protected int numBytesPerElement() {
        return ELEMENT_SIZE_IN_BYTES;
    }

    @Override
    public void resize(long newSize) {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: implement
    }
}
