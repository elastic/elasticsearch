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
    // public static final int ELEMENT_SIZE_IN_BYTES = 1 << 5; // 32 (8 + 8 + 8 = 24bytes + 8 pad = 32 byte)
    public static final int ELEMENT_SIZE_IN_BYTES = 24;   // 8 + 8 + 8 = 24bytes

    public static final int ELEMENTS_PER_PAGE = PAGE_SIZE_IN_BYTES / ELEMENT_SIZE_IN_BYTES;

    private static final BigLongDoubleDoubleArray ESTIMATOR = new BigLongDoubleDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    static final VarHandle VH_PLATFORM_NATIVE_DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    private byte[][] pages;
    // 512 elements per page (accounting for the extra 8 bytes padding)

    /** Constructor. */
    BigLongDoubleDoubleArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(bigArrays, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    int numPages(long capacity) {
        final long numPages = (capacity + ELEMENTS_PER_PAGE - 1) / ELEMENTS_PER_PAGE;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + ELEMENTS_PER_PAGE + " is too small for such as capacity: " + capacity);
        }
        return (int) numPages;
    }

    @Override
    int pageSize() {
        return ELEMENTS_PER_PAGE;
    }

    int pageIndex(long index) {
        return (int) (index / ELEMENTS_PER_PAGE);
    }

    int indexInPage(long index) {
        return (int) (index % ELEMENTS_PER_PAGE);
    }

    @Override
    public long getLong0(OpaqueIndex index) {
        return (long) VH_PLATFORM_NATIVE_LONG.get(pages[index.pageIndex], index.byteIndex);
    }

    @Override
    public double getDouble0(OpaqueIndex index) {
        // + prev all elements, which is one long
        return (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[index.pageIndex], index.byteIndex + 8);
    }

    @Override
    public double getDouble1(OpaqueIndex index) {
        // + prev all elements, which is one long and one double
        return (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[index.pageIndex], index.byteIndex + 16);
    }

    @Override
    public void set(OpaqueIndex index, long lValue0, double dValue0, double dValue1) {
        final byte[] page = pages[index.pageIndex];
        final int offset = index.byteIndex;
        VH_PLATFORM_NATIVE_LONG.set(page, offset, lValue0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 8, dValue0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 16, dValue1);
    }

    @Override
    public void increment(OpaqueIndex index, long lValue0Inc, double dValue0Inc, double dValue1Inc) {
        final byte[] page = pages[index.pageIndex];
        final int offset = index.byteIndex;
        var newLong0 = (long) VH_PLATFORM_NATIVE_LONG.get(page, offset) + lValue0Inc;
        var newDouble0 = (double) VH_PLATFORM_NATIVE_DOUBLE.get(page, offset + 8) + dValue0Inc;
        var newDouble1 = (double) VH_PLATFORM_NATIVE_DOUBLE.get(page, offset + 16) + dValue1Inc;
        VH_PLATFORM_NATIVE_LONG.set(page, offset, newLong0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 8, newDouble0);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, offset + 16, newDouble1);
    }

    @Override
    public void get(OpaqueIndex index, Holder holder) {
        final byte[] page = pages[index.pageIndex];
        final int offset = index.byteIndex;
        holder.setLong0((long) VH_PLATFORM_NATIVE_LONG.get(page, offset));
        holder.setDouble0((double) VH_PLATFORM_NATIVE_DOUBLE.get(page, offset + 8));
        holder.setDouble1((double) VH_PLATFORM_NATIVE_DOUBLE.get(page, offset + 16));
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
