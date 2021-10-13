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

import static org.elasticsearch.common.util.PageCacheRecycler.DOUBLE_PAGE_SIZE;

/**
 * Double array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigDoubleArray extends AbstractBigArray implements DoubleArray {

    private static final BigDoubleArray ESTIMATOR = new BigDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    private static final VarHandle doublePlatformNative = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    private byte[][] pages;

    /** Constructor. */
    BigDoubleArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(DOUBLE_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public double get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (double) doublePlatformNative.get(pages[pageIndex], indexInPage << 3);
    }

    @Override
    public double set(long index, double value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final double ret = (double) doublePlatformNative.get(page, indexInPage << 3);
        doublePlatformNative.set(page, indexInPage << 3, value);
        return ret;
    }

    @Override
    public double increment(long index, double inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final double newVal = (double) doublePlatformNative.get(page, indexInPage << 3) + inc;
        doublePlatformNative.set(page, indexInPage << 3, newVal);
        return newVal;
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

    @Override
    public void fill(long fromIndex, long toIndex, double value) {
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

    public static void fill(byte[] page, int from, int to, double value) {
        for (int i = from; i < to; i++) {
            doublePlatformNative.set(page, i << 3, value);
        }
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

}
