/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static org.elasticsearch.common.util.BigLongArray.writePages;
import static org.elasticsearch.common.util.PageCacheRecycler.DOUBLE_PAGE_SIZE;

/**
 * Double array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigDoubleArray extends AbstractBigByteArray implements DoubleArray {

    private static final BigDoubleArray ESTIMATOR = new BigDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    /** Constructor. */
    BigDoubleArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(DOUBLE_PAGE_SIZE, bigArrays, clearOnResize, size);
    }

    @Override
    public double get(long index) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        return (double) VH_PLATFORM_NATIVE_DOUBLE.get(pages[pageIndex], indexInPage << 3);
    }

    @Override
    public void set(long index, double value) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        VH_PLATFORM_NATIVE_DOUBLE.set(page, indexInPage << 3, value);
    }

    @Override
    public double increment(long index, double inc) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        final double newVal = (double) VH_PLATFORM_NATIVE_DOUBLE.get(page, indexInPage << 3) + inc;
        VH_PLATFORM_NATIVE_DOUBLE.set(page, indexInPage << 3, newVal);
        return newVal;
    }

    @Override
    protected int numBytesPerElement() {
        return Integer.BYTES;
    }

    @Override
    public void fill(long fromIndex, long toIndex, double value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final int fromPage = pageIdx(fromIndex);
        final int toPage = pageIdx(toIndex - 1);
        if (fromPage == toPage) {
            fill(getPageForWriting(fromPage), idxInPage(fromIndex), idxInPage(toIndex - 1) + 1, value);
        } else {
            fill(getPageForWriting(fromPage), idxInPage(fromIndex), DOUBLE_PAGE_SIZE, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                fill(getPageForWriting(i), 0, DOUBLE_PAGE_SIZE, value);
            }
            fill(getPageForWriting(toPage), 0, idxInPage(toIndex - 1) + 1, value);
        }
    }

    public static void fill(byte[] page, int from, int to, double value) {
        if (from < to) {
            VH_PLATFORM_NATIVE_DOUBLE.set(page, from << 3, value);
            fillBySelfCopy(page, from << 3, to << 3, Double.BYTES);
        }
    }

    @Override
    public void fillWith(StreamInput in) throws IOException {
        readPages(in);
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        set(index, buf, offset, len, 3);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writePages(out, size, pages, Double.BYTES);
    }

    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(DOUBLE_PAGE_SIZE);

    private static int pageIdx(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int idxInPage(long index) {
        return (int) (index & DOUBLE_PAGE_SIZE - 1);
    }
}
