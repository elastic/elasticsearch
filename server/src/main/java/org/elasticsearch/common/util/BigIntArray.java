/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static org.elasticsearch.common.util.BigLongArray.writePages;
import static org.elasticsearch.common.util.PageCacheRecycler.INT_PAGE_SIZE;

/**
 * Int array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigIntArray extends AbstractBigByteArray implements IntArray {
    private static final BigIntArray ESTIMATOR = new BigIntArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    /** Constructor. */
    BigIntArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(INT_PAGE_SIZE, bigArrays, clearOnResize, size);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writePages(out, size, pages, Integer.BYTES);
    }

    @Override
    public int get(long index) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        return (int) VH_PLATFORM_NATIVE_INT.get(pages[pageIndex], indexInPage << 2);
    }

    @Override
    public int getAndSet(long index, int value) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        final int ret = (int) VH_PLATFORM_NATIVE_INT.get(page, indexInPage << 2);
        VH_PLATFORM_NATIVE_INT.set(page, indexInPage << 2, value);
        return ret;
    }

    @Override
    public void set(long index, int value) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        VH_PLATFORM_NATIVE_INT.set(getPageForWriting(pageIndex), indexInPage << 2, value);
    }

    @Override
    public int increment(long index, int inc) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        final int newVal = (int) VH_PLATFORM_NATIVE_INT.get(page, indexInPage << 2) + inc;
        VH_PLATFORM_NATIVE_INT.set(page, indexInPage << 2, newVal);
        return newVal;
    }

    @Override
    public void fill(long fromIndex, long toIndex, int value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final int fromPage = pageIdx(fromIndex);
        final int toPage = pageIdx(toIndex - 1);
        if (fromPage == toPage) {
            fill(getPageForWriting(fromPage), idxInPage(fromIndex), idxInPage(toIndex - 1) + 1, value);
        } else {
            fill(getPageForWriting(fromPage), idxInPage(fromIndex), INT_PAGE_SIZE, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                fill(getPageForWriting(i), 0, INT_PAGE_SIZE, value);
            }
            fill(getPageForWriting(toPage), 0, idxInPage(toIndex - 1) + 1, value);
        }
    }

    @Override
    public void fillWith(StreamInput in) throws IOException {
        readPages(in);
    }

    public static void fill(byte[] page, int from, int to, int value) {
        if (from < to) {
            VH_PLATFORM_NATIVE_INT.set(page, from << 2, value);
            fillBySelfCopy(page, from << 2, to << 2, Integer.BYTES);
        }
    }

    @Override
    protected int numBytesPerElement() {
        return Integer.BYTES;
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        set(index, buf, offset, len, 2);
    }

    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(INT_PAGE_SIZE);

    private static int pageIdx(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int idxInPage(long index) {
        return (int) (index & INT_PAGE_SIZE - 1);
    }
}
