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

import static org.elasticsearch.common.util.PageCacheRecycler.FLOAT_PAGE_SIZE;

/**
 * Float array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigFloatArray extends AbstractBigByteArray implements FloatArray {

    private static final BigFloatArray ESTIMATOR = new BigFloatArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_FLOAT = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.nativeOrder());

    /** Constructor. */
    BigFloatArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(FLOAT_PAGE_SIZE, bigArrays, clearOnResize, size);
    }

    @Override
    public float set(long index, float value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final float ret = (float) VH_PLATFORM_NATIVE_FLOAT.get(page, indexInPage << 2);
        VH_PLATFORM_NATIVE_FLOAT.set(page, indexInPage << 2, value);
        return ret;
    }

    @Override
    public float get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (float) VH_PLATFORM_NATIVE_FLOAT.get(pages[pageIndex], indexInPage << 2);
    }

    @Override
    protected int numBytesPerElement() {
        return Float.BYTES;
    }

    @Override
    public void fill(long fromIndex, long toIndex, float value) {
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

    public static void fill(byte[] page, int from, int to, float value) {
        if (from < to) {
            VH_PLATFORM_NATIVE_FLOAT.set(page, from << 2, value);
            fillBySelfCopy(page, from << 2, to << 2, Float.BYTES);
        }
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        set(index, buf, offset, len, pages, 2);
    }
}
