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

import static org.elasticsearch.common.util.PageCacheRecycler.LONG_PAGE_SIZE;

/**
 * Long array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigLongArray extends AbstractBigByteArray implements LongArray {

    private static final BigLongArray ESTIMATOR = new BigLongArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    /** Constructor. */
    BigLongArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(LONG_PAGE_SIZE, bigArrays, clearOnResize, size);
    }

    @Override
    public long get(long index) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        return (long) VH_PLATFORM_NATIVE_LONG.get(pages[pageIndex], indexInPage << 3);
    }

    @Override
    public long getAndSet(long index, long value) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        final long ret = (long) VH_PLATFORM_NATIVE_LONG.get(page, indexInPage << 3);
        VH_PLATFORM_NATIVE_LONG.set(page, indexInPage << 3, value);
        return ret;
    }

    @Override
    public void set(long index, long value) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        VH_PLATFORM_NATIVE_LONG.set(page, indexInPage << 3, value);
    }

    @Override
    public long increment(long index, long inc) {
        final int pageIndex = pageIdx(index);
        final int indexInPage = idxInPage(index);
        final byte[] page = getPageForWriting(pageIndex);
        final long newVal = (long) VH_PLATFORM_NATIVE_LONG.get(page, indexInPage << 3) + inc;
        VH_PLATFORM_NATIVE_LONG.set(page, indexInPage << 3, newVal);
        return newVal;
    }

    @Override
    protected int numBytesPerElement() {
        return Long.BYTES;
    }

    @Override
    public void fill(long fromIndex, long toIndex, long value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        if (fromIndex == toIndex) {
            return; // empty range
        }
        final int fromPage = pageIdx(fromIndex);
        final int toPage = pageIdx(toIndex - 1);
        if (fromPage == toPage) {
            fill(getPageForWriting(fromPage), idxInPage(fromIndex), idxInPage(toIndex - 1) + 1, value);
        } else {
            fill(getPageForWriting(fromPage), idxInPage(fromIndex), LONG_PAGE_SIZE, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                fill(getPageForWriting(i), 0, LONG_PAGE_SIZE, value);
            }
            fill(getPageForWriting(toPage), 0, idxInPage(toIndex - 1) + 1, value);
        }
    }

    public static void fill(byte[] page, int from, int to, long value) {
        if (from < to) {
            VH_PLATFORM_NATIVE_LONG.set(page, from << 3, value);
            fillBySelfCopy(page, from << 3, to << 3, Long.BYTES);
        }
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
        writePages(out, size, pages, Long.BYTES);
    }

    @Override
    public void fillWith(StreamInput in) throws IOException {
        readPages(in);
    }

    static void writePages(StreamOutput out, long size, byte[][] pages, int bytesPerValue) throws IOException {
        int remainedBytes = Math.toIntExact(size * bytesPerValue);
        out.writeVInt(remainedBytes);
        for (int i = 0; i < pages.length && remainedBytes > 0; i++) {
            int len = Math.min(remainedBytes, pages[i].length);
            out.writeBytes(pages[i], 0, len);
            remainedBytes -= len;
        }
    }

    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(LONG_PAGE_SIZE);

    private static int pageIdx(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int idxInPage(long index) {
        return (int) (index & LONG_PAGE_SIZE - 1);
    }
}
