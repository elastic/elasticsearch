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
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.LONG_PAGE_SIZE;

/**
 * Long array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigLongArray extends AbstractBigArray implements LongArray {

    private static final BigLongArray ESTIMATOR = new BigLongArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    static final VarHandle VH_PLATFORM_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private byte[][] pages;

    /** Constructor. */
    BigLongArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(LONG_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public long get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (long) VH_PLATFORM_NATIVE_LONG.get(pages[pageIndex], indexInPage << 3);
    }

    @Override
    public long set(long index, long value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final long ret = (long) VH_PLATFORM_NATIVE_LONG.get(page, indexInPage << 3);
        VH_PLATFORM_NATIVE_LONG.set(page, indexInPage << 3, value);
        return ret;
    }

    @Override
    public long increment(long index, long inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final long newVal = (long) VH_PLATFORM_NATIVE_LONG.get(page, indexInPage << 3) + inc;
        VH_PLATFORM_NATIVE_LONG.set(page, indexInPage << 3, newVal);
        return newVal;
    }

    @Override
    protected int numBytesPerElement() {
        return Long.BYTES;
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
    public void fill(long fromIndex, long toIndex, long value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        if (fromIndex == toIndex) {
            return; // empty range
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
        set(index, buf, offset, len, pages, 3);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writePages(out, Math.toIntExact(size), pages, Long.BYTES, LONG_PAGE_SIZE);
    }

    static void writePages(StreamOutput out, int size, byte[][] pages, int bytesPerValue, int pageSize) throws IOException {
        out.writeVInt(size * bytesPerValue);
        int lastPageEnd = size % pageSize;
        if (lastPageEnd == 0) {
            for (byte[] page : pages) {
                out.write(page);
            }
            return;
        }
        for (int i = 0; i < pages.length - 1; i++) {
            out.write(pages[i]);
        }
        out.write(pages[pages.length - 1], 0, lastPageEnd * bytesPerValue);
    }
}
