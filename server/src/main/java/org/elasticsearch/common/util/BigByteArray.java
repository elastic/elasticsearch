/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.util.BigLongArray.writePages;
import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * Byte array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigByteArray extends AbstractBigArray implements ByteArray {

    private static final BigByteArray ESTIMATOR = new BigByteArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    private byte[][] pages;

    /** Constructor. */
    BigByteArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(BYTE_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writePages(out, Math.toIntExact(size), pages, Byte.BYTES, BYTE_PAGE_SIZE);
    }

    @Override
    public byte get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    @Override
    public byte set(long index, byte value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final byte ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    @Override
    public boolean get(long index, int len, BytesRef ref) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            ref.bytes = pages[pageIndex];
            ref.offset = indexInPage;
            ref.length = len;
            return false;
        } else {
            ref.bytes = new byte[len];
            ref.offset = 0;
            ref.length = pageSize() - indexInPage;
            System.arraycopy(pages[pageIndex], indexInPage, ref.bytes, 0, ref.length);
            do {
                ++pageIndex;
                final int copyLength = Math.min(pageSize(), len - ref.length);
                System.arraycopy(pages[pageIndex], 0, ref.bytes, ref.length, copyLength);
                ref.length += copyLength;
            } while (ref.length < len);
            return true;
        }
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            System.arraycopy(buf, offset, pages[pageIndex], indexInPage, len);
        } else {
            int copyLen = pageSize() - indexInPage;
            System.arraycopy(buf, offset, pages[pageIndex], indexInPage, copyLen);
            do {
                ++pageIndex;
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, pageSize());
                System.arraycopy(buf, offset, pages[pageIndex], 0, copyLen);
            } while (len > copyLen);
        }
    }

    @Override
    public void fill(long fromIndex, long toIndex, byte value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            Arrays.fill(pages[fromPage], indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, value);
        } else {
            Arrays.fill(pages[fromPage], indexInPage(fromIndex), pages[fromPage].length, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(pages[i], value);
            }
            Arrays.fill(pages[toPage], 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] array() {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    protected int numBytesPerElement() {
        return 1;
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

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

}
