/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.recycler.Recycler;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.OBJECT_PAGE_SIZE;

/**
 * Int array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigObjectArray<T> extends AbstractBigArray implements ObjectArray<T> {

    private static final BigObjectArray<Long> ESTIMATOR = new BigObjectArray<Long>(0, BigArrays.NON_RECYCLING_INSTANCE);

    private Object[][] pages;

    /** Constructor. */
    BigObjectArray(long size, BigArrays bigArrays) {
        super(OBJECT_PAGE_SIZE, bigArrays, true);
        this.size = size;
        pages = new Object[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newObjectPage(i);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (T) pages[pageIndex][indexInPage];
    }

    @Override
    public void set(long index, T value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final Object[] page = pages[pageIndex];
        page[indexInPage] = value;
    }

    @Override
    public T getAndSet(long index, T value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final Object[] page = pages[pageIndex];
        @SuppressWarnings("unchecked")
        final T ret = (T) page[indexInPage];
        page[indexInPage] = value;
        return ret;
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
            pages[i] = newObjectPage(i);
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

    private Object[] newObjectPage(int page) {
        if (recycler != null) {
            final Recycler.V<Object[]> v = recycler.objectPage();
            return registerNewPage(v, page, PageCacheRecycler.OBJECT_PAGE_SIZE);
        } else {
            return new Object[PageCacheRecycler.OBJECT_PAGE_SIZE];
        }
    }
}
