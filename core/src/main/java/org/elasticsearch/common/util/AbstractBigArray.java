/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.recycler.Recycler;

import java.util.Arrays;
import java.util.function.Supplier;

/** Common implementation for array lists that slice data into fixed-size blocks. */
abstract class AbstractBigArray<V> extends AbstractArray {

    protected Recycler.V<V>[] pages;
    private final Supplier<Recycler.V<V>> pageSupplier;
    private final PageCacheRecycler recycler;

    protected long size;
    protected int offset;
    private final int pageShift;
    private final int pageMask;

    @SuppressWarnings("unchecked")
    protected AbstractBigArray(long size, int pageSize, Supplier<Recycler.V<V>> pageSupplier, BigArrays bigArrays, boolean clearOnResize) {
        super(bigArrays, clearOnResize);
        if (pageSize < 128) {
            throw new IllegalArgumentException("pageSize must be >= 128");
        }
        if ((pageSize & (pageSize - 1)) != 0) {
            throw new IllegalArgumentException("pageSize must be a power of two");
        }

        this.size = size;
        this.offset = 0;
        this.pageSupplier = pageSupplier;
        this.recycler = bigArrays.recycler;
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = pageSize - 1;
        this.pages = new Recycler.V[numPages(size)];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newPage();
        }
    }

    final V getPage(int pageIndex) {
        return pages[pageIndex].v();
    }

    final int pageSize() {
        return pageMask + 1;
    }

    final int pageIndex(long index) {
        long indexWithOffset = index + offset;
        return (int) (indexWithOffset >>> pageShift);
    }

    final int indexInPage(long index) {
        long indexWithOffset = index + offset;
        return (int) (indexWithOffset & pageMask);
    }

    @Override
    public final long size() {
        return size;
    }

    /** Change the size of this array. Content between indexes <code>0</code> and <code>min(size(), newSize)</code> will be preserved. */
    public void resize(long newSize) {
        final int numPages = numPages(newSize + offset);
        if (numPages > pages.length) {
            pages = Arrays.copyOf(pages, ArrayUtil.oversize(numPages, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        for (int i = numPages - 1; i >= 0 && pages[i] == null; --i) {
            pages[i] = newPage();
        }
        for (int i = numPages; i < pages.length && pages[i] != null; ++i) {
            pages[i].close();
            pages[i] = null;
        }
        this.size = newSize;
    }

    public void dropFromHead(long dropFromHead) {
        if (dropFromHead > size) {
            throw new IllegalArgumentException("");
        }

        int numberInFirstPage = pageSize() - offset;

        if (numberInFirstPage > dropFromHead) {
            offset += dropFromHead;
            size -= dropFromHead;
            return;
        }

        int newOffset = indexInPage(dropFromHead);

        int newFirstPage = pageIndex(dropFromHead);

        dropPagesFromHead(newFirstPage);

        offset = newOffset;
        size -= dropFromHead;
    }

    @SuppressWarnings("unchecked")
    private void dropPagesFromHead(int n) {
        for (int i = 0; i < n; ++i) {
            pages[i].close();
            pages[i] = null;
        }

        int tailPage = pageIndex(size - 1);
        int numPages = (tailPage - n) + 1;

        int oversize = ArrayUtil.oversize(numPages, RamUsageEstimator.NUM_BYTES_OBJECT_REF);

        Recycler.V<V>[] newPages;
        if (oversize == pages.length) {
            newPages = pages;
        } else {
            newPages = new Recycler.V[oversize];
        }
        System.arraycopy(pages, n, newPages, 0, numPages);
        pages = newPages;

        for (int i = numPages; i < pages.length && pages[i] != null; ++i) {
            pages[i] = null;
        }
    }

    protected abstract int numBytesPerElement();

    @Override
    public final long ramBytesUsed() {
        return ramBytesEstimated(size);
    }

    /** Given the size of the array, estimate the number of bytes it will use. */
    public final long ramBytesEstimated(final long size) {
        // rough approximate, we only take into account the size of the values, not the overhead of the array objects
        return ((long) pageIndex(size - 1) + 1) * pageSize() * numBytesPerElement();
    }

    private int numPages(long capacity) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity);
        }
        return (int) numPages;
    }

    private Recycler.V<V> newPage() {
        return pageSupplier.get();
    }

    static Supplier<Recycler.V<long[]>> createLongSupplier(BigArrays bigArrays, boolean clearOnResize) {
        if (bigArrays.recycler == null) {
            return () -> new NonRecycledPage<>(new long[BigArrays.LONG_PAGE_SIZE]);
        } else {
            return () -> bigArrays.recycler.longPage(clearOnResize);
        }
    }

    static Supplier<Recycler.V<Object[]>> createObjectSupplier(BigArrays bigArrays) {
        if (bigArrays.recycler == null) {
            return () -> new NonRecycledPage<>(new Object[BigArrays.OBJECT_PAGE_SIZE]);
        } else {
            return bigArrays.recycler::objectPage;
        }
    }

    static Supplier<Recycler.V<int[]>> createIntSupplier(BigArrays bigArrays, boolean clearOnResize) {
        if (bigArrays.recycler == null) {
            return () -> new NonRecycledPage<>(new int[BigArrays.INT_PAGE_SIZE]);
        } else {
            return () -> bigArrays.recycler.intPage(clearOnResize);
        }
    }

    static Supplier<Recycler.V<byte[]>> createByteSupplier(BigArrays bigArrays, boolean clearOnResize) {
        if (bigArrays.recycler == null) {
            return () -> new NonRecycledPage<>(new byte[BigArrays.BYTE_PAGE_SIZE]);
        } else {
            return () -> bigArrays.recycler.bytePage(clearOnResize);
        }
    }

    @Override
    protected final void doClose() {
        if (recycler != null) {
            Releasables.close(pages);
            pages = null;
        }
    }

    static class NonRecycledPage<V> implements Recycler.V<V> {

        private final V value;
        private final Releasable releasable;

        NonRecycledPage(V value) {
            this.value = value;
            this.releasable = () -> {};
        }

        public V v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return false;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }
}
