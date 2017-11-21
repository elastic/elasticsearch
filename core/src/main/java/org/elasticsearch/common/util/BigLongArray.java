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
import org.elasticsearch.common.recycler.Recycler;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.BigArrays.LONG_PAGE_SIZE;

/**
 * Long array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigLongArray extends AbstractBigArray<long[]> implements LongArray {

    private static final BigLongArray ESTIMATOR = new BigLongArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    /** Constructor. */
    @SuppressWarnings("unchecked")
    BigLongArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(size, LONG_PAGE_SIZE, createLongSupplier(bigArrays, clearOnResize), bigArrays, clearOnResize);
    }

    @Override
    public long get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return getPage(pageIndex)[indexInPage];
    }

    @Override
    public long set(long index, long value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final long[] page = getPage(pageIndex);
        final long ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    @Override
    public long increment(long index, long inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return getPage(pageIndex)[indexInPage] += inc;
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
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            Arrays.fill(getPage(fromPage), indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, value);
        } else {
            Arrays.fill(getPage(fromPage), indexInPage(fromIndex), getPage(fromPage).length, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(getPage(i), value);
            }
            Arrays.fill(getPage(toPage), 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }
}
