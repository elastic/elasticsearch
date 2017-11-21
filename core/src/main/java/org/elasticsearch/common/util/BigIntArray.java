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

import static org.elasticsearch.common.util.BigArrays.INT_PAGE_SIZE;

/**
 * Int array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigIntArray extends AbstractBigArray<int[]> implements IntArray {

    private static final BigIntArray ESTIMATOR = new BigIntArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    /** Constructor. */
    @SuppressWarnings("unchecked")
    BigIntArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(size, INT_PAGE_SIZE, createIntSupplier(bigArrays, clearOnResize), bigArrays, clearOnResize);
    }

    @Override
    public int get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return getPage(pageIndex)[indexInPage];
    }

    @Override
    public int set(long index, int value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = getPage(pageIndex);
        final int ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    @Override
    public int increment(long index, int inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return getPage(pageIndex)[indexInPage] += inc;
    }

    @Override
    public void fill(long fromIndex, long toIndex, int value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
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

    @Override
    protected int numBytesPerElement() {
        return Integer.BYTES;
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

}
