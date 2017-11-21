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

import java.util.Arrays;

import static org.elasticsearch.common.util.BigArrays.LONG_PAGE_SIZE;

/**
 * Double array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigDoubleArray extends AbstractBigArray<long[]> implements DoubleArray {

    private static final BigDoubleArray ESTIMATOR = new BigDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    /** Constructor. */
    @SuppressWarnings("unchecked")
    BigDoubleArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(size, LONG_PAGE_SIZE, createLongSupplier(bigArrays, clearOnResize), bigArrays, clearOnResize);
    }

    @Override
    public double get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return Double.longBitsToDouble(getPage(pageIndex)[indexInPage]);
    }

    @Override
    public double set(long index, double value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final long[] page = getPage(pageIndex);
        final double ret = Double.longBitsToDouble(page[indexInPage]);
        page[indexInPage] = Double.doubleToRawLongBits(value);
        return ret;
    }

    @Override
    public double increment(long index, double inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final long[] page = getPage(pageIndex);
        return page[indexInPage] = Double.doubleToRawLongBits(Double.longBitsToDouble(page[indexInPage]) + inc);
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
        final long longBits = Double.doubleToRawLongBits(value);
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            Arrays.fill(getPage(fromPage), indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, longBits);
        } else {
            Arrays.fill(getPage(fromPage), indexInPage(fromIndex), getPage(fromPage).length, longBits);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(getPage(i), longBits);
            }
            Arrays.fill(getPage(toPage), 0, indexInPage(toIndex - 1) + 1, longBits);
        }
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }
}
