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
 * Float array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigFloatArray extends AbstractBigArray<int[]> implements FloatArray {

    private static final BigFloatArray ESTIMATOR = new BigFloatArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    /** Constructor. */
    @SuppressWarnings("unchecked")
    BigFloatArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(size, INT_PAGE_SIZE, createIntSupplier(bigArrays, clearOnResize), bigArrays, clearOnResize);
    }

    @Override
    public float set(long index, float value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = getPage(pageIndex);
        final float ret = Float.intBitsToFloat(page[indexInPage]);
        page[indexInPage] = Float.floatToRawIntBits(value);
        return ret;
    }

    @Override
    public float increment(long index, float inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = getPage(pageIndex);
        return page[indexInPage] = Float.floatToRawIntBits(Float.intBitsToFloat(page[indexInPage]) + inc);
    }

    @Override
    public float get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return Float.intBitsToFloat(getPage(pageIndex)[indexInPage]);
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
        final int intBits = Float.floatToRawIntBits(value);
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            Arrays.fill(getPage(fromPage), indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, intBits);
        } else {
            Arrays.fill(getPage(fromPage), indexInPage(fromIndex), getPage(fromPage).length, intBits);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(getPage(i), intBits);
            }
            Arrays.fill(getPage(toPage), 0, indexInPage(toIndex - 1) + 1, intBits);
        }
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

}
