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

import static org.elasticsearch.common.util.BigArrays.OBJECT_PAGE_SIZE;

/**
 * Int array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigObjectArray<T> extends AbstractBigArray<Object[]> implements ObjectArray<T> {

    private static final BigObjectArray ESTIMATOR = new BigObjectArray(0, BigArrays.NON_RECYCLING_INSTANCE);

    /** Constructor. */
    @SuppressWarnings("unchecked")
    BigObjectArray(long size, BigArrays bigArrays) {
        super(size, OBJECT_PAGE_SIZE, createObjectSupplier(bigArrays), bigArrays, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return (T) getPage(pageIndex)[indexInPage];
    }

    @Override
    public T set(long index, T value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final Object[] page = getPage(pageIndex);
        @SuppressWarnings("unchecked")
        final T ret = (T) page[indexInPage];
        page[indexInPage] = value;
        return ret;
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
