/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.Arrays;

/**
 * Float array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
public final class BigDoubleArrayList extends AbstractBigArray {

    /**
     * Default page size, 16KB of memory per page.
     */
    private static final int DEFAULT_PAGE_SIZE = 1 << 11;

    private double[][] pages;

    public BigDoubleArrayList(int pageSize, long initialCapacity) {
        super(pageSize);
        pages = new double[numPages(initialCapacity)][];
    }

    public BigDoubleArrayList(long initialCapacity) {
        this(DEFAULT_PAGE_SIZE, initialCapacity);
    }

    public BigDoubleArrayList() {
        this(1024);
    }

    public double get(long index) {
        assert index >= 0 && index < size;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public void add(double d) {
        final int pageIndex = pageIndex(size);
        if (pageIndex >= pages.length) {
            final int newLength = ArrayUtil.oversize(pageIndex + 1, numBytesPerElement());
            pages = Arrays.copyOf(pages, newLength);
        }
        if (pages[pageIndex] == null) {
            pages[pageIndex] = new double[pageSize()];
        }
        final int indexInPage = indexInPage(size);
        pages[pageIndex][indexInPage] = d;
        ++size;
    }

    @Override
    protected int numBytesPerElement() {
        return RamUsageEstimator.NUM_BYTES_DOUBLE;
    }

}
