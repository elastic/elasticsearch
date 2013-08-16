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

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * Int array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
public final class BigIntArray extends AbstractBigArray implements IntArray {

    /**
     * Default page size, 16KB of memory per page.
     */
    public static final int DEFAULT_PAGE_SIZE = 1 << 12;

    private int[][] pages;

    public BigIntArray(int pageSize, long size) {
        super(pageSize);
        this.size = size;
        pages = new int[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = new int[pageSize()];
        }
    }

    public BigIntArray(long size) {
        this(DEFAULT_PAGE_SIZE, size);
    }

    public int get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public void set(long index, int value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex][indexInPage] = value;
    }

    public int increment(long index, int inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage] += inc;
    }

    @Override
    protected int numBytesPerElement() {
        return RamUsageEstimator.NUM_BYTES_INT;
    }

    @Override
    public void clear(int sentinal) {
        for (int[] page : pages) {
            Arrays.fill(page, sentinal);
        }
    }
}
