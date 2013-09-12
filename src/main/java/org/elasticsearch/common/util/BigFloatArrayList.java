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

/**
 * Float array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
public final class BigFloatArrayList extends AbstractBigArray {

    /**
     * Default page size, 16KB of memory per page.
     */
    private static final int DEFAULT_PAGE_SIZE = 1 << 12;

    private float[][] pages;

    public BigFloatArrayList(int pageSize, long initialCapacity) {
        super(pageSize);
        pages = new float[numPages(initialCapacity)][];
    }

    public BigFloatArrayList(long initialCapacity) {
        this(DEFAULT_PAGE_SIZE, initialCapacity);
    }

    public BigFloatArrayList() {
        this(1024);
    }

    public float get(long index) {
        assert index >= 0 && index < size;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public void add(float f) {
        final int pageIndex = pageIndex(size);
        pages = ArrayUtil.grow(pages, pageIndex + 1);
        if (pages[pageIndex] == null) {
            pages[pageIndex] = new float[pageSize()];
        }
        final int indexInPage = indexInPage(size);
        pages[pageIndex][indexInPage] = f;
        ++size;
    }

    @Override
    protected int numBytesPerElement() {
        return RamUsageEstimator.NUM_BYTES_FLOAT;
    }

}
