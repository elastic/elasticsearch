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

import java.util.Locale;

/**
 * A GC friendly long[].
 * Allocating large arrays (that are not short-lived) generate fragmentation
 * in old-gen space. This breaks such large long array into fixed size pages
 * to avoid that problem.
 */
public class BigLongArray {

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private final long[][] pages;
    public final int size;

    private final int pageSize;
    private final int pageCount;

    public BigLongArray(int size) {
        this(size, DEFAULT_PAGE_SIZE);
    }

    public BigLongArray(int size, int pageSize) {
        this.size = size;
        this.pageSize = pageSize;

        int lastPageSize = size % pageSize;
        int fullPageCount = size / pageSize;
        pageCount = fullPageCount + (lastPageSize == 0 ? 0 : 1);
        pages = new long[pageCount][];

        for (int i = 0; i < fullPageCount; ++i)
            pages[i] = new long[pageSize];

        if (lastPageSize != 0)
            pages[pages.length - 1] = new long[lastPageSize];
    }

    public void set(int idx, long value) {
        if (idx < 0 || idx > size)
            throw new IndexOutOfBoundsException(String.format(Locale.ROOT, "%d is not whithin [0, %d)", idx, size));

        int page = idx / pageSize;
        int pageIdx = idx % pageSize;
        pages[page][pageIdx] = value;
    }

    public long get(int idx) {
        if (idx < 0 || idx > size)
            throw new IndexOutOfBoundsException(String.format(Locale.ROOT, "%d is not whithin [0, %d)", idx, size));

        int page = idx / pageSize;
        int pageIdx = idx % pageSize;
        return pages[page][pageIdx];
    }
}
