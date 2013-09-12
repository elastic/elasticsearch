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

import com.google.common.base.Preconditions;

/** Common implementation for array lists that slice data into fixed-size blocks. */
abstract class AbstractBigArray {

    private final int pageShift;
    private final int pageMask;
    protected long size;

    protected AbstractBigArray(int pageSize) {
        Preconditions.checkArgument(pageSize >= 128, "pageSize must be >= 128");
        Preconditions.checkArgument((pageSize & (pageSize - 1)) == 0, "pageSize must be a power of two");
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = pageSize - 1;
        size = 0;
    }

    final int numPages(long capacity) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        Preconditions.checkArgument(numPages <= Integer.MAX_VALUE, "pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity);
        return (int) numPages;
    }

    final int pageSize() {
        return pageMask + 1;
    }

    final int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    final int indexInPage(long index) {
        return (int) (index & pageMask);
    }

    public final long size() {
        return size;
    }

    protected abstract int numBytesPerElement();

    public final long sizeInBytes() {
        // rough approximate, we only take into account the size of the values, not the overhead of the array objects
        return ((long) pageIndex(size - 1) + 1) * pageSize() * numBytesPerElement();
    }

}
