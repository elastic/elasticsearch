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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cache.recycler.PageCacheRecycler;

import java.util.Arrays;

import static org.elasticsearch.common.util.BigArrays.BYTE_PAGE_SIZE;

/**
 * Byte array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigByteArray extends AbstractBigArray implements ByteArray {

    private byte[][] pages;

    /** Constructor. */
    public BigByteArray(long size, PageCacheRecycler recycler, boolean clearOnResize) {
        super(BYTE_PAGE_SIZE, recycler, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    @Override
    public byte get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    @Override
    public byte set(long index, byte value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final byte ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    @Override
    public void get(long index, int len, BytesRef ref) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            ref.bytes = pages[pageIndex];
            ref.offset = indexInPage;
            ref.length = len;
        } else {
            ref.bytes = new byte[len];
            ref.offset = 0;
            ref.length = pageSize() - indexInPage;
            System.arraycopy(pages[pageIndex], indexInPage, ref.bytes, 0, ref.length);
            do {
                ++pageIndex;
                final int copyLength = Math.min(pageSize(), len - ref.length);
                System.arraycopy(pages[pageIndex], 0, ref.bytes, ref.length, copyLength);
                ref.length += copyLength;
            } while (ref.length < len);
        }
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            System.arraycopy(buf, offset, pages[pageIndex], indexInPage, len);
        } else {
            int copyLen = pageSize() - indexInPage;
            System.arraycopy(buf, offset, pages[pageIndex], indexInPage, copyLen);
            do {
                ++pageIndex;
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, pageSize());
                System.arraycopy(buf, offset, pages[pageIndex], 0, copyLen);
            } while (len > copyLen);
        }
    }

    @Override
    protected int numBytesPerElement() {
        return RamUsageEstimator.NUM_BYTES_BYTE;
    }

    /** Change the size of this array. Content between indexes <code>0</code> and <code>min(size(), newSize)</code> will be preserved. */
    public void resize(long newSize) {
        final int numPages = numPages(newSize);
        if (numPages > pages.length) {
            pages = Arrays.copyOf(pages, ArrayUtil.oversize(numPages, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        for (int i = numPages - 1; i >= 0 && pages[i] == null; --i) {
            pages[i] = newBytePage(i);
        }
        for (int i = numPages; i < pages.length && pages[i] != null; ++i) {
            pages[i] = null;
            releasePage(i);
        }
        this.size = newSize;
    }

}
