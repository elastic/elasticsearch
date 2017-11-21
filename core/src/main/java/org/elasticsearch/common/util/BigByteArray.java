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

import org.apache.lucene.util.BytesRef;

import java.util.Arrays;

import static org.elasticsearch.common.util.BigArrays.BYTE_PAGE_SIZE;

/**
 * Byte array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 */
final class BigByteArray extends AbstractBigArray<byte[]> implements ByteArray {

    private static final BigByteArray ESTIMATOR = new BigByteArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    /** Constructor. */
    @SuppressWarnings("unchecked")
    BigByteArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(size, BYTE_PAGE_SIZE, createByteSupplier(bigArrays, clearOnResize), bigArrays, clearOnResize);
    }

    @Override
    public byte get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return getPage(pageIndex)[indexInPage];
    }

    @Override
    public byte set(long index, byte value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = getPage(pageIndex);
        final byte ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    @Override
    public boolean get(long index, int len, BytesRef ref) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            ref.bytes = getPage(pageIndex);
            ref.offset = indexInPage;
            ref.length = len;
            return false;
        } else {
            ref.bytes = new byte[len];
            ref.offset = 0;
            ref.length = pageSize() - indexInPage;
            System.arraycopy(getPage(pageIndex), indexInPage, ref.bytes, 0, ref.length);
            do {
                ++pageIndex;
                final int copyLength = Math.min(pageSize(), len - ref.length);
                System.arraycopy(getPage(pageIndex), 0, ref.bytes, ref.length, copyLength);
                ref.length += copyLength;
            } while (ref.length < len);
            return true;
        }
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        if (indexInPage + len <= pageSize()) {
            System.arraycopy(buf, offset, getPage(pageIndex), indexInPage, len);
        } else {
            int copyLen = pageSize() - indexInPage;
            System.arraycopy(buf, offset, getPage(pageIndex), indexInPage, copyLen);
            do {
                ++pageIndex;
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, pageSize());
                System.arraycopy(buf, offset, getPage(pageIndex), 0, copyLen);
            } while (len > copyLen);
        }
    }

    @Override
    public void fill(long fromIndex, long toIndex, byte value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        byte[] page = getPage(fromPage);
        if (fromPage == toPage) {
            Arrays.fill(page, indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, value);
        } else {
            Arrays.fill(page, indexInPage(fromIndex), page.length, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(getPage(i), value);
            }
            Arrays.fill(getPage(toPage), 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    @Override
    protected int numBytesPerElement() {
        return 1;
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }
}
