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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cache.recycler.PageCacheRecycler;

import java.util.Arrays;

/** Utility class to work with arrays. */
public enum BigArrays {
    ;

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int BYTE_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_BYTE;
    public static final int INT_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_INT;
    public static final int LONG_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_LONG;
    public static final int DOUBLE_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_DOUBLE;
    public static final int OBJECT_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    /** Returns the next size to grow when working with parallel arrays that may have different page sizes or number of bytes per element. */
    public static long overSize(long minTargetSize) {
        return overSize(minTargetSize, PAGE_SIZE_IN_BYTES / 8, 1);
    }

    /** Return the next size to grow to that is &gt;= <code>minTargetSize</code>.
     *  Inspired from {@link ArrayUtil#oversize(int, int)} and adapted to play nicely with paging. */
    public static long overSize(long minTargetSize, int pageSize, int bytesPerElement) {
        Preconditions.checkArgument(minTargetSize >= 0, "minTargetSize must be >= 0");
        Preconditions.checkArgument(pageSize >= 0, "pageSize must be > 0");
        Preconditions.checkArgument(bytesPerElement > 0, "bytesPerElement must be > 0");

        long newSize;
        if (minTargetSize < pageSize) {
            newSize = ArrayUtil.oversize((int)minTargetSize, bytesPerElement);
        } else {
            newSize = minTargetSize + (minTargetSize >>> 3);
        }

        if (newSize > pageSize) {
            // round to a multiple of pageSize
            newSize = newSize - (newSize % pageSize) + pageSize;
            assert newSize % pageSize == 0;
        }

        return newSize;
    }

    static boolean indexIsInt(long index) {
        return index == (int) index;
    }

    private static class ByteArrayWrapper extends AbstractArray implements ByteArray {

        private final byte[] array;

        ByteArrayWrapper(byte[] array, PageCacheRecycler recycler, boolean clearOnResize) {
            super(recycler, clearOnResize);
            this.array = array;
        }

        @Override
        public long size() {
            return array.length;
        }

        @Override
        public byte get(long index) {
            assert indexIsInt(index);
            return array[(int) index];
        }

        @Override
        public byte set(long index, byte value) {
            assert indexIsInt(index);
            final byte ret = array[(int) index];
            array[(int) index] = value;
            return ret;
        }

        @Override
        public void get(long index, int len, BytesRef ref) {
            assert indexIsInt(index);
            ref.bytes = array;
            ref.offset = (int) index;
            ref.length = len;
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert indexIsInt(index);
            System.arraycopy(buf, offset, array, (int) index, len);
        }

    }

    private static class IntArrayWrapper extends AbstractArray implements IntArray {

        private final int[] array;

        IntArrayWrapper(int[] array, PageCacheRecycler recycler, boolean clearOnResize) {
            super(recycler, clearOnResize);
            this.array = array;
        }

        @Override
        public long size() {
            return array.length;
        }

        @Override
        public int get(long index) {
            assert indexIsInt(index);
            return array[(int) index];
        }

        @Override
        public int set(long index, int value) {
            assert indexIsInt(index);
            final int ret = array[(int) index];
            array[(int) index] = value;
            return ret;
        }

        @Override
        public int increment(long index, int inc) {
            assert indexIsInt(index);
            return array[(int) index] += inc;
        }

    }

    private static class LongArrayWrapper extends AbstractArray implements LongArray {

        private final long[] array;

        LongArrayWrapper(long[] array, PageCacheRecycler recycler, boolean clearOnResize) {
            super(recycler, clearOnResize);
            this.array = array;
        }

        @Override
        public long size() {
            return array.length;
        }

        @Override
        public long get(long index) {
            assert indexIsInt(index);
            return array[(int) index];
        }

        @Override
        public long set(long index, long value) {
            assert indexIsInt(index);
            final long ret = array[(int) index];
            array[(int) index] = value;
            return ret;
        }

        @Override
        public long increment(long index, long inc) {
            assert indexIsInt(index);
            return array[(int) index] += inc;
        }

        @Override
        public void fill(long fromIndex, long toIndex, long value) {
            assert indexIsInt(fromIndex);
            assert indexIsInt(toIndex);
            Arrays.fill(array, (int) fromIndex, (int) toIndex, value);
        }
    }

    private static class DoubleArrayWrapper extends AbstractArray implements DoubleArray {

        private final double[] array;

        DoubleArrayWrapper(double[] array, PageCacheRecycler recycler, boolean clearOnResize) {
            super(recycler, clearOnResize);
            this.array = array;
        }

        @Override
        public long size() {
            return array.length;
        }

        @Override
        public double get(long index) {
            assert indexIsInt(index);
            return array[(int) index];
        }

        @Override
        public double set(long index, double value) {
            assert indexIsInt(index);
            double ret = array[(int) index];
            array[(int) index] = value;
            return ret;
        }

        @Override
        public double increment(long index, double inc) {
            assert indexIsInt(index);
            return array[(int) index] += inc;
        }

        @Override
        public void fill(long fromIndex, long toIndex, double value) {
            assert indexIsInt(fromIndex);
            assert indexIsInt(toIndex);
            Arrays.fill(array, (int) fromIndex, (int) toIndex, value);
        }

    }

    private static class ObjectArrayWrapper<T> extends AbstractArray implements ObjectArray<T> {

        private final Object[] array;

        ObjectArrayWrapper(Object[] array, PageCacheRecycler recycler) {
            super(recycler, true);
            this.array = array;
        }

        @Override
        public long size() {
            return array.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(long index) {
            assert indexIsInt(index);
            return (T) array[(int) index];
        }

        @Override
        public T set(long index, T value) {
            assert indexIsInt(index);
            @SuppressWarnings("unchecked")
            T ret = (T) array[(int) index];
            array[(int) index] = value;
            return ret;
        }

    }

    /** Allocate a new {@link ByteArray} of the given capacity. */
    public static ByteArray newByteArray(long size, PageCacheRecycler recycler, boolean clearOnResize) {
        if (size <= BYTE_PAGE_SIZE) {
            return new ByteArrayWrapper(new byte[(int) size], recycler, clearOnResize);
        } else {
            return new BigByteArray(size, recycler, clearOnResize);
        }
    }

    /** Allocate a new {@link ByteArray} of the given capacity. */
    public static ByteArray newByteArray(long size) {
        return newByteArray(size, null, true);
    }

    /** Resize the array to the exact provided size. */
    public static ByteArray resize(ByteArray array, long size) {
        if (array instanceof BigByteArray) {
            ((BigByteArray) array).resize(size);
            return array;
        } else {
            AbstractArray arr = (AbstractArray) array;
            final ByteArray newArray = newByteArray(size, arr.recycler, arr.clearOnResize);
            final byte[] rawArray = ((ByteArrayWrapper) array).array;
            newArray.set(0, rawArray, 0, (int) Math.min(rawArray.length, newArray.size()));
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public static ByteArray grow(ByteArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, BYTE_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_BYTE);
        return resize(array, newSize);
    }

    /** Allocate a new {@link IntArray} of the given capacity. */
    public static IntArray newIntArray(long size, PageCacheRecycler recycler, boolean clearOnResize) {
        if (size <= INT_PAGE_SIZE) {
            return new IntArrayWrapper(new int[(int) size], recycler, clearOnResize);
        } else {
            return new BigIntArray(size, recycler, clearOnResize);
        }
    }

    /** Allocate a new {@link IntArray} of the given capacity. */
    public static IntArray newIntArray(long size) {
        return newIntArray(size, null, true);
    }

    /** Resize the array to the exact provided size. */
    public static IntArray resize(IntArray array, long size) {
        if (array instanceof BigIntArray) {
            ((BigIntArray) array).resize(size);
            return array;
        } else {
            AbstractArray arr = (AbstractArray) array;
            final IntArray newArray = newIntArray(size, arr.recycler, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public static IntArray grow(IntArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, INT_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_INT);
        return resize(array, newSize);
    }

    /** Allocate a new {@link LongArray} of the given capacity. */
    public static LongArray newLongArray(long size, PageCacheRecycler recycler, boolean clearOnResize) {
        if (size <= LONG_PAGE_SIZE) {
            return new LongArrayWrapper(new long[(int) size], recycler, clearOnResize);
        } else {
            return new BigLongArray(size, recycler, clearOnResize);
        }
    }

    /** Allocate a new {@link LongArray} of the given capacity. */
    public static LongArray newLongArray(long size) {
        return newLongArray(size, null, true);
    }

    /** Resize the array to the exact provided size. */
    public static LongArray resize(LongArray array, long size) {
        if (array instanceof BigLongArray) {
            ((BigLongArray) array).resize(size);
            return array;
        } else {
            AbstractArray arr = (AbstractArray) array;
            final LongArray newArray = newLongArray(size, arr.recycler, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public static LongArray grow(LongArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, LONG_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_LONG);
        return resize(array, newSize);
    }

    /** Allocate a new {@link DoubleArray} of the given capacity. */
    public static DoubleArray newDoubleArray(long size, PageCacheRecycler recycler, boolean clearOnResize) {
        if (size <= LONG_PAGE_SIZE) {
            return new DoubleArrayWrapper(new double[(int) size], recycler, clearOnResize);
        } else {
            return new BigDoubleArray(size, recycler, clearOnResize);
        }
    }

    /** Allocate a new {@link DoubleArray} of the given capacity. */
    public static DoubleArray newDoubleArray(long size) {
        return newDoubleArray(size, null, true);
    }

    /** Resize the array to the exact provided size. */
    public static DoubleArray resize(DoubleArray array, long size) {
        if (array instanceof BigDoubleArray) {
            ((BigDoubleArray) array).resize(size);
            return array;
        } else {
            AbstractArray arr = (AbstractArray) array;
            final DoubleArray newArray = newDoubleArray(size, arr.recycler, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public static DoubleArray grow(DoubleArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, DOUBLE_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_DOUBLE);
        return resize(array, newSize);
    }

    /** Allocate a new {@link ObjectArray} of the given capacity. */
    public static <T> ObjectArray<T> newObjectArray(long size, PageCacheRecycler recycler) {
        if (size <= OBJECT_PAGE_SIZE) {
            return new ObjectArrayWrapper<T>(new Object[(int) size], recycler);
        } else {
            return new BigObjectArray<T>(size, recycler);
        }
    }

    /** Allocate a new {@link ObjectArray} of the given capacity. */
    public static <T> ObjectArray<T> newObjectArray(long size) {
        return newObjectArray(size, null);
    }

    /** Resize the array to the exact provided size. */
    public static <T> ObjectArray<T> resize(ObjectArray<T> array, long size) {
        if (array instanceof BigObjectArray) {
            ((BigObjectArray<?>) array).resize(size);
            return array;
        } else {
            final ObjectArray<T> newArray = newObjectArray(size, ((AbstractArray) array).recycler);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public static <T> ObjectArray<T> grow(ObjectArray<T> array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, OBJECT_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        return resize(array, newSize);
    }

}
