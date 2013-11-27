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
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/** Utility class to work with arrays. */
public enum BigArrays {
    ;

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;

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

    private static class IntArrayWrapper implements IntArray {

        private final int[] array;

        IntArrayWrapper(int[] array) {
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

    private static class LongArrayWrapper implements LongArray {

        private final long[] array;

        LongArrayWrapper(long[] array) {
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

    private static class DoubleArrayWrapper implements DoubleArray {

        private final double[] array;

        DoubleArrayWrapper(double[] array) {
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

    private static class ObjectArrayWrapper<T> implements ObjectArray<T> {

        private final Object[] array;

        ObjectArrayWrapper(Object[] array) {
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

    /** Allocate a new {@link IntArray} of the given capacity. */
    public static IntArray newIntArray(long size) {
        if (size <= BigIntArray.PAGE_SIZE) {
            return new IntArrayWrapper(new int[(int) size]);
        } else {
            return new BigIntArray(size);
        }
    }

    /** Resize the array to the exact provided size. */
    public static IntArray resize(IntArray array, long size) {
        if (array instanceof BigIntArray) {
            ((BigIntArray) array).resize(size);
            return array;
        } else {
            final IntArray newArray = newIntArray(size);
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
        final long newSize = overSize(minSize, BigIntArray.PAGE_SIZE, RamUsageEstimator.NUM_BYTES_INT);
        return resize(array, newSize);
    }

    /** Allocate a new {@link LongArray} of the given capacity. */
    public static LongArray newLongArray(long size) {
        if (size <= BigLongArray.PAGE_SIZE) {
            return new LongArrayWrapper(new long[(int) size]);
        } else {
            return new BigLongArray(size);
        }
    }

    /** Resize the array to the exact provided size. */
    public static LongArray resize(LongArray array, long size) {
        if (array instanceof BigLongArray) {
            ((BigLongArray) array).resize(size);
            return array;
        } else {
            final LongArray newArray = newLongArray(size);
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
        final long newSize = overSize(minSize, BigLongArray.PAGE_SIZE, RamUsageEstimator.NUM_BYTES_LONG);
        return resize(array, newSize);
    }

    /** Allocate a new {@link LongArray} of the given capacity. */
    public static DoubleArray newDoubleArray(long size) {
        if (size <= BigLongArray.PAGE_SIZE) {
            return new DoubleArrayWrapper(new double[(int) size]);
        } else {
            return new BigDoubleArray(size);
        }
    }

    /** Resize the array to the exact provided size. */
    public static DoubleArray resize(DoubleArray array, long size) {
        if (array instanceof BigDoubleArray) {
            ((BigDoubleArray) array).resize(size);
            return array;
        } else {
            final DoubleArray newArray = newDoubleArray(size);
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
        final long newSize = overSize(minSize, BigDoubleArray.PAGE_SIZE, RamUsageEstimator.NUM_BYTES_DOUBLE);
        return resize(array, newSize);
    }

    /** Allocate a new {@link LongArray} of the given capacity. */
    public static <T> ObjectArray<T> newObjectArray(long size) {
        if (size <= BigLongArray.PAGE_SIZE) {
            return new ObjectArrayWrapper<T>(new Object[(int) size]);
        } else {
            return new BigObjectArray<T>(size);
        }
    }

    /** Resize the array to the exact provided size. */
    public static <T> ObjectArray<T> resize(ObjectArray<T> array, long size) {
        if (array instanceof BigObjectArray) {
            ((BigObjectArray<?>) array).resize(size);
            return array;
        } else {
            final ObjectArray<T> newArray = newObjectArray(size);
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
        final long newSize = overSize(minSize, BigObjectArray.PAGE_SIZE, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        return resize(array, newSize);
    }

}
