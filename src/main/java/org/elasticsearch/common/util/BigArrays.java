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

import com.google.common.base.Preconditions;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/** Utility class to work with arrays. */
public class BigArrays extends AbstractComponent {

    // TODO: switch to a circuit breaker that is shared not only on big arrays level, and applies to other request level data structures
    public static final String MAX_SIZE_IN_BYTES_SETTING = "requests.memory.breaker.limit";
    public static final BigArrays NON_RECYCLING_INSTANCE = new BigArrays(ImmutableSettings.EMPTY, null, Long.MAX_VALUE);

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int BYTE_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_BYTE;
    public static final int INT_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_INT;
    public static final int FLOAT_PAGE_SIZE = BigArrays.PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_FLOAT;
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

    private static abstract class AbstractArrayWrapper extends AbstractArray implements BigArray {

        protected static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ByteArrayWrapper.class);

        private final Releasable releasable;
        private final long size;

        AbstractArrayWrapper(BigArrays bigArrays, long size, Releasable releasable, boolean clearOnResize) {
            super(bigArrays, clearOnResize);
            this.releasable = releasable;
            this.size = size;
        }

        public final long size() {
            return size;
        }

        @Override
        protected final void doClose() {
            Releasables.close(releasable);
        }

    }

    private static class ByteArrayWrapper extends AbstractArrayWrapper implements ByteArray {

        private final byte[] array;

        ByteArrayWrapper(BigArrays bigArrays, byte[] array, long size, Recycler.V<byte[]> releasable, boolean clearOnResize) {
            super(bigArrays, size, releasable, clearOnResize);
            this.array = array;
        }

        @Override
        public long sizeInBytes() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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
        public boolean get(long index, int len, BytesRef ref) {
            assert indexIsInt(index);
            ref.bytes = array;
            ref.offset = (int) index;
            ref.length = len;
            return false;
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert indexIsInt(index);
            System.arraycopy(buf, offset, array, (int) index, len);
        }

        @Override
        public void fill(long fromIndex, long toIndex, byte value) {
            assert indexIsInt(fromIndex);
            assert indexIsInt(toIndex);
            Arrays.fill(array, (int) fromIndex, (int) toIndex, value);
        }
    }

    private static class IntArrayWrapper extends AbstractArrayWrapper implements IntArray {

        private final int[] array;

        IntArrayWrapper(BigArrays bigArrays, int[] array, long size, Recycler.V<int[]> releasable, boolean clearOnResize) {
            super(bigArrays, size, releasable, clearOnResize);
            this.array = array;
        }

        @Override
        public long sizeInBytes() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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

        @Override
        public void fill(long fromIndex, long toIndex, int value) {
            assert indexIsInt(fromIndex);
            assert indexIsInt(toIndex);
            Arrays.fill(array, (int) fromIndex, (int) toIndex, value);
        }

    }

    private static class LongArrayWrapper extends AbstractArrayWrapper implements LongArray {

        private final long[] array;

        LongArrayWrapper(BigArrays bigArrays, long[] array, long size, Recycler.V<long[]> releasable, boolean clearOnResize) {
            super(bigArrays, size, releasable, clearOnResize);
            this.array = array;
        }

        @Override
        public long sizeInBytes() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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

    private static class DoubleArrayWrapper extends AbstractArrayWrapper implements DoubleArray {

        private final double[] array;

        DoubleArrayWrapper(BigArrays bigArrays, double[] array, long size, Recycler.V<double[]> releasable, boolean clearOnResize) {
            super(bigArrays, size, releasable, clearOnResize);
            this.array = array;
        }

        @Override
        public long sizeInBytes() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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

    private static class FloatArrayWrapper extends AbstractArrayWrapper implements FloatArray {

        private final float[] array;

        FloatArrayWrapper(BigArrays bigArrays, float[] array, long size, Recycler.V<float[]> releasable, boolean clearOnResize) {
            super(bigArrays, size, releasable, clearOnResize);
            this.array = array;
        }

        @Override
        public long sizeInBytes() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
        }

        @Override
        public float get(long index) {
            assert indexIsInt(index);
            return array[(int) index];
        }

        @Override
        public float set(long index, float value) {
            assert indexIsInt(index);
            float ret = array[(int) index];
            array[(int) index] = value;
            return ret;
        }

        @Override
        public float increment(long index, float inc) {
            assert indexIsInt(index);
            return array[(int) index] += inc;
        }

        @Override
        public void fill(long fromIndex, long toIndex, float value) {
            assert indexIsInt(fromIndex);
            assert indexIsInt(toIndex);
            Arrays.fill(array, (int) fromIndex, (int) toIndex, value);
        }

    }

    private static class ObjectArrayWrapper<T> extends AbstractArrayWrapper implements ObjectArray<T> {

        private final Object[] array;

        ObjectArrayWrapper(BigArrays bigArrays, Object[] array, long size, Recycler.V<Object[]> releasable) {
            super(bigArrays, size, releasable, true);
            this.array = array;
        }

        @Override
        public long sizeInBytes() {
            return SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * size());
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

    final PageCacheRecycler recycler;
    final AtomicLong ramBytesUsed;
    final long maxSizeInBytes;

    @Inject
    public BigArrays(Settings settings, PageCacheRecycler recycler) {
        this(settings, recycler, settings.getAsMemory(MAX_SIZE_IN_BYTES_SETTING, Long.toString(Long.MAX_VALUE)).bytes());
    }

    private BigArrays(Settings settings, PageCacheRecycler recycler, final long maxSizeInBytes) {
        super(settings);
        this.maxSizeInBytes = maxSizeInBytes;
        this.recycler = recycler;
        ramBytesUsed = new AtomicLong();
    }

    private void validate(long delta) {
        final long totalSizeInBytes = ramBytesUsed.addAndGet(delta);
        if (totalSizeInBytes > maxSizeInBytes) {
            throw new ElasticsearchIllegalStateException("Maximum number of bytes allocated exceeded: [" + totalSizeInBytes + "] (> " + maxSizeInBytes + ")");
        }
    }

    private <T extends AbstractBigArray> T resizeInPlace(T array, long newSize) {
        final long oldMemSize = array.sizeInBytes();
        array.resize(newSize);
        validate(array.sizeInBytes() - oldMemSize);
        return array;
    }

    private <T extends BigArray> T validate(T array) {
        boolean success = false;
        try {
            validate(array.sizeInBytes());
            success = true;
        } finally {
            if (!success) {
                Releasables.closeWhileHandlingException(array);
            }
        }
        return array;
    }

    /**
     * Allocate a new {@link ByteArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        final ByteArray array;
        if (size > BYTE_PAGE_SIZE) {
            array = new BigByteArray(size, this, clearOnResize);
        } else if (size >= BYTE_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<byte[]> page = recycler.bytePage(clearOnResize);
            array = new ByteArrayWrapper(this, page.v(), size, page, clearOnResize);
        } else {
            array = new ByteArrayWrapper(this, new byte[(int) size], size, null, clearOnResize);
        }
        return validate(array);
    }

    /**
     * Allocate a new {@link ByteArray} initialized with zeros.
     * @param size          the initial length of the array
     */
    public ByteArray newByteArray(long size) {
        return newByteArray(size, true);
    }

    /** Resize the array to the exact provided size. */
    public ByteArray resize(ByteArray array, long size) {
        if (array instanceof BigByteArray) {
            return resizeInPlace((BigByteArray) array, size);
        } else {
            AbstractArray arr = (AbstractArray) array;
            final ByteArray newArray = newByteArray(size, arr.clearOnResize);
            final byte[] rawArray = ((ByteArrayWrapper) array).array;
            newArray.set(0, rawArray, 0, (int) Math.min(rawArray.length, newArray.size()));
            arr.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public ByteArray grow(ByteArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, BYTE_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_BYTE);
        return resize(array, newSize);
    }

    /** @see Arrays.hashCode(byte[]) */
    public int hashCode(ByteArray array) {
        if (array == null) {
            return 0;
        }

        int hash = 1;
        for (long i = 0; i < array.size(); i++) {
            hash = 31 * hash + array.get(i);
        }

        return hash;
    }

    /** @see Arrays.equals(byte[], byte[]) */
    public boolean equals(ByteArray array, ByteArray other) {
        if (array == other) {
            return true;
        }

        if (array.size() != other.size()) {
            return false;
        }

        for (long i = 0; i < array.size(); i++) {
            if (array.get(i) != other.get(i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Allocate a new {@link IntArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public IntArray newIntArray(long size, boolean clearOnResize) {
        final IntArray array;
        if (size > INT_PAGE_SIZE) {
            array = new BigIntArray(size, this, clearOnResize);
        } else if (size >= INT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<int[]> page = recycler.intPage(clearOnResize);
            array = new IntArrayWrapper(this, page.v(), size, page, clearOnResize);
        } else {
            array = new IntArrayWrapper(this, new int[(int) size], size, null, clearOnResize);
        }
        return validate(array);
    }

    /**
     * Allocate a new {@link IntArray}.
     * @param size          the initial length of the array
     */
    public IntArray newIntArray(long size) {
        return newIntArray(size, true);
    }

    /** Resize the array to the exact provided size. */
    public IntArray resize(IntArray array, long size) {
        if (array instanceof BigIntArray) {
            return resizeInPlace((BigIntArray) array, size);
        } else {
            AbstractArray arr = (AbstractArray) array;
            final IntArray newArray = newIntArray(size, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public IntArray grow(IntArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, INT_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_INT);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link LongArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public LongArray newLongArray(long size, boolean clearOnResize) {
        final LongArray array;
        if (size > LONG_PAGE_SIZE) {
            array = new BigLongArray(size, this, clearOnResize);
        } else if (size >= LONG_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<long[]> page = recycler.longPage(clearOnResize);
            array = new LongArrayWrapper(this, page.v(), size, page, clearOnResize);
        } else {
            array = new LongArrayWrapper(this, new long[(int) size], size, null, clearOnResize);
        }
        return validate(array);
    }

    /**
     * Allocate a new {@link LongArray}.
     * @param size          the initial length of the array
     */
    public LongArray newLongArray(long size) {
        return newLongArray(size, true);
    }

    /** Resize the array to the exact provided size. */
    public LongArray resize(LongArray array, long size) {
        if (array instanceof BigLongArray) {
            return resizeInPlace((BigLongArray) array, size);
        } else {
            AbstractArray arr = (AbstractArray) array;
            final LongArray newArray = newLongArray(size, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public LongArray grow(LongArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, LONG_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_LONG);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link DoubleArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        final DoubleArray arr;
        if (size > DOUBLE_PAGE_SIZE) {
            arr = new BigDoubleArray(size, this, clearOnResize);
        } else if (size >= DOUBLE_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<double[]> page = recycler.doublePage(clearOnResize);
            arr = new DoubleArrayWrapper(this, page.v(), size, page, clearOnResize);
        } else {
            arr = new DoubleArrayWrapper(this, new double[(int) size], size, null, clearOnResize);
        }
        return validate(arr);
    }

    /** Allocate a new {@link DoubleArray} of the given capacity. */
    public DoubleArray newDoubleArray(long size) {
        return newDoubleArray(size, true);
    }

    /** Resize the array to the exact provided size. */
    public DoubleArray resize(DoubleArray array, long size) {
        if (array instanceof BigDoubleArray) {
            return resizeInPlace((BigDoubleArray) array, size);
        } else {
            AbstractArray arr = (AbstractArray) array;
            final DoubleArray newArray = newDoubleArray(size, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public DoubleArray grow(DoubleArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, DOUBLE_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_DOUBLE);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link FloatArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        final FloatArray array;
        if (size > FLOAT_PAGE_SIZE) {
            array = new BigFloatArray(size, this, clearOnResize);
        } else if (size >= FLOAT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<float[]> page = recycler.floatPage(clearOnResize);
            array = new FloatArrayWrapper(this, page.v(), size, page, clearOnResize);
        } else {
            array = new FloatArrayWrapper(this, new float[(int) size], size, null, clearOnResize);
        }
        return validate(array);
    }

    /** Allocate a new {@link FloatArray} of the given capacity. */
    public FloatArray newFloatArray(long size) {
        return newFloatArray(size, true);
    }

    /** Resize the array to the exact provided size. */
    public FloatArray resize(FloatArray array, long size) {
        if (array instanceof BigFloatArray) {
            return resizeInPlace((BigFloatArray) array, size);
        } else {
            AbstractArray arr = (AbstractArray) array;
            final FloatArray newArray = newFloatArray(size, arr.clearOnResize);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            arr.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public FloatArray grow(FloatArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, FLOAT_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_FLOAT);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link ObjectArray}.
     * @param size          the initial length of the array
     */
    public <T> ObjectArray<T> newObjectArray(long size) {
        final ObjectArray<T> array;
        if (size > OBJECT_PAGE_SIZE) {
            array = new BigObjectArray<>(size, this);
        } else if (size >= OBJECT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<Object[]> page = recycler.objectPage();
            array = new ObjectArrayWrapper<>(this, page.v(), size, page);
        } else {
            array = new ObjectArrayWrapper<>(this, new Object[(int) size], size, null);
        }
        return validate(array);
    }

    /** Resize the array to the exact provided size. */
    public <T> ObjectArray<T> resize(ObjectArray<T> array, long size) {
        if (array instanceof BigObjectArray) {
            return resizeInPlace((BigObjectArray<T>) array, size);
        } else {
            final ObjectArray<T> newArray = newObjectArray(size);
            for (long i = 0, end = Math.min(size, array.size()); i < end; ++i) {
                newArray.set(i, array.get(i));
            }
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>, preserving content, and potentially reusing part of the provided array. */
    public <T> ObjectArray<T> grow(ObjectArray<T> array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, OBJECT_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        return resize(array, newSize);
    }

    /**
     * Return an approximate number of bytes that have been allocated but not released yet.
     */
    public long sizeInBytes() {
        return ramBytesUsed.get();
    }
}
