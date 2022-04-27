/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.Arrays;

import static org.elasticsearch.common.util.BigDoubleArray.VH_PLATFORM_NATIVE_DOUBLE;
import static org.elasticsearch.common.util.BigFloatArray.VH_PLATFORM_NATIVE_FLOAT;
import static org.elasticsearch.common.util.BigIntArray.VH_PLATFORM_NATIVE_INT;
import static org.elasticsearch.common.util.BigLongArray.VH_PLATFORM_NATIVE_LONG;

/** Utility class to work with arrays. */
public class BigArrays {

    public static final BigArrays NON_RECYCLING_INSTANCE = new BigArrays(null, null, CircuitBreaker.REQUEST);

    /** Returns the next size to grow when working with parallel arrays that
     *  may have different page sizes or number of bytes per element. */
    public static long overSize(long minTargetSize) {
        return overSize(minTargetSize, PageCacheRecycler.PAGE_SIZE_IN_BYTES / 8, 1);
    }

    /** Return the next size to grow to that is &gt;= <code>minTargetSize</code>.
     *  Inspired from {@link ArrayUtil#oversize(int, int)} and adapted to play nicely with paging. */
    public static long overSize(long minTargetSize, int pageSize, int bytesPerElement) {
        if (minTargetSize < 0) {
            throw new IllegalArgumentException("minTargetSize must be >= 0");
        }
        if (pageSize < 0) {
            throw new IllegalArgumentException("pageSize must be > 0");
        }
        if (bytesPerElement <= 0) {
            throw new IllegalArgumentException("bytesPerElement must be > 0");
        }

        long newSize;
        if (minTargetSize < pageSize) {
            newSize = Math.min(ArrayUtil.oversize((int) minTargetSize, bytesPerElement), pageSize);
        } else {
            final long pages = (minTargetSize + pageSize - 1) / pageSize; // ceil(minTargetSize/pageSize)
            newSize = pages * pageSize;
        }

        return newSize;
    }

    static boolean indexIsInt(long index) {
        return index == (int) index;
    }

    private abstract static class AbstractArrayWrapper extends AbstractArray implements BigArray {

        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ByteArrayWrapper.class);

        private final Releasable releasable;
        private final long size;

        AbstractArrayWrapper(BigArrays bigArrays, long size, Releasable releasable, boolean clearOnResize) {
            super(bigArrays, clearOnResize);
            this.releasable = releasable;
            this.size = size;
        }

        @Override
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
        public long ramBytesUsed() {
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

        @Override
        public boolean hasArray() {
            return true;
        }

        @Override
        public byte[] array() {
            return array;
        }
    }

    private static class ByteArrayAsIntArrayWrapper extends AbstractArrayWrapper implements IntArray {

        final byte[] array;

        ByteArrayAsIntArrayWrapper(BigArrays bigArrays, long size, boolean clearOnResize) {
            super(bigArrays, size, null, clearOnResize);
            assert size >= 0L && size <= PageCacheRecycler.INT_PAGE_SIZE;
            this.array = new byte[(int) size << 2];
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
        }

        @Override
        public int get(long index) {
            assert index >= 0 && index < size();
            return (int) VH_PLATFORM_NATIVE_INT.get(array, (int) index << 2);
        }

        @Override
        public int set(long index, int value) {
            assert index >= 0 && index < size();
            final int ret = (int) VH_PLATFORM_NATIVE_INT.get(array, (int) index << 2);
            VH_PLATFORM_NATIVE_INT.set(array, (int) index << 2, value);
            return ret;
        }

        @Override
        public int increment(long index, int inc) {
            assert index >= 0 && index < size();
            final int ret = (int) VH_PLATFORM_NATIVE_INT.get(array, (int) index << 2) + inc;
            VH_PLATFORM_NATIVE_INT.set(array, (int) index << 2, ret);
            return ret;
        }

        @Override
        public void fill(long fromIndex, long toIndex, int value) {
            assert fromIndex >= 0 && fromIndex <= toIndex;
            assert toIndex >= 0 && toIndex <= size();
            BigIntArray.fill(array, (int) fromIndex, (int) toIndex, value);
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert index >= 0 && index < size();
            System.arraycopy(buf, offset << 2, array, (int) index << 2, len << 2);
        }
    }

    private static class ByteArrayAsLongArrayWrapper extends AbstractArrayWrapper implements LongArray {

        private final byte[] array;

        ByteArrayAsLongArrayWrapper(BigArrays bigArrays, long size, boolean clearOnResize) {
            super(bigArrays, size, null, clearOnResize);
            assert size >= 0 && size <= PageCacheRecycler.LONG_PAGE_SIZE;
            this.array = new byte[(int) size << 3];
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
        }

        @Override
        public long get(long index) {
            assert index >= 0 && index < size();
            return (long) VH_PLATFORM_NATIVE_LONG.get(array, (int) index << 3);
        }

        @Override
        public long set(long index, long value) {
            assert index >= 0 && index < size();
            final long ret = (long) VH_PLATFORM_NATIVE_LONG.get(array, (int) index << 3);
            VH_PLATFORM_NATIVE_LONG.set(array, (int) index << 3, value);
            return ret;
        }

        @Override
        public long increment(long index, long inc) {
            assert index >= 0 && index < size();
            final long ret = (long) VH_PLATFORM_NATIVE_LONG.get(array, (int) index << 3) + inc;
            VH_PLATFORM_NATIVE_LONG.set(array, (int) index << 3, ret);
            return ret;
        }

        @Override
        public void fill(long fromIndex, long toIndex, long value) {
            assert fromIndex >= 0 && fromIndex <= toIndex;
            assert toIndex >= 0 && toIndex <= size();
            BigLongArray.fill(array, (int) fromIndex, (int) toIndex, value);
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert index >= 0 && index < size();
            System.arraycopy(buf, offset << 3, array, (int) index << 3, len << 3);
        }
    }

    private static class ByteArrayAsDoubleArrayWrapper extends AbstractArrayWrapper implements DoubleArray {

        private final byte[] array;

        ByteArrayAsDoubleArrayWrapper(BigArrays bigArrays, long size, boolean clearOnResize) {
            super(bigArrays, size, null, clearOnResize);
            assert size >= 0L && size <= PageCacheRecycler.DOUBLE_PAGE_SIZE;
            this.array = new byte[(int) size << 3];
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
        }

        @Override
        public double get(long index) {
            assert index >= 0 && index < size();
            return (double) VH_PLATFORM_NATIVE_DOUBLE.get(array, (int) index << 3);
        }

        @Override
        public double set(long index, double value) {
            assert index >= 0 && index < size();
            final double ret = (double) VH_PLATFORM_NATIVE_DOUBLE.get(array, (int) index << 3);
            VH_PLATFORM_NATIVE_DOUBLE.set(array, (int) index << 3, value);
            return ret;
        }

        @Override
        public double increment(long index, double inc) {
            assert index >= 0 && index < size();
            final double ret = (double) VH_PLATFORM_NATIVE_DOUBLE.get(array, (int) index << 3) + inc;
            VH_PLATFORM_NATIVE_DOUBLE.set(array, (int) index << 3, ret);
            return ret;
        }

        @Override
        public void fill(long fromIndex, long toIndex, double value) {
            assert fromIndex >= 0 && fromIndex <= toIndex;
            assert toIndex >= 0 && toIndex <= size();
            BigDoubleArray.fill(array, (int) fromIndex, (int) toIndex, value);
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert index >= 0 && index < size();
            System.arraycopy(buf, offset << 3, array, (int) index << 3, len << 3);
        }
    }

    private static class ByteArrayAsFloatArrayWrapper extends AbstractArrayWrapper implements FloatArray {

        private final byte[] array;

        ByteArrayAsFloatArrayWrapper(BigArrays bigArrays, long size, boolean clearOnResize) {
            super(bigArrays, size, null, clearOnResize);
            assert size >= 0 && size <= PageCacheRecycler.FLOAT_PAGE_SIZE;
            this.array = new byte[(int) size << 2];
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
        }

        @Override
        public float get(long index) {
            assert index >= 0 && index < size();
            return (float) VH_PLATFORM_NATIVE_FLOAT.get(array, (int) index << 2);
        }

        @Override
        public float set(long index, float value) {
            assert index >= 0 && index < size();
            final float ret = (float) VH_PLATFORM_NATIVE_FLOAT.get(array, (int) index << 2);
            VH_PLATFORM_NATIVE_FLOAT.set(array, (int) index << 2, value);
            return ret;
        }

        @Override
        public float increment(long index, float inc) {
            assert index >= 0 && index < size();
            final float ret = (float) VH_PLATFORM_NATIVE_FLOAT.get(array, (int) index << 2) + inc;
            VH_PLATFORM_NATIVE_FLOAT.set(array, (int) index << 2, ret);
            return ret;
        }

        @Override
        public void fill(long fromIndex, long toIndex, float value) {
            assert fromIndex >= 0 && fromIndex <= toIndex;
            assert toIndex >= 0 && toIndex <= size();
            BigFloatArray.fill(array, (int) fromIndex, (int) toIndex, value);
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            assert index >= 0 && index < size();
            System.arraycopy(buf, offset << 2, array, (int) index << 2, len << 2);
        }
    }

    private static class ObjectArrayWrapper<T> extends AbstractArrayWrapper implements ObjectArray<T> {

        private final Object[] array;

        ObjectArrayWrapper(BigArrays bigArrays, Object[] array, long size, Recycler.V<Object[]> releasable) {
            super(bigArrays, size, releasable, true);
            this.array = array;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * size()
            );
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(long index) {
            assert index >= 0 && index < size();
            return (T) array[(int) index];
        }

        @Override
        public T set(long index, T value) {
            assert index >= 0 && index < size();
            @SuppressWarnings("unchecked")
            T ret = (T) array[(int) index];
            array[(int) index] = value;
            return ret;
        }

    }

    final PageCacheRecycler recycler;
    @Nullable
    private final CircuitBreakerService breakerService;
    @Nullable
    private final CircuitBreaker breaker;
    private final boolean checkBreaker;
    private final BigArrays circuitBreakingInstance;
    private final String breakerName;

    public BigArrays(PageCacheRecycler recycler, @Nullable final CircuitBreakerService breakerService, String breakerName) {
        // Checking the breaker is disabled if not specified
        this(recycler, breakerService, breakerName, false);
    }

    protected BigArrays(
        PageCacheRecycler recycler,
        @Nullable final CircuitBreakerService breakerService,
        String breakerName,
        boolean checkBreaker
    ) {
        this.checkBreaker = checkBreaker;
        this.recycler = recycler;
        this.breakerService = breakerService;
        if (breakerService != null) {
            breaker = breakerService.getBreaker(breakerName);
        } else {
            breaker = null;
        }
        this.breakerName = breakerName;
        if (checkBreaker) {
            this.circuitBreakingInstance = this;
        } else {
            this.circuitBreakingInstance = new BigArrays(recycler, breakerService, breakerName, true);
        }
    }

    /**
     * Adjust the circuit breaker with the given delta, if the delta is
     * negative, or checkBreaker is false, the breaker will be adjusted
     * without tripping.  If the data was already created before calling
     * this method, and the breaker trips, we add the delta without breaking
     * to account for the created data.  If the data has not been created yet,
     * we do not add the delta to the breaker if it trips.
     */
    void adjustBreaker(final long delta, final boolean isDataAlreadyCreated) {
        if (this.breaker != null) {
            if (this.checkBreaker) {
                // checking breaker means potentially tripping, but it doesn't
                // have to if the delta is negative
                if (delta > 0) {
                    try {
                        breaker.addEstimateBytesAndMaybeBreak(delta, "<reused_arrays>");
                    } catch (CircuitBreakingException e) {
                        if (isDataAlreadyCreated) {
                            // since we've already created the data, we need to
                            // add it so closing the stream re-adjusts properly
                            breaker.addWithoutBreaking(delta);
                        }
                        // re-throw the original exception
                        throw e;
                    }
                } else {
                    breaker.addWithoutBreaking(delta);
                }
            } else {
                // even if we are not checking the breaker, we need to adjust
                // its' totals, so add without breaking
                breaker.addWithoutBreaking(delta);
            }
        }
    }

    /**
     * Return an instance of this BigArrays class with circuit breaking
     * explicitly enabled, instead of only accounting enabled
     */
    public BigArrays withCircuitBreaking() {
        return this.circuitBreakingInstance;
    }

    /**
     * Creates a new {@link BigArray} pointing at the specified
     * {@link CircuitBreakerService}. Use with {@link PreallocatedCircuitBreakerService}.
     */
    public BigArrays withBreakerService(CircuitBreakerService breakerService) {
        return new BigArrays(recycler, breakerService, breakerName, checkBreaker);
    }

    public CircuitBreakerService breakerService() {   // TODO this feels like it is for tests but it has escaped
        return this.circuitBreakingInstance.breakerService;
    }

    private <T extends AbstractBigArray> T resizeInPlace(T array, long newSize) {
        final long oldMemSize = array.ramBytesUsed();
        final long oldSize = array.size();
        assert oldMemSize == array.ramBytesEstimated(oldSize)
            : "ram bytes used should equal that which was previously estimated: ramBytesUsed="
                + oldMemSize
                + ", ramBytesEstimated="
                + array.ramBytesEstimated(oldSize);
        final long estimatedIncreaseInBytes = array.ramBytesEstimated(newSize) - oldMemSize;
        adjustBreaker(estimatedIncreaseInBytes, false);
        array.resize(newSize);
        return array;
    }

    private <T extends BigArray> T validate(T array) {
        boolean success = false;
        try {
            adjustBreaker(array.ramBytesUsed(), true);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(array);
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
        if (size > PageCacheRecycler.BYTE_PAGE_SIZE) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigByteArray.estimateRamBytes(size), false);
            return new BigByteArray(size, this, clearOnResize);
        } else if (size >= PageCacheRecycler.BYTE_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<byte[]> page = recycler.bytePage(clearOnResize);
            return validate(new ByteArrayWrapper(this, page.v(), size, page, clearOnResize));
        } else {
            return validate(new ByteArrayWrapper(this, new byte[(int) size], size, null, clearOnResize));
        }
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

    /** Grow an array to a size that is larger than <code>minSize</code>,
     * preserving content, and potentially reusing part of the provided array. */
    public ByteArray grow(ByteArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.BYTE_PAGE_SIZE, 1);
        return resize(array, newSize);
    }

    /** @see Arrays#hashCode(byte[]) */
    public static int hashCode(ByteArray array) {
        if (array == null) {
            return 0;
        }

        int hash = 1;
        for (long i = 0; i < array.size(); i++) {
            hash = 31 * hash + array.get(i);
        }

        return hash;
    }

    /** @see Arrays#equals(byte[], byte[]) */
    public static boolean equals(ByteArray array, ByteArray other) {
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
        if (size > PageCacheRecycler.INT_PAGE_SIZE || (size >= PageCacheRecycler.INT_PAGE_SIZE / 2 && recycler != null)) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigIntArray.estimateRamBytes(size), false);
            return new BigIntArray(size, this, clearOnResize);
        } else {
            return validate(new ByteArrayAsIntArrayWrapper(this, size, clearOnResize));
        }
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
            newArray.set(0, ((ByteArrayAsIntArrayWrapper) arr).array, 0, (int) Math.min(size, array.size()));
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>,
     * preserving content, and potentially reusing part of the provided array. */
    public IntArray grow(IntArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.INT_PAGE_SIZE, Integer.BYTES);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link LongArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public LongArray newLongArray(long size, boolean clearOnResize) {
        if (size > PageCacheRecycler.LONG_PAGE_SIZE || (size >= PageCacheRecycler.LONG_PAGE_SIZE / 2 && recycler != null)) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigLongArray.estimateRamBytes(size), false);
            return new BigLongArray(size, this, clearOnResize);
        } else {
            return validate(new ByteArrayAsLongArrayWrapper(this, size, clearOnResize));
        }
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
            newArray.set(0, ((ByteArrayAsLongArrayWrapper) arr).array, 0, (int) Math.min(size, array.size()));
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>,
     * preserving content, and potentially reusing part of the provided array. */
    public LongArray grow(LongArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.LONG_PAGE_SIZE, Long.BYTES);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link DoubleArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        if (size > PageCacheRecycler.DOUBLE_PAGE_SIZE || (size >= PageCacheRecycler.DOUBLE_PAGE_SIZE / 2 && recycler != null)) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigDoubleArray.estimateRamBytes(size), false);
            return new BigDoubleArray(size, this, clearOnResize);
        } else {
            return validate(new ByteArrayAsDoubleArrayWrapper(this, size, clearOnResize));
        }
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
            newArray.set(0, ((ByteArrayAsDoubleArrayWrapper) arr).array, 0, (int) Math.min(size, array.size()));
            array.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>,
     * preserving content, and potentially reusing part of the provided array. */
    public DoubleArray grow(DoubleArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.DOUBLE_PAGE_SIZE, Double.BYTES);
        return resize(array, newSize);
    }

    public static class DoubleBinarySearcher extends BinarySearcher {

        DoubleArray array;
        double searchFor;

        public DoubleBinarySearcher(DoubleArray array) {
            this.array = array;
            this.searchFor = Integer.MIN_VALUE;
        }

        @Override
        protected int compare(int index) {
            // Prevent use of BinarySearcher.search() and force the use of DoubleBinarySearcher.search()
            assert this.searchFor != Integer.MIN_VALUE;

            return Double.compare(array.get(index), searchFor);
        }

        @Override
        protected double distance(int index) {
            return Math.abs(array.get(index) - searchFor);
        }

        public int search(int from, int to, double searchFor) {
            this.searchFor = searchFor;
            return super.search(from, to);
        }
    }

    /**
     * Allocate a new {@link FloatArray}.
     * @param size          the initial length of the array
     * @param clearOnResize whether values should be set to 0 on initialization and resize
     */
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        if (size > PageCacheRecycler.FLOAT_PAGE_SIZE || (size >= PageCacheRecycler.FLOAT_PAGE_SIZE / 2 && recycler != null)) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigFloatArray.estimateRamBytes(size), false);
            return new BigFloatArray(size, this, clearOnResize);
        } else {
            return validate(new ByteArrayAsFloatArrayWrapper(this, size, clearOnResize));
        }
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
            newArray.set(0, ((ByteArrayAsFloatArrayWrapper) arr).array, 0, (int) Math.min(size, array.size()));
            arr.close();
            return newArray;
        }
    }

    /** Grow an array to a size that is larger than <code>minSize</code>,
     * preserving content, and potentially reusing part of the provided array. */
    public FloatArray grow(FloatArray array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.FLOAT_PAGE_SIZE, Float.BYTES);
        return resize(array, newSize);
    }

    /**
     * Allocate a new {@link ObjectArray}.
     * @param size          the initial length of the array
     */
    public <T> ObjectArray<T> newObjectArray(long size) {
        if (size > PageCacheRecycler.OBJECT_PAGE_SIZE) {
            // when allocating big arrays, we want to first ensure we have the capacity by
            // checking with the circuit breaker before attempting to allocate
            adjustBreaker(BigObjectArray.estimateRamBytes(size), false);
            return new BigObjectArray<>(size, this);
        } else if (size >= PageCacheRecycler.OBJECT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<Object[]> page = recycler.objectPage();
            return validate(new ObjectArrayWrapper<>(this, page.v(), size, page));
        } else {
            return validate(new ObjectArrayWrapper<>(this, new Object[(int) size], size, null));
        }
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

    /** Grow an array to a size that is larger than <code>minSize</code>,
     * preserving content, and potentially reusing part of the provided array. */
    public <T> ObjectArray<T> grow(ObjectArray<T> array, long minSize) {
        if (minSize <= array.size()) {
            return array;
        }
        final long newSize = overSize(minSize, PageCacheRecycler.OBJECT_PAGE_SIZE, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        return resize(array, newSize);
    }
}
