/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestByteArray;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
import org.elasticsearch.tdigest.arrays.TDigestLongArray;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TDigestArrays with raw arrays and circuit breaking.
 */
public class MemoryTrackingTDigestArrays implements TDigestArrays {

    /**
     * Default no-op CB instance of the wrapper.
     *
     * @deprecated This instance shouldn't be used, and will be removed after all usages are replaced.
     */
    @Deprecated
    public static final MemoryTrackingTDigestArrays INSTANCE = new MemoryTrackingTDigestArrays(
        new NoopCircuitBreaker("default-wrapper-tdigest-arrays")
    );

    private final CircuitBreaker breaker;

    public MemoryTrackingTDigestArrays(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    @Override
    public MemoryTrackingTDigestDoubleArray newDoubleArray(int initialSize) {
        breaker.addEstimateBytesAndMaybeBreak(
            MemoryTrackingTDigestDoubleArray.estimatedRamBytesUsed(initialSize),
            "tdigest-new-double-array"
        );
        return new MemoryTrackingTDigestDoubleArray(breaker, initialSize);
    }

    @Override
    public MemoryTrackingTDigestIntArray newIntArray(int initialSize) {
        breaker.addEstimateBytesAndMaybeBreak(MemoryTrackingTDigestIntArray.estimatedRamBytesUsed(initialSize), "tdigest-new-int-array");
        return new MemoryTrackingTDigestIntArray(breaker, initialSize);
    }

    @Override
    public TDigestLongArray newLongArray(int initialSize) {
        breaker.addEstimateBytesAndMaybeBreak(MemoryTrackingTDigestLongArray.estimatedRamBytesUsed(initialSize), "tdigest-new-long-array");
        return new MemoryTrackingTDigestLongArray(breaker, initialSize);
    }

    @Override
    public TDigestByteArray newByteArray(int initialSize) {
        breaker.addEstimateBytesAndMaybeBreak(MemoryTrackingTDigestByteArray.estimatedRamBytesUsed(initialSize), "tdigest-new-byte-array");
        return new MemoryTrackingTDigestByteArray(breaker, initialSize);
    }

    public MemoryTrackingTDigestDoubleArray newDoubleArray(double[] array) {
        breaker.addEstimateBytesAndMaybeBreak(
            MemoryTrackingTDigestDoubleArray.estimatedRamBytesUsed(array.length),
            "tdigest-new-double-array"
        );
        return new MemoryTrackingTDigestDoubleArray(breaker, array);
    }

    public MemoryTrackingTDigestIntArray newIntArray(int[] array) {
        breaker.addEstimateBytesAndMaybeBreak(MemoryTrackingTDigestIntArray.estimatedRamBytesUsed(array.length), "tdigest-new-int-array");
        return new MemoryTrackingTDigestIntArray(breaker, array);
    }

    private static long estimatedArraySize(long arrayLength, long bytesPerElement) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + arrayLength * bytesPerElement);
    }

    private abstract static class AbstractMemoryTrackingArray implements Releasable, Accountable {
        protected final CircuitBreaker breaker;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        AbstractMemoryTrackingArray(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public final void close() {
            if (closed.compareAndSet(false, true)) {
                breaker.addWithoutBreaking(-ramBytesUsed());
            }
        }
    }

    public static class MemoryTrackingTDigestDoubleArray extends AbstractMemoryTrackingArray implements TDigestDoubleArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MemoryTrackingTDigestDoubleArray.class);

        private double[] array;
        private int size;

        public MemoryTrackingTDigestDoubleArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new double[initialSize]);
        }

        public MemoryTrackingTDigestDoubleArray(CircuitBreaker breaker, double[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        public static long estimatedRamBytesUsed(int size) {
            return SHALLOW_SIZE + estimatedArraySize(size, Double.BYTES);
        }

        @Override
        public long ramBytesUsed() {
            return estimatedRamBytesUsed(array.length);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public double get(int index) {
            assert index >= 0 && index < size;
            return array[index];
        }

        @Override
        public void set(int index, double value) {
            assert index >= 0 && index < size;
            array[index] = value;
        }

        @Override
        public void add(double value) {
            ensureCapacity(size + 1);
            array[size++] = value;
        }

        @Override
        public void sort() {
            Arrays.sort(array, 0, size);
        }

        @Override
        public void resize(int newSize) {
            ensureCapacity(newSize);

            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }

            size = newSize;
        }

        @Override
        public void ensureCapacity(int requiredCapacity) {
            if (requiredCapacity > array.length) {
                double[] oldArray = array;
                // Used for used bytes assertion
                long oldRamBytesUsed = ramBytesUsed();
                long oldArraySize = RamUsageEstimator.sizeOf(oldArray);

                int newSize = ArrayUtil.oversize(requiredCapacity, Double.BYTES);
                long newArraySize = estimatedArraySize(newSize, Double.BYTES);
                breaker.addEstimateBytesAndMaybeBreak(newArraySize, "tdigest-new-capacity-double-array");
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(oldArray));

                assert ramBytesUsed() - oldRamBytesUsed == newArraySize - oldArraySize
                    : "ramBytesUsed() should be aligned with manual array calculations";
            }
        }
    }

    public static class MemoryTrackingTDigestIntArray extends AbstractMemoryTrackingArray implements TDigestIntArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MemoryTrackingTDigestIntArray.class);

        private int[] array;
        private int size;

        public MemoryTrackingTDigestIntArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new int[initialSize]);
        }

        public MemoryTrackingTDigestIntArray(CircuitBreaker breaker, int[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        public static long estimatedRamBytesUsed(int size) {
            return SHALLOW_SIZE + estimatedArraySize(size, Integer.BYTES);
        }

        @Override
        public long ramBytesUsed() {
            return estimatedRamBytesUsed(array.length);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int get(int index) {
            assert index >= 0 && index < size;
            return array[index];
        }

        @Override
        public void set(int index, int value) {
            assert index >= 0 && index < size;
            array[index] = value;
        }

        @Override
        public void resize(int newSize) {
            ensureCapacity(newSize);
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }

        private void ensureCapacity(int requiredCapacity) {
            if (requiredCapacity > array.length) {
                int[] oldArray = array;
                // Used for used bytes assertion
                long oldRamBytesUsed = ramBytesUsed();
                long oldArraySize = RamUsageEstimator.sizeOf(oldArray);

                int newSize = ArrayUtil.oversize(requiredCapacity, Integer.BYTES);
                long newArraySize = estimatedArraySize(newSize, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak(newArraySize, "tdigest-new-capacity-int-array");
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(oldArray));

                assert ramBytesUsed() - oldRamBytesUsed == newArraySize - oldArraySize
                    : "ramBytesUsed() should be aligned with manual array calculations";
            }
        }
    }

    public static class MemoryTrackingTDigestLongArray extends AbstractMemoryTrackingArray implements TDigestLongArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MemoryTrackingTDigestLongArray.class);

        private long[] array;
        private int size;

        public MemoryTrackingTDigestLongArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new long[initialSize]);
        }

        public MemoryTrackingTDigestLongArray(CircuitBreaker breaker, long[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        public static long estimatedRamBytesUsed(int size) {
            return SHALLOW_SIZE + estimatedArraySize(size, Long.BYTES);
        }

        @Override
        public long ramBytesUsed() {
            return estimatedRamBytesUsed(array.length);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public long get(int index) {
            assert index >= 0 && index < size;
            return array[index];
        }

        @Override
        public void set(int index, long value) {
            assert index >= 0 && index < size;
            array[index] = value;
        }

        @Override
        public void resize(int newSize) {
            ensureCapacity(newSize);
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }

        private void ensureCapacity(int requiredCapacity) {
            if (requiredCapacity > array.length) {
                long[] oldArray = array;
                // Used for used bytes assertion
                long oldRamBytesUsed = ramBytesUsed();
                long oldArraySize = RamUsageEstimator.sizeOf(oldArray);

                int newSize = ArrayUtil.oversize(requiredCapacity, Long.BYTES);
                long newArraySize = estimatedArraySize(newSize, Long.BYTES);
                breaker.addEstimateBytesAndMaybeBreak(newArraySize, "tdigest-new-capacity-long-array");
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(oldArray));

                assert ramBytesUsed() - oldRamBytesUsed == newArraySize - oldArraySize
                    : "ramBytesUsed() should be aligned with manual array calculations";
            }
        }
    }

    public static class MemoryTrackingTDigestByteArray extends AbstractMemoryTrackingArray implements TDigestByteArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MemoryTrackingTDigestByteArray.class);

        private byte[] array;
        private int size;

        public MemoryTrackingTDigestByteArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new byte[initialSize]);
        }

        public MemoryTrackingTDigestByteArray(CircuitBreaker breaker, byte[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        public static long estimatedRamBytesUsed(int size) {
            return SHALLOW_SIZE + estimatedArraySize(size, Byte.BYTES);
        }

        @Override
        public long ramBytesUsed() {
            return estimatedRamBytesUsed(array.length);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public byte get(int index) {
            assert index >= 0 && index < size;
            return array[index];
        }

        @Override
        public void set(int index, byte value) {
            assert index >= 0 && index < size;
            array[index] = value;
        }

        @Override
        public void resize(int newSize) {
            ensureCapacity(newSize);
            if (newSize > size) {
                Arrays.fill(array, size, newSize, (byte) 0);
            }
            size = newSize;
        }

        private void ensureCapacity(int requiredCapacity) {
            if (requiredCapacity > array.length) {
                byte[] oldArray = array;
                // Used for used bytes assertion
                long oldRamBytesUsed = ramBytesUsed();
                long oldArraySize = RamUsageEstimator.sizeOf(oldArray);

                int newSize = ArrayUtil.oversize(requiredCapacity, Byte.BYTES);
                long newArraySize = estimatedArraySize(newSize, Byte.BYTES);
                breaker.addEstimateBytesAndMaybeBreak(newArraySize, "tdigest-new-capacity-byte-array");
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(oldArray));

                assert ramBytesUsed() - oldRamBytesUsed == newArraySize - oldArraySize
                    : "ramBytesUsed() should be aligned with manual array calculations";
            }
        }
    }
}
