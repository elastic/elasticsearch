/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
public class WrapperTDigestArrays implements TDigestArrays {

    public static final WrapperTDigestArrays INSTANCE = new WrapperTDigestArrays(new NoopCircuitBreaker("default-wrapper-tdigest-arrays"));

    private final CircuitBreaker breaker;

    public WrapperTDigestArrays(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    @Override
    public WrapperTDigestDoubleArray newDoubleArray(int initialCapacity) {
        return validate(new WrapperTDigestDoubleArray(breaker, initialCapacity));
    }

    @Override
    public WrapperTDigestIntArray newIntArray(int initialSize) {
        return validate(new WrapperTDigestIntArray(breaker, initialSize));
    }

    @Override
    public TDigestLongArray newLongArray(int initialSize) {
        return validate(new WrapperTDigestLongArray(breaker, initialSize));
    }

    @Override
    public TDigestByteArray newByteArray(int initialSize) {
        return validate(new WrapperTDigestByteArray(breaker, initialSize));
    }

    public WrapperTDigestDoubleArray newDoubleArray(double[] array) {
        return validate(new WrapperTDigestDoubleArray(breaker, array));
    }

    public WrapperTDigestIntArray newIntArray(int[] array) {
        return validate(new WrapperTDigestIntArray(breaker, array));
    }

    /**
     * Safely adjusts the breaker with an array.
     * <p>
     *     Copied from {@link org.elasticsearch.common.util.BigArrays}.
     * </p>
     */
    private <T extends Accountable & Releasable> T validate(T array) {
        boolean success = false;
        try {
            adjustBreaker(array.ramBytesUsed());
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(array);
            }
        }
        return array;
    }

    /**
     * Adjusts the breaker with the delta for an already created array.
     * <p>
     *     Copied from {@link org.elasticsearch.common.util.BigArrays}.
     * </p>
     */
    void adjustBreaker(final long delta) {
        // checking breaker means potentially tripping, but it doesn't
        // have to if the delta is negative
        if (delta > 0) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(delta, "tdigest-array-wrapper");
            } catch (CircuitBreakingException e) {
                // since we've already created the data, we need to
                // add it so closing the stream re-adjusts properly
                breaker.addWithoutBreaking(delta);
                throw e;
            }
        } else {
            breaker.addWithoutBreaking(delta);
        }
    }

    private abstract static class MemoryAwareArray implements Releasable, Accountable {
        protected final CircuitBreaker breaker;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        MemoryAwareArray(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public final void close() {
            if (closed.compareAndSet(false, true)) {
                breaker.addWithoutBreaking(-ramBytesUsed());
            }
        }
    }

    public static class WrapperTDigestDoubleArray extends MemoryAwareArray implements TDigestDoubleArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(WrapperTDigestDoubleArray.class);

        private double[] array;
        private int size;

        public WrapperTDigestDoubleArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new double[initialSize]);
        }

        public WrapperTDigestDoubleArray(CircuitBreaker breaker, double[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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
                int newSize = array.length + (array.length >> 1);
                if (newSize < requiredCapacity) {
                    newSize = requiredCapacity;
                }
                double[] newArray = new double[newSize];
                System.arraycopy(array, 0, newArray, 0, size);

                long oldRamBytesUsed = ramBytesUsed();
                array = newArray;
                breaker.addWithoutBreaking(ramBytesUsed() - oldRamBytesUsed);
            }
        }
    }

    public static class WrapperTDigestIntArray extends MemoryAwareArray implements TDigestIntArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(WrapperTDigestIntArray.class);

        private int[] array;
        private int size;

        public WrapperTDigestIntArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new int[initialSize]);
        }

        public WrapperTDigestIntArray(CircuitBreaker breaker, int[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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
            if (newSize > array.length) {
                long oldRamBytesUsed = ramBytesUsed();
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(ramBytesUsed() - oldRamBytesUsed);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }
    }

    public static class WrapperTDigestLongArray extends MemoryAwareArray implements TDigestLongArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(WrapperTDigestLongArray.class);

        private long[] array;
        private int size;

        public WrapperTDigestLongArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new long[initialSize]);
        }

        public WrapperTDigestLongArray(CircuitBreaker breaker, long[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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
            if (newSize > array.length) {
                long oldRamBytesUsed = ramBytesUsed();
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(ramBytesUsed() - oldRamBytesUsed);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }
    }

    public static class WrapperTDigestByteArray extends MemoryAwareArray implements TDigestByteArray {
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(WrapperTDigestByteArray.class);

        private byte[] array;
        private int size;

        public WrapperTDigestByteArray(CircuitBreaker breaker, int initialSize) {
            this(breaker, new byte[initialSize]);
        }

        public WrapperTDigestByteArray(CircuitBreaker breaker, byte[] array) {
            super(breaker);
            this.array = array;
            this.size = array.length;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(array);
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
            if (newSize > array.length) {
                long oldRamBytesUsed = ramBytesUsed();
                array = Arrays.copyOf(array, newSize);
                breaker.addWithoutBreaking(ramBytesUsed() - oldRamBytesUsed);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, (byte) 0);
            }
            size = newSize;
        }
    }
}
