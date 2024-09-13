/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestByteArray;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
import org.elasticsearch.tdigest.arrays.TDigestLongArray;

/**
 * {@link TDigestArrays} implementation using {@link BigArrays}.
 */
public class TDigestBigArrays implements TDigestArrays {
    private final BigArrays bigArrays;

    public TDigestBigArrays(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    @Override
    public TDigestDoubleBigArray newDoubleArray(int initialSize) {
        return new TDigestDoubleBigArray(bigArrays, initialSize);
    }

    @Override
    public TDigestIntBigArray newIntArray(int initialSize) {
        return new TDigestIntBigArray(bigArrays, initialSize);
    }

    @Override
    public TDigestLongBigArray newLongArray(int initialSize) {
        return new TDigestLongBigArray(bigArrays, initialSize);
    }

    @Override
    public TDigestByteBigArray newByteArray(int initialSize) {
        return new TDigestByteBigArray(bigArrays, initialSize);
    }

    public static class TDigestDoubleBigArray implements TDigestDoubleArray {
        private final BigArrays bigArrays;

        private DoubleArray array;
        private int size;

        TDigestDoubleBigArray(BigArrays bigArrays, int initialSize) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.newDoubleArray(initialSize);
            this.size = initialSize;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public double get(int index) {
            return array.get(index);
        }

        @Override
        public void set(int index, double value) {
            array.set(index, value);
        }

        @Override
        public void add(double value) {
            ensureCapacity(size + 1);
            array.set(size, value);
            size++;
        }

        @Override
        public void ensureCapacity(int requiredCapacity) {
            this.array = bigArrays.grow(array, requiredCapacity);
        }

        @Override
        public void resize(int newSize) {
            ensureCapacity(newSize);
            size = newSize;
        }

        @Override
        public void sort() {
            // TODO: Swap with a better implementation...
            for (int i = 0; i < size - 1; i++) {
                for (int j = i + 1; j < size; j++) {
                    if (get(i) > get(j)) {
                        double temp = get(i);
                        set(i, get(j));
                        set(j, temp);
                    }
                }
            }
        }

        @Override
        public void close() {
            array.close();
        }
    }

    public static class TDigestIntBigArray implements TDigestIntArray {
        private final BigArrays bigArrays;
        private IntArray array;
        private int size;

        TDigestIntBigArray(BigArrays bigArrays, int initialSize) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.newIntArray(initialSize);
            this.size = initialSize;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int get(int index) {
            return array.get(index);
        }

        @Override
        public void set(int index, int value) {
            array.set(index, value);
        }

        @Override
        public void resize(int newSize) {
            array = bigArrays.grow(array, newSize);

            if (newSize > size) {
                array.fill(size, newSize, 0);
            }

            size = newSize;
        }

        @Override
        public void close() {
            array.close();
        }
    }

    public static class TDigestLongBigArray implements TDigestLongArray {
        private final BigArrays bigArrays;
        private LongArray array;
        private int size;

        TDigestLongBigArray(BigArrays bigArrays, int initialSize) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.newLongArray(initialSize);
            this.size = initialSize;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public long get(int index) {
            return array.get(index);
        }

        @Override
        public void set(int index, long value) {
            array.set(index, value);
        }

        @Override
        public void resize(int newSize) {
            array = bigArrays.grow(array, newSize);

            if (newSize > size) {
                array.fill(size, newSize, 0);
            }

            size = newSize;
        }

        @Override
        public void close() {
            array.close();
        }
    }

    public static class TDigestByteBigArray implements TDigestByteArray {
        private final BigArrays bigArrays;
        private ByteArray array;
        private int size;

        TDigestByteBigArray(BigArrays bigArrays, int initialSize) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.newByteArray(initialSize);
            this.size = initialSize;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public byte get(int index) {
            return array.get(index);
        }

        @Override
        public void set(int index, byte value) {
            array.set(index, value);
        }

        @Override
        public void resize(int newSize) {
            array = bigArrays.grow(array, newSize);

            if (newSize > size) {
                array.fill(size, newSize, (byte) 0);
            }

            size = newSize;
        }

        @Override
        public void close() {
            array.close();
        }
    }
}
