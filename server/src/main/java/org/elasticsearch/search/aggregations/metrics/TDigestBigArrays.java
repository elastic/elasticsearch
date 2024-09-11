/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;

/**
 * {@link TDigestArrays} implementation using {@link BigArrays}.
 */
public class TDigestBigArrays implements TDigestArrays {
    public static final TDigestBigArrays NON_RECYCLING_INSTANCE = new TDigestBigArrays(BigArrays.NON_RECYCLING_INSTANCE);

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

    public static class TDigestIntBigArray implements TDigestIntArray, Releasable {
        private final IntArray array;

        TDigestIntBigArray(BigArrays bigArrays, int initialSize) {
            this.array = bigArrays.newIntArray(initialSize);
        }

        @Override
        public int size() {
            return Math.toIntExact(array.size());
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
        public void close() {
            array.close();
        }
    }

    public static class TDigestDoubleBigArray implements TDigestDoubleArray, Releasable {
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
}
