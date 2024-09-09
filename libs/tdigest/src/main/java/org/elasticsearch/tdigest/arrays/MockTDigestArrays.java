/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest.arrays;

import java.util.Arrays;

/**
 * Temporal TDigestArraysFactory mock with raw arrays.
 * <p>
 *     For testing only, delete after the right implementation for BigArrays is made.
 * </p>
 */
public class MockTDigestArrays implements TDigestArraysFactory {

    public static final MockTDigestArrays INSTANCE = new MockTDigestArrays();

    private MockTDigestArrays() {
    }

    @Override
    public TDigestDoubleArray newDoubleArray(int initialCapacity) {
        return new MockTDigestDoubleArray(initialCapacity);
    }

    public static class MockTDigestDoubleArray implements TDigestDoubleArray {
        private double[] array;
        private int size;

        MockTDigestDoubleArray(int initialSize) {
            array = new double[initialSize];
            size = initialSize;
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
        public void sorted() {
            Arrays.sort(array, 0, size);
        }

        @Override
        public void ensureCapacity(int requiredCapacity) {
            if (requiredCapacity > array.length) {
                double[] newArray = new double[requiredCapacity];
                System.arraycopy(array, 0, newArray, 0, size);
                array = newArray;
            }
        }
    }
}
