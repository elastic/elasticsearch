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

package org.elasticsearch.tdigest.arrays;

import java.util.Arrays;

/**
 * Temporal TDigestArrays with raw arrays.
 *
 * <p>
 *     Delete after the right implementation for BigArrays is made.
 * </p>
 */
public class WrapperTDigestArrays implements TDigestArrays {

    public static final WrapperTDigestArrays INSTANCE = new WrapperTDigestArrays();

    private WrapperTDigestArrays() {}

    @Override
    public WrapperTDigestDoubleArray newDoubleArray(int initialCapacity) {
        return new WrapperTDigestDoubleArray(initialCapacity);
    }

    @Override
    public WrapperTDigestIntArray newIntArray(int initialSize) {
        return new WrapperTDigestIntArray(initialSize);
    }

    @Override
    public TDigestLongArray newLongArray(int initialSize) {
        return new WrapperTDigestLongArray(initialSize);
    }

    @Override
    public TDigestByteArray newByteArray(int initialSize) {
        return new WrapperTDigestByteArray(initialSize);
    }

    public WrapperTDigestDoubleArray newDoubleArray(double[] array) {
        return new WrapperTDigestDoubleArray(array);
    }

    public WrapperTDigestIntArray newIntArray(int[] array) {
        return new WrapperTDigestIntArray(array);
    }

    public static class WrapperTDigestDoubleArray implements TDigestDoubleArray {
        private double[] array;
        private int size;

        public WrapperTDigestDoubleArray(int initialSize) {
            this(new double[initialSize]);
        }

        public WrapperTDigestDoubleArray(double[] array) {
            this.array = array;
            this.size = array.length;
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
        public void ensureCapacity(int requiredCapacity) {
            if (requiredCapacity > array.length) {
                int newSize = array.length + (array.length >> 1);
                if (newSize < requiredCapacity) {
                    newSize = requiredCapacity;
                }
                double[] newArray = new double[newSize];
                System.arraycopy(array, 0, newArray, 0, size);
                array = newArray;
            }
        }

        @Override
        public void resize(int newSize) {
            if (newSize > array.length) {
                array = Arrays.copyOf(array, newSize);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }
    }

    public static class WrapperTDigestIntArray implements TDigestIntArray {
        private int[] array;
        private int size;

        public WrapperTDigestIntArray(int initialSize) {
            this(new int[initialSize]);
        }

        public WrapperTDigestIntArray(int[] array) {
            this.array = array;
            this.size = array.length;
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
                array = Arrays.copyOf(array, newSize);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }
    }

    public static class WrapperTDigestLongArray implements TDigestLongArray {
        private long[] array;
        private int size;

        public WrapperTDigestLongArray(int initialSize) {
            this(new long[initialSize]);
        }

        public WrapperTDigestLongArray(long[] array) {
            this.array = array;
            this.size = array.length;
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
                array = Arrays.copyOf(array, newSize);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, 0);
            }
            size = newSize;
        }
    }

    public static class WrapperTDigestByteArray implements TDigestByteArray {
        private byte[] array;
        private int size;

        public WrapperTDigestByteArray(int initialSize) {
            this(new byte[initialSize]);
        }

        public WrapperTDigestByteArray(byte[] array) {
            this.array = array;
            this.size = array.length;
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
                array = Arrays.copyOf(array, newSize);
            }
            if (newSize > size) {
                Arrays.fill(array, size, newSize, (byte) 0);
            }
            size = newSize;
        }
    }
}
