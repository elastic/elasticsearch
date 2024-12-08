/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestByteArray;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
import org.elasticsearch.tdigest.arrays.TDigestLongArray;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MemoryTrackingTDigestArraysTests extends ESTestCase {
    // Int arrays

    public void testIntEmpty() {
        try (TDigestIntArray array = intArray(0)) {
            assertThat(array.size(), equalTo(0));
        }
    }

    public void testIntGetAndSet() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestIntArray array = intArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            int value = randomInt();
            for (int i = 9; i < initialSize; i++) {
                array.set(i, value);
            }

            for (int i = 0; i < initialSize; i++) {
                if (i < 9) {
                    assertThat(array.get(i), equalTo(0));
                } else {
                    assertThat(array.get(i), equalTo(value));
                }
            }
        }
    }

    public void testIntResize() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestIntArray array = intArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            // Fill with a non-zero value
            int value = randomBoolean() ? randomIntBetween(Integer.MIN_VALUE, -1) : randomIntBetween(1, Integer.MAX_VALUE);
            for (int i = 0; i < initialSize; i++) {
                array.set(i, value);
            }

            // Resize to a size-1
            array.resize(initialSize - 1);
            assertThat(array.size(), equalTo(initialSize - 1));

            for (int i = 0; i < initialSize - 1; i++) {
                assertThat(array.get(i), equalTo(value));
            }

            // Resize to the original size + 1
            array.resize(initialSize + 1);
            assertThat(array.size(), equalTo(initialSize + 1));

            // Ensure all new elements are 0
            for (int i = 0; i < initialSize - 1; i++) {
                if (i < initialSize) {
                    assertThat(array.get(i), equalTo(value));
                } else {
                    assertThat(array.get(i), equalTo(0));
                }
            }
        }
    }

    public void testIntBulkSet() {
        int targetArraySize = randomIntBetween(10, 1000);
        int sourceArraySize = randomIntBetween(0, 1000);

        try (TDigestIntArray target = intArray(targetArraySize); TDigestIntArray source = intArray(sourceArraySize)) {
            assertThat(target.size(), equalTo(targetArraySize));
            assertThat(source.size(), equalTo(sourceArraySize));

            int value = randomInt();
            for (int i = 0; i < sourceArraySize; i++) {
                source.set(i, value);
            }

            int targetOffset = randomIntBetween(0, targetArraySize);
            int sourceOffset = randomIntBetween(0, sourceArraySize);
            int elementsToCopy = randomIntBetween(0, Math.min(sourceArraySize - sourceOffset, targetArraySize - targetOffset));

            target.set(targetOffset, source, sourceOffset, elementsToCopy);

            for (int i = 0; i < targetArraySize; i++) {
                if (i < targetOffset || i >= targetOffset + elementsToCopy) {
                    assertThat(target.get(i), equalTo(0));
                } else {
                    assertThat(target.get(i), equalTo(value));
                }
            }
        }
    }

    // Long arrays

    public void testLongEmpty() {
        try (TDigestIntArray array = intArray(0)) {
            assertThat(array.size(), equalTo(0));
        }
    }

    public void testLongGetAndSet() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestLongArray array = longArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            long value = randomLong();
            for (int i = 9; i < initialSize; i++) {
                array.set(i, value);
            }

            for (int i = 0; i < initialSize; i++) {
                if (i < 9) {
                    assertThat(array.get(i), equalTo(0L));
                } else {
                    assertThat(array.get(i), equalTo(value));
                }
            }
        }
    }

    public void testLongResize() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestLongArray array = longArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            // Fill with a non-zero value
            long value = randomBoolean() ? randomLongBetween(Long.MIN_VALUE, -1) : randomLongBetween(1, Long.MAX_VALUE);
            for (int i = 0; i < initialSize; i++) {
                array.set(i, value);
            }

            // Resize to a size-1
            array.resize(initialSize - 1);
            assertThat(array.size(), equalTo(initialSize - 1));

            for (int i = 0; i < initialSize - 1; i++) {
                assertThat(array.get(i), equalTo(value));
            }

            // Resize to the original size + 1
            array.resize(initialSize + 1);
            assertThat(array.size(), equalTo(initialSize + 1));

            // Ensure all new elements are 0
            for (int i = 0; i < initialSize - 1; i++) {
                if (i < initialSize) {
                    assertThat(array.get(i), equalTo(value));
                } else {
                    assertThat(array.get(i), equalTo(0L));
                }
            }
        }
    }

    // Byte arrays

    public void testByteEmpty() {
        try (TDigestByteArray array = byteArray(0)) {
            assertThat(array.size(), equalTo(0));
        }
    }

    public void testByteGetAndSet() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestByteArray array = byteArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            byte value = randomByte();
            for (int i = 9; i < initialSize; i++) {
                array.set(i, value);
            }

            for (int i = 0; i < initialSize; i++) {
                if (i < 9) {
                    assertThat(array.get(i), equalTo((byte) 0));
                } else {
                    assertThat(array.get(i), equalTo(value));
                }
            }
        }
    }

    public void testByteResize() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestByteArray array = byteArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            // Fill with a non-zero value
            byte value = randomBoolean() ? randomByteBetween(Byte.MIN_VALUE, (byte) -1) : randomByteBetween((byte) 1, Byte.MAX_VALUE);
            for (int i = 0; i < initialSize; i++) {
                array.set(i, value);
            }

            // Resize to a size-1
            array.resize(initialSize - 1);
            assertThat(array.size(), equalTo(initialSize - 1));

            for (int i = 0; i < initialSize - 1; i++) {
                assertThat(array.get(i), equalTo(value));
            }

            // Resize to the original size + 1
            array.resize(initialSize + 1);
            assertThat(array.size(), equalTo(initialSize + 1));

            // Ensure all new elements are 0
            for (int i = 0; i < initialSize - 1; i++) {
                if (i < initialSize) {
                    assertThat(array.get(i), equalTo(value));
                } else {
                    assertThat(array.get(i), equalTo((byte) 0));
                }
            }
        }
    }

    // Double arrays

    public void testDoubleEmpty() {
        try (TDigestDoubleArray array = doubleArray(0)) {
            assertThat(array.size(), equalTo(0));
        }
    }

    public void testDoubleGetAndSet() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestDoubleArray array = doubleArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            double value = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
            for (int i = 9; i < initialSize; i++) {
                array.set(i, value);
            }

            for (int i = 0; i < initialSize; i++) {
                if (i < 9) {
                    assertThat(array.get(i), equalTo(0.0));
                } else {
                    assertThat(array.get(i), equalTo(value));
                }
            }
        }
    }

    public void testDoubleAdd() {
        int initialSize = randomIntBetween(10, 1000);
        try (TDigestDoubleArray array = doubleArray(initialSize)) {
            assertThat(array.size(), equalTo(initialSize));

            int newValueCount = randomIntBetween(1, 100);
            if (randomBoolean()) {
                array.ensureCapacity(initialSize + newValueCount);
            }
            double value = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
            for (int i = 0; i < newValueCount; i++) {
                array.add(value);
            }

            for (int i = 0; i < newValueCount; i++) {
                if (i < initialSize) {
                    assertThat(array.get(i), equalTo(0.0));
                } else {
                    assertThat(array.get(i), equalTo(value));
                }
            }
        }
    }

    public void testDoubleBulkSet() {
        int targetArraySize = randomIntBetween(10, 1000);
        int sourceArraySize = randomIntBetween(0, 1000);

        try (TDigestDoubleArray target = doubleArray(targetArraySize); TDigestDoubleArray source = doubleArray(sourceArraySize)) {
            assertThat(target.size(), equalTo(targetArraySize));
            assertThat(source.size(), equalTo(sourceArraySize));

            double value = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
            for (int i = 0; i < sourceArraySize; i++) {
                source.set(i, value);
            }

            int targetOffset = randomIntBetween(0, targetArraySize);
            int sourceOffset = randomIntBetween(0, sourceArraySize);
            int elementsToCopy = randomIntBetween(0, Math.min(sourceArraySize - sourceOffset, targetArraySize - targetOffset));

            target.set(targetOffset, source, sourceOffset, elementsToCopy);

            for (int i = 0; i < targetArraySize; i++) {
                if (i < targetOffset || i >= targetOffset + elementsToCopy) {
                    assertThat(target.get(i), equalTo(0.0));
                } else {
                    assertThat(target.get(i), equalTo(value));
                }
            }
        }
    }

    public void testDoubleSort() {
        try (TDigestDoubleArray array = doubleArray(0)) {
            int elementsToAdd = randomIntBetween(0, 100);
            array.ensureCapacity(elementsToAdd);
            for (int i = 0; i < elementsToAdd; i++) {
                array.add(randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true));
            }

            array.sort();

            double previous = -Double.MAX_VALUE;
            for (int i = 0; i < array.size(); i++) {
                double current = array.get(i);
                assertThat(current, greaterThanOrEqualTo(previous));
                previous = current;
            }
        }
    }

    // Helpers

    private TDigestIntArray intArray(int initialSize) {
        return arrays().newIntArray(initialSize);
    }

    private TDigestLongArray longArray(int initialSize) {
        return arrays().newLongArray(initialSize);
    }

    private TDigestByteArray byteArray(int initialSize) {
        return arrays().newByteArray(initialSize);
    }

    private TDigestDoubleArray doubleArray(int initialSize) {
        return arrays().newDoubleArray(initialSize);
    }

    private TDigestArrays arrays() {
        return new MemoryTrackingTDigestArrays(newLimitedBreaker(ByteSizeValue.ofMb(100)));
    }
}
