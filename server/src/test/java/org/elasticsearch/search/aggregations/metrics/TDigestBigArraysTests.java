/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
import org.elasticsearch.tdigest.arrays.WrapperTDigestArrays;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TDigestBigArraysTests extends ESTestCase {
    public void testIntEmpty() {
        try (TDigestIntArray array = intArray(0)) {
            assertThat(array.size(), equalTo(0));
        }
    }

    public void testIntBasicOperations() {
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

    public void testDoubleEmpty() {
        try (TDigestDoubleArray array = doubleArray(0)) {
            assertThat(array.size(), equalTo(0));
        }
    }

    public void testDoubleBasicOperations() {
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
        int initialSize = randomIntBetween(10, 1000);
        int sourceArraySize = randomIntBetween(0, initialSize);

        try (TDigestDoubleArray array = doubleArray(initialSize); TDigestDoubleArray source = doubleArray(sourceArraySize)) {
            assertThat(array.size(), equalTo(initialSize));
            assertThat(source.size(), equalTo(sourceArraySize));

            double value = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
            for (int i = 0; i < sourceArraySize; i++) {
                source.set(i, value);
            }

            int initialOffset = randomIntBetween(0, initialSize - sourceArraySize);
            int sourceOffset = randomIntBetween(0, sourceArraySize - 1);
            int elementsToCopy = randomIntBetween(1, sourceArraySize - sourceOffset);

            array.set(initialOffset, source, sourceOffset, elementsToCopy);

            for (int i = 0; i < initialSize; i++) {
                if (i < initialOffset || i >= initialOffset + elementsToCopy) {
                    assertThat(array.get(i), equalTo(0.0));
                } else {
                    assertThat(array.get(i), equalTo(value));
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
                assertThat(current, equalTo(Math.max(previous, current)));
                previous = current;
            }
        }
    }

    private TDigestIntArray intArray(int initialSize) {
        return arrays().newIntArray(initialSize);
    }

    private TDigestDoubleArray doubleArray(int initialSize) {
        return arrays().newDoubleArray(initialSize);
    }

    protected TDigestArrays arrays() {
        return WrapperTDigestArrays.INSTANCE;
    }
}
