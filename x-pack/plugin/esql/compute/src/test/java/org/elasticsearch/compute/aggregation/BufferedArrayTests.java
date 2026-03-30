/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.DoubleBuffer;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.IntBuffer;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.LongBuffer;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class BufferedArrayTests extends ComputeTestCase {

    public void testLongBuffer() {
        int count = randomIntBetween(1, PAGE_SIZE * 3);
        try (LongBuffer buf = new LongBuffer(blockFactory().breaker(), between(1, 1024))) {
            buf.ensureCapacity(count);
            assertThat(buf.capacity, greaterThanOrEqualTo((long) count));
            long[] expected = new long[count];
            for (int i = 0; i < count; i++) {
                expected[i] = randomLong();
                buf.set(i, expected[i]);
            }
            for (int i = 0; i < count; i++) {
                assertThat(buf.get(i), equalTo(expected[i]));
            }
        }
    }

    public void testDoubleBuffer() {
        int count = randomIntBetween(1, PAGE_SIZE * 3);
        try (DoubleBuffer buf = new DoubleBuffer(blockFactory().breaker(), between(1, 1024))) {
            buf.ensureCapacity(count);
            double[] expected = new double[count];
            for (int i = 0; i < count; i++) {
                expected[i] = randomDouble();
                buf.set(i, expected[i]);
            }
            for (int i = 0; i < count; i++) {
                assertThat(buf.get(i), equalTo(expected[i]));
            }
        }
    }

    public void testIntBuffer() {
        int count = randomIntBetween(1, PAGE_SIZE * 3);
        try (IntBuffer buf = new IntBuffer(blockFactory().breaker(), between(1, 1024))) {
            buf.ensureCapacity(count);
            int[] expected = new int[count];
            for (int i = 0; i < count; i++) {
                expected[i] = randomInt();
                buf.set(i, expected[i]);
            }
            for (int i = 0; i < count; i++) {
                assertThat(buf.get(i), equalTo(expected[i]));
            }
        }
    }
}
