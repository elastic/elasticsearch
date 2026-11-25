/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.DoubleBuffer;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.ExponentialHistogramBuffer;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.IntBuffer;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.LongBuffer;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.ArrayList;
import java.util.List;

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
                buf.append(expected[i]);
            }
            assertThat(buf.size(), equalTo(count));
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
                buf.append(expected[i]);
            }
            assertThat(buf.size(), equalTo(count));
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
                buf.append(expected[i]);
            }
            assertThat(buf.size(), equalTo(count));
            for (int i = 0; i < count; i++) {
                assertThat(buf.get(i), equalTo(expected[i]));
            }
        }
    }

    public void testExponentialHistogramBuffer() {
        try (ExponentialHistogramBuffer buf = new ExponentialHistogramBuffer(blockFactory(), between(1, 1024))) {
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    buf.clear();
                }
                int count = randomIntBetween(1, PAGE_SIZE * 3);
                ExponentialHistogram[] expected = new ExponentialHistogram[count];
                for (int i = 0; i < count; i++) {
                    expected[i] = BlockTestUtils.randomExponentialHistogram();
                    buf.append(expected[i]);
                }
                assertThat(buf.size(), equalTo(count));
                ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
                for (int i = 0; i < count; i++) {
                    assertThat(buf.get(i, scratch), equalTo(expected[i]));
                }
            }
        }
    }

    public void testExponentialHistogramBufferAppendRangeWithNulls() {
        int count = randomIntBetween(1, PAGE_SIZE * 3);
        List<ExponentialHistogram> expected = new ArrayList<>();
        ExponentialHistogramBlock.Builder builder = blockFactory().newExponentialHistogramBlockBuilder(count);
        try {
            int skippedValueCount = randomIntBetween(0, PAGE_SIZE * 3);
            for (int i = 0; i < skippedValueCount; i++) {
                if (randomBoolean()) {
                    builder.appendNull();
                } else if (randomBoolean()) {
                    builder.beginPositionEntry();
                    builder.append(BlockTestUtils.randomExponentialHistogram());
                    builder.append(BlockTestUtils.randomExponentialHistogram());
                    builder.endPositionEntry();
                } else {
                    builder.append(BlockTestUtils.randomExponentialHistogram());
                }
            }
            for (int i = 0; i < count; i++) {
                ExponentialHistogram histo = BlockTestUtils.randomExponentialHistogram();
                builder.append(histo);
                expected.add(histo);
            }
            ExponentialHistogramBlock block = builder.build();
            try (ExponentialHistogramBuffer buf = new ExponentialHistogramBuffer(blockFactory(), between(1, 1024))) {
                buf.appendRange(block, skippedValueCount, count);
                assertThat(buf.size(), equalTo(expected.size()));
                ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
                for (int i = 0; i < expected.size(); i++) {
                    assertThat(buf.get(i, scratch), equalTo(expected.get(i)));
                }
            } finally {
                Releasables.close(block);
            }
        } finally {
            Releasables.close(builder);
        }
    }
}
