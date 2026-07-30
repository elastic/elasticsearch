/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.FlushQueue;
import org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.FlushQueues;
import org.elasticsearch.compute.aggregation.RateLongGroupingAggregatorFunction.LongRawBuffer;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlushQueueTests extends ComputeTestCase {

    public void testEmptyGroupReturnsNull() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 1, 2 }, new long[] { 0, 0 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 0, 1, 1 }).getFlushQueue(0);
            assertThat(queue, Matchers.nullValue());
        }
    }

    public void testOneSlice() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 30, 20, 10 }, new long[] { 1, 2, 3 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 3 }).getFlushQueue(0);
            assertThat(queue.size(), Matchers.equalTo(1));
            assertThat(queue.valueCount, Matchers.equalTo(3));
            assertThat(queue.topStart(), Matchers.equalTo(0));
            assertThat(queue.topEnd(), Matchers.equalTo(3));
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(30L));
            assertThat(queue.topLastTimestamp(), Matchers.equalTo(10L));

            assertThat(queue.consumeTop(), Matchers.equalTo(0));
            assertThat(queue.topStart(), Matchers.equalTo(1));
            assertFalse(queue.topExhausted());
            queue.updateTop();
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(20L));
        }
    }

    public void testTwoSlicesAlreadyOrdered() {
        // slice `A`: 50,40 ; slice B: 30,20 - `A` should stay root
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 50, 40, 30, 20 }, new long[] { 1, 2, 3, 4 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 2, 2, 4 }).getFlushQueue(0);
            assertThat(queue.size(), Matchers.equalTo(2));
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(50L));
            assertThat(queue.secondNextTimestamp(), Matchers.equalTo(30L));
        }
    }

    public void testTwoSlicesRequireSwap() {
        // first pair older, second newer - heapify must swap
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 10, 5, 40, 30 }, new long[] { 1, 2, 3, 4 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 2, 2, 4 }).getFlushQueue(0);
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(40L));
            assertThat(queue.topStart(), Matchers.equalTo(2));
        }
    }

    public void testThreeOrMoreSlices() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 10, 9, 50, 40, 30, 20 }, new long[] { 1, 2, 3, 4, 5, 6 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 2, 2, 4, 4, 6 }).getFlushQueue(0);
            assertThat(queue.size(), Matchers.equalTo(3));
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(50L));
            assertThat(queue.secondNextTimestamp(), Matchers.equalTo(Math.max(10L, 30L)));
        }
    }

    public void testRootAdvanceWithoutExhaustion() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 50, 40, 30, 20 }, new long[] { 1, 2, 3, 4 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 2, 2, 4 }).getFlushQueue(0);
            assertThat(queue.consumeTop(), Matchers.equalTo(0));
            assertFalse(queue.topExhausted());
            queue.updateTop();
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(40L));
        }
    }

    public void testRootExhaustionAndReplacement() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 50, 30, 20 }, new long[] { 1, 2, 3 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 1, 1, 3 }).getFlushQueue(0);
            assertThat(queue.consumeTop(), Matchers.equalTo(0));
            assertTrue(queue.topExhausted());
            queue.popTop();
            assertThat(queue.size(), Matchers.equalTo(1));
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(30L));
        }
    }

    public void testRepeatedRootRemovals() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 90, 80, 70, 60, 50, 40 }, new long[] { 1, 2, 3, 4, 5, 6 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 2, 2, 4, 4, 6 }).getFlushQueue(0);
            List<Long> order = new ArrayList<>();
            while (queue.size() > 0) {
                order.add(buf.timestamps.get(queue.consumeTop()));
                if (queue.topExhausted()) {
                    queue.popTop();
                } else {
                    queue.updateTop();
                }
            }
            assertThat(order, Matchers.equalTo(List.of(90L, 80L, 70L, 60L, 50L, 40L)));
        }
    }

    public void testEqualTimestampsPreferSmallerStart() {
        // both slices start at ts=100; smaller start index should win
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 100, 50, 100, 40 }, new long[] { 1, 2, 3, 4 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 2, 4, 0, 2 }).getFlushQueue(0);
            assertThat(queue.topStart(), Matchers.equalTo(0));
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(100L));
        }
    }

    public void testEmptyRangesExcluded() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 30, 20 }, new long[] { 1, 2 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 0, 0, 2, 2, 2 }).getFlushQueue(0);
            assertThat(queue.size(), Matchers.equalTo(1));
            assertThat(queue.valueCount, Matchers.equalTo(2));
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(30L));
        }
    }

    public void testLengthOneSlices() {
        try (LongRawBuffer buf = newLongRawBuffer(new long[] { 10, 30, 20 }, new long[] { 1, 2, 3 })) {
            FlushQueue queue = flushQueues(buf, new int[] { 0, 1, 1, 2, 2, 3 }).getFlushQueue(0);
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(30L));
            assertThat(queue.consumeTop(), Matchers.equalTo(1));
            assertTrue(queue.topExhausted());
            queue.popTop();
            assertThat(queue.topNextTimestamp(), Matchers.equalTo(20L));
        }
    }

    private static FlushQueues flushQueues(LongRawBuffer buffer, int[] sliceOffsets) {
        int numSlices = sliceOffsets.length / 2;
        // runningOffsets holds exclusive end indices per group (see RawBuffer.prepareForFlush)
        return new FlushQueues(buffer, 0, 0, new int[] { numSlices }, Arrays.copyOf(sliceOffsets, sliceOffsets.length));
    }

    private LongRawBuffer newLongRawBuffer(long[] timestamps, long[] values) {
        return newLongRawBuffer(blockFactory().breaker(), timestamps, values);
    }

    private static LongRawBuffer newLongRawBuffer(CircuitBreaker breaker, long[] timestamps, long[] values) {
        assert timestamps.length == values.length;
        LongRawBuffer buffer = new LongRawBuffer(breaker);
        if (timestamps.length == 0) {
            return buffer;
        }
        buffer.prepareForAppend(0, timestamps.length, timestamps[0]);
        for (int i = 0; i < timestamps.length; i++) {
            buffer.appendWithoutResize(timestamps[i], values[i]);
        }
        return buffer;
    }
}
