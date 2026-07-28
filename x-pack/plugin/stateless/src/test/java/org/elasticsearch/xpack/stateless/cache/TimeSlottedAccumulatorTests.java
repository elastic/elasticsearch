/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimeSlottedAccumulatorTests extends ESTestCase {

    public void testInvalidConstructorArgs() {
        TimeProvider timeProvider = fixedAbsoluteTime(randomLongBetween(0, 1_000_000));
        expectThrows(IllegalArgumentException.class, () -> new TimeSlottedAccumulator(TimeValue.ZERO, 1, 0, timeProvider));
        expectThrows(IllegalArgumentException.class, () -> new TimeSlottedAccumulator(randomGranularity(), 0, 0, timeProvider));
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(randomGranularity(), 1, randomIntBetween(-10, -1), timeProvider)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(randomGranularity(), Integer.MAX_VALUE, 1, timeProvider)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(randomGranularity(), Integer.MAX_VALUE, Integer.MAX_VALUE, timeProvider)
        );
        long overflowingGranularityMillis = Long.MAX_VALUE / 2 + 1;
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(TimeValue.timeValueMillis(overflowingGranularityMillis), 3, 0, timeProvider)
        );
        assertThat(TimeSlottedAccumulator.MAX_TOTAL_SLOTS, equalTo((int) (ByteSizeValue.ofGb(4).getBytes() / Integer.BYTES)));
        int maxTotalSlots = TimeSlottedAccumulator.MAX_TOTAL_SLOTS;
        expectThrows(IllegalArgumentException.class, () -> new TimeSlottedAccumulator(randomGranularity(), maxTotalSlots, 1, timeProvider));
        expectThrows(IllegalArgumentException.class, () -> new TimeSlottedAccumulator(randomGranularity(), 1, maxTotalSlots, timeProvider));
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(randomGranularity(), maxTotalSlots / 2 + 1, maxTotalSlots / 2 + 1, timeProvider)
        );
        TimeValue granularity = randomGranularity();
        int pastSlots = randomIntBetween(3, 6);
        TimeProvider earlyTimeProvider = fixedAbsoluteTime(randomLongBetween(-1_000_000, -1));
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), earlyTimeProvider)
        );
    }

    public void testCreateFromSettings() {
        TimeValue granularity = randomGranularity();
        int pastCount = randomIntBetween(1, 200);
        int futureCount = randomIntBetween(0, 100);
        Settings settings = Settings.builder()
            .put(TimeSlottedAccumulator.TIME_SLOTS_GRANULARITY_SETTING.getKey(), granularity)
            .put(TimeSlottedAccumulator.TIME_SLOTS_PAST_COUNT_SETTING.getKey(), pastCount)
            .put(TimeSlottedAccumulator.TIME_SLOTS_FUTURE_COUNT_SETTING.getKey(), futureCount)
            .build();
        long anchorMillis = randomAnchorSlot(granularity.millis(), pastCount);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorMillis);
        TimeSlottedAccumulator accumulator = (TimeSlottedAccumulator) TimeSlottedAccumulator.createFromSettings(settings, timeProvider);
        assertThat(accumulator.granularity(), equalTo(granularity));
        assertThat(accumulator.slots(), equalTo(pastCount + futureCount));
    }

    public void testAddInRetainedPastSlot() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(10, 500);
        int slotsBack = randomIntBetween(1, Math.min(pastSlots - 1, 100));
        long anchorMillis = randomLongBetween(granularityMillis * pastSlots, granularityMillis * pastSlots * 10);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorMillis);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        long anchorSlot = alignToSlot(anchorMillis, granularityMillis);
        long pastTimestamp = anchorSlot - (long) slotsBack * granularityMillis;
        int delta = randomNonZeroDelta();
        assertThat(accumulator.accumulate(pastTimestamp, delta), equalTo(delta));
        assertThat(accumulator.sum(pastTimestamp, pastTimestamp + granularityMillis), equalTo(delta));
    }

    public void testSlotTruncationAndClamping() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 6);
        long anchorSlot = randomLongBetween(granularityMillis * pastSlots, granularityMillis * pastSlots * 10);
        anchorSlot = alignToSlot(anchorSlot, granularityMillis);
        long offsetInSlot = randomLongBetween(1, granularityMillis - 1);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot + offsetInSlot);
        int futureSlots = randomFutureSlotCount();
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, futureSlots, timeProvider);

        int delta1 = randomNonZeroDelta();
        assertThat(accumulator.accumulate(timeProvider.absoluteTimeInMillis(), delta1), equalTo(delta1));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(delta1));

        int delta2 = randomNonZeroDelta();
        long midSlotTimestamp = anchorSlot + randomLongBetween(1, granularityMillis - 1);
        assertThat(accumulator.accumulate(midSlotTimestamp, delta2), equalTo(delta1 + delta2));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(delta1 + delta2));

        long nextSlot = anchorSlot + granularityMillis;
        int delta3 = randomNonZeroDelta();
        if (futureSlots == 0) {
            // timestamps at or beyond the next slot clamp to the head (anchor) slot
            assertThat(accumulator.accumulate(nextSlot, delta3), equalTo(delta1 + delta2 + delta3));
            assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(delta1 + delta2 + delta3));
        } else {
            assertThat(accumulator.accumulate(nextSlot, delta3), equalTo(delta3));
            assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(delta1 + delta2));
            assertThat(accumulator.sum(nextSlot, nextSlot + granularityMillis), equalTo(delta3));
        }

        long tailSlot = anchorSlot - (long) (pastSlots - 1) * granularityMillis;
        int delta4 = randomNonZeroDelta();
        assertThat(accumulator.accumulate(tailSlot, delta4), equalTo(delta4));
        assertThat(accumulator.sum(tailSlot, tailSlot + granularityMillis), equalTo(delta4));
    }

    public void testAddRemoveSymmetry() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 10);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        int futureSlots = randomPositiveFutureSlotCount();
        long clockSlot = anchorSlot + granularityMillis;
        TimeProvider timeProvider = fixedAbsoluteTime(clockSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, futureSlots, timeProvider);

        long timestamp = clockSlot + randomLongBetween(1, granularityMillis - 1);
        int delta1 = randomIntBetween(1, 100);
        int delta2 = randomIntBetween(1, 100);
        assertThat(accumulator.accumulate(timestamp, delta1), equalTo(delta1));
        assertThat(accumulator.accumulate(timestamp, delta2), equalTo(delta1 + delta2));
        assertThat(accumulator.sum(clockSlot, clockSlot + granularityMillis), equalTo(delta1 + delta2));

        int total = delta1 + delta2;
        int removeDelta = randomIntBetween(1, total);
        assertThat(accumulator.accumulate(timestamp, -removeDelta), equalTo(total - removeDelta));
        assertThat(accumulator.sum(clockSlot, clockSlot + granularityMillis), equalTo(total - removeDelta));
    }

    public void testAccumulateReturnValue() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 10);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        int firstDelta = randomIntBetween(1, 100);
        int secondDelta = randomIntBetween(1, 100);
        assertThat(accumulator.accumulate(anchorSlot, firstDelta), equalTo(firstDelta));
        assertThat(accumulator.accumulate(anchorSlot, secondDelta), equalTo(firstDelta + secondDelta));
        assertThat(accumulator.accumulate(anchorSlot, 0), equalTo(firstDelta + secondDelta));

        int overSubtract = randomIntBetween(firstDelta + secondDelta + 1, firstDelta + secondDelta + 100);
        int slotCount = accumulator.accumulate(anchorSlot, -overSubtract);
        assertThat(slotCount, lessThan(0));
        assertThat(slotCount, equalTo(firstDelta + secondDelta - overSubtract));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(slotCount));
    }

    public void testConcurrentAdds() throws Exception {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(10, 48);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);
        long timestamp = timeProvider.absoluteTimeInMillis();
        int threads = randomIntBetween(2, 8);
        int iterations = randomIntBetween(100, 1000);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            accumulator.accumulate(timestamp, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertThat(accumulator.sum(timestamp, timestamp + granularityMillis), equalTo(threads * iterations));
        } finally {
            terminate(executor);
        }
    }

    public void testConcurrentAddAndSum() throws Exception {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(24, 48);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);
        long windowStart = anchorSlot;
        int slotsInWindow = randomIntBetween(3, 6);
        long windowEnd = anchorSlot + (long) slotsInWindow * granularityMillis;
        int addThreads = randomIntBetween(2, 4);
        int sumThreads = randomIntBetween(2, 4);
        int iterations = randomIntBetween(100, 500);
        int expectedTotal = addThreads * iterations;
        AtomicLong completedAdds = new AtomicLong();
        ExecutorService executor = Executors.newFixedThreadPool(addThreads + sumThreads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(addThreads + sumThreads);

            for (int t = 0; t < addThreads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            long ts = windowStart + (long) (i % slotsInWindow) * granularityMillis;
                            completedAdds.incrementAndGet();
                            accumulator.accumulate(ts, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int t = 0; t < sumThreads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            int sum = accumulator.sum(windowStart, windowEnd);
                            int completed = (int) completedAdds.get();
                            assertThat(sum, greaterThanOrEqualTo(0));
                            assertThat(sum, lessThanOrEqualTo(completed));
                            assertThat(sum, lessThanOrEqualTo(expectedTotal));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertThat((int) completedAdds.get(), equalTo(expectedTotal));
            assertThat(accumulator.sum(windowStart, windowEnd), equalTo(expectedTotal));
        } finally {
            terminate(executor);
        }
    }

    public void testConcurrentAddRemove() throws Exception {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(24, 48);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);
        int slotsInWindow = randomIntBetween(3, 6);
        long windowStart = anchorSlot;
        long windowEnd = anchorSlot + (long) slotsInWindow * granularityMillis;
        int threads = randomIntBetween(2, 8);
        int iterations = randomIntBetween(100, 1000);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            long ts = windowStart + (long) (i % slotsInWindow) * granularityMillis;
                            accumulator.accumulate(ts, 1);
                            accumulator.accumulate(ts, -1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertThat(accumulator.sum(windowStart, windowEnd), equalTo(0));
        } finally {
            terminate(executor);
        }
    }

    public void testFutureTimestampSlotMappingAndClamping() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(2, 5);
        int futureSlots = randomPositiveFutureSlotCount();
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, futureSlots, timeProvider);

        int futureSlotOffset = randomIntBetween(1, futureSlots);
        long futureSlot = anchorSlot + (long) futureSlotOffset * granularityMillis;
        int futureDelta = randomNonZeroDelta();
        accumulator.accumulate(futureSlot, futureDelta);
        assertThat(accumulator.sum(futureSlot, futureSlot + granularityMillis), equalTo(futureDelta));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(0));

        // timestamps beyond the configured future window clamp to the head slot
        long headSlot = anchorSlot + (long) futureSlots * granularityMillis;
        long beyondHeadTimestamp = headSlot + randomLongBetween(granularityMillis * 2, granularityMillis * 20);
        int headDelta = randomNonZeroDelta();
        accumulator.accumulate(beyondHeadTimestamp, headDelta);
        int expectedHeadSum = headDelta + (futureSlot == headSlot ? futureDelta : 0);
        assertThat(accumulator.sum(headSlot, headSlot + granularityMillis), equalTo(expectedHeadSum));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(0));

        long queryStart = headSlot + randomLongBetween(granularityMillis * 2, granularityMillis * 20);
        long queryEnd = queryStart + randomLongBetween(granularityMillis, granularityMillis * 10);
        assertThat(accumulator.sum(queryStart, queryEnd), equalTo(0));
    }

    public void testPastTimestampClampingAndSumBeforeTailZero() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(10, 48);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        long tailSlot = anchorSlot - (long) (pastSlots - 1) * granularityMillis;
        int clampedDelta = randomNonZeroDelta();
        long beforeTail = tailSlot - granularityMillis;
        long timestampBeforeTail = beforeTail < 0 ? beforeTail : randomLongBetween(0, beforeTail);
        accumulator.accumulate(timestampBeforeTail, clampedDelta);
        assertThat(accumulator.sum(tailSlot, tailSlot + granularityMillis), equalTo(clampedDelta));

        int delta = randomNonZeroDelta();
        accumulator.accumulate(tailSlot + granularityMillis, delta);
        assertThat(accumulator.sum(0, tailSlot), equalTo(0));
    }

    public void testSumWithNoEventsReturnsZero() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(5, 10);
        int futureSlots = randomFutureSlotCount();
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, futureSlots, timeProvider);

        long windowStart = anchorSlot - (long) (pastSlots - 1) * granularityMillis;
        long windowEnd = anchorSlot + (long) futureSlots * granularityMillis + granularityMillis;
        assertThat(accumulator.sum(windowStart, windowEnd), equalTo(0));
    }

    public void testZeroDeltaAccumulateIsNoOp() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 10);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        int delta = randomNonZeroDelta();
        assertThat(accumulator.accumulate(anchorSlot, delta), equalTo(delta));
        assertThat(accumulator.accumulate(anchorSlot, 0), equalTo(delta));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(delta));
    }

    public void testSumWithNonPositiveRangeReturnsZero() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 10);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        int delta = randomNonZeroDelta();
        accumulator.accumulate(anchorSlot, delta);

        assertThat(accumulator.sum(anchorSlot, anchorSlot), equalTo(0));
        assertThat(accumulator.sum(anchorSlot + granularityMillis, anchorSlot), equalTo(0));
        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(delta));
    }

    public void testSumSaturatesOnOverflow() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 6);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        long previousSlot = anchorSlot - granularityMillis;
        int high = Integer.MAX_VALUE / 2 + 1;
        int low = Integer.MAX_VALUE / 2;
        accumulator.accumulate(previousSlot, high);
        accumulator.accumulate(anchorSlot, low);
        assertThat(accumulator.sum(previousSlot, anchorSlot + granularityMillis), equalTo(Integer.MAX_VALUE));
    }

    public void testSumSaturatesOnUnderflow() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 6);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        long previousSlot = anchorSlot - granularityMillis;
        int low = Integer.MIN_VALUE / 2 - 1;
        int high = Integer.MIN_VALUE / 2;
        accumulator.accumulate(previousSlot, low);
        accumulator.accumulate(anchorSlot, high);
        assertThat(accumulator.sum(previousSlot, anchorSlot + granularityMillis), equalTo(Integer.MIN_VALUE));
    }

    public void testSumAcrossMultipleSlots() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(4, 8);
        int futureSlots = randomPositiveFutureSlotCount();
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, futureSlots, timeProvider);

        long tailSlot = anchorSlot - (long) (pastSlots - 1) * granularityMillis;
        long headSlot = anchorSlot + (long) futureSlots * granularityMillis;
        int tailDelta = randomNonZeroDelta();
        int middleDelta = randomNonZeroDelta();
        int anchorDelta = randomNonZeroDelta();
        int headDelta = randomNonZeroDelta();
        accumulator.accumulate(tailSlot, tailDelta);
        accumulator.accumulate(anchorSlot - granularityMillis, middleDelta);
        accumulator.accumulate(anchorSlot, anchorDelta);
        accumulator.accumulate(headSlot, headDelta);

        long retainedEndExclusive = headSlot + granularityMillis;
        assertThat(accumulator.sum(tailSlot, retainedEndExclusive), equalTo(tailDelta + middleDelta + anchorDelta + headDelta));
    }

    public void testSumQueryRangeClampedToRetainedWindow() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(5, 10);
        int futureSlots = randomPositiveFutureSlotCount();
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, futureSlots, timeProvider);

        int delta = randomNonZeroDelta();
        accumulator.accumulate(anchorSlot, delta);

        long tailSlot = anchorSlot - (long) (pastSlots - 1) * granularityMillis;
        long headSlot = anchorSlot + (long) futureSlots * granularityMillis;
        long queryStart = tailSlot - randomLongBetween(granularityMillis, granularityMillis * 100);
        long queryEnd = headSlot + granularityMillis + randomLongBetween(granularityMillis, granularityMillis * 100);
        assertThat(accumulator.sum(queryStart, queryEnd), equalTo(delta));
    }

    public void testSumEndExclusiveAtSlotBoundary() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 6);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        long previousSlot = anchorSlot - granularityMillis;
        int previousDelta = randomNonZeroDelta();
        int anchorDelta = randomNonZeroDelta();
        accumulator.accumulate(previousSlot, previousDelta);
        accumulator.accumulate(anchorSlot, anchorDelta);

        assertThat(accumulator.sum(anchorSlot, anchorSlot + granularityMillis), equalTo(anchorDelta));
        assertThat(accumulator.sum(previousSlot, anchorSlot + granularityMillis), equalTo(previousDelta + anchorDelta));
    }

    public void testPartialSlotQueryRange() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(4, 8);
        long anchorSlot = randomAnchorSlot(granularityMillis, pastSlots);
        TimeProvider timeProvider = fixedAbsoluteTime(anchorSlot);
        TimestampAccumulator accumulator = new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider);

        long firstSlot = anchorSlot - granularityMillis;
        int firstDelta = randomNonZeroDelta();
        int secondDelta = randomNonZeroDelta();
        accumulator.accumulate(firstSlot, firstDelta);
        accumulator.accumulate(anchorSlot, secondDelta);

        long queryStart = firstSlot + randomLongBetween(1, granularityMillis - 1);
        assertThat(accumulator.sum(queryStart, anchorSlot), equalTo(firstDelta));
        assertThat(accumulator.sum(queryStart, anchorSlot + granularityMillis), equalTo(firstDelta + secondDelta));
    }

    public void testConstructorRejectsTooEarlyTime() {
        TimeValue granularity = randomGranularity();
        long granularityMillis = granularity.millis();
        int pastSlots = randomIntBetween(3, 6);
        long minAnchorSlotStartMillis = (long) (pastSlots - 1) * granularityMillis;
        TimeProvider timeProvider = fixedAbsoluteTime(minAnchorSlotStartMillis - granularityMillis);
        expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSlottedAccumulator(granularity, pastSlots, randomFutureSlotCount(), timeProvider)
        );
    }

    private static int randomFutureSlotCount() {
        return randomIntBetween(0, 24);
    }

    private static int randomPositiveFutureSlotCount() {
        return randomIntBetween(1, 24);
    }

    private static int randomNonZeroDelta() {
        int abs = randomIntBetween(1, 1000);
        return randomBoolean() ? abs : -abs;
    }

    private static TimeValue randomGranularity() {
        return randomFrom(
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueMinutes(5),
            TimeValue.timeValueMinutes(15),
            TimeValue.timeValueMinutes(30),
            TimeValue.timeValueHours(1),
            TimeValue.timeValueHours(6),
            TimeValue.timeValueHours(12),
            TimeValue.timeValueDays(1),
            TimeValue.timeValueDays(7)
        );
    }

    private static long randomAnchorSlot(long granularityMillis, int pastSlots) {
        return alignToSlot(randomLongBetween(granularityMillis * pastSlots, granularityMillis * pastSlots * 10), granularityMillis);
    }

    private static long alignToSlot(long timestampMillis, long granularityMillis) {
        return Math.floorDiv(timestampMillis, granularityMillis) * granularityMillis;
    }

    private static TimeProvider fixedAbsoluteTime(long absoluteTimeMillis) {
        return new TimeProvider() {
            @Override
            public long relativeTimeInMillis() {
                return absoluteTimeMillis;
            }

            @Override
            public long relativeTimeInNanos() {
                return TimeUnit.MILLISECONDS.toNanos(absoluteTimeMillis);
            }

            @Override
            public long rawRelativeTimeInMillis() {
                return absoluteTimeMillis;
            }

            @Override
            public long absoluteTimeInMillis() {
                return absoluteTimeMillis;
            }
        };
    }
}
