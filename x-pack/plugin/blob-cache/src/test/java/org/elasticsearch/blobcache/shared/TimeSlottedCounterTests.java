/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class TimeSlottedCounterTests extends ESTestCase {

    private static final long HOUR = TimeValue.timeValueHours(1).millis();
    private static final long DAY = TimeValue.timeValueDays(1).millis();

    public void testAddInRetainedPastSlot() {
        AtomicLong clock = new AtomicLong(10 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, clock::get);
        counter.add(8 * DAY, 7);
        assertThat(counter.sum(7 * DAY, 9 * DAY), equalTo(7L));
    }

    public void testCreateFromSettings() {
        Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_GRANULARITY_SETTING.getKey(), "30m")
            .put(SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_COUNT_SETTING.getKey(), 48)
            .build();
        AtomicLong clock = new AtomicLong(0);
        TimeSlottedCounter counter = TimeSlottedCounter.createFromSettings(settings, clock::get);
        assertThat(counter.granularity(), equalTo(TimeValue.timeValueMinutes(30)));
        assertThat(counter.maxBuckets(), equalTo(48));
    }

    public void testSlotTruncationAndClamping() {
        AtomicLong clock = new AtomicLong(5 * HOUR + 30 * 60 * 1000);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, clock::get);

        long headSlot = 5 * HOUR;
        counter.add(clock.get(), 10);
        assertThat(counter.sum(headSlot, headSlot + HOUR), equalTo(10L));

        counter.add(headSlot + 15 * 60 * 1000, 5);
        assertThat(counter.sum(headSlot, headSlot + HOUR), equalTo(15L));

        counter.add(headSlot + HOUR, 100);
        assertThat(counter.sum(headSlot, headSlot + HOUR), equalTo(115L));

        long tailSlot = headSlot - 2 * HOUR;
        counter.add(tailSlot, 7);
        assertThat(counter.sum(tailSlot, tailSlot + HOUR), equalTo(7L));
    }

    public void testRolloverAdvancesHead() {
        AtomicLong clock = new AtomicLong(0);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 2, clock::get);

        counter.add(0, 1);
        assertThat(counter.sum(0, HOUR), equalTo(1L));

        clock.set(2 * HOUR);
        counter.advanceToNow();
        counter.add(2 * HOUR, 2);
        assertThat(counter.sum(2 * HOUR, 3 * HOUR), equalTo(2L));
        assertThat(counter.sum(0, HOUR), equalTo(0L));
    }

    public void testTailMergeRetainsOverflowMass() {
        AtomicLong clock = new AtomicLong(0);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 2, clock::get);

        counter.add(0, 5);
        clock.set(3 * HOUR);
        counter.advanceToNow();
        counter.add(0, 3);
        assertThat(counter.sum(2 * HOUR, 3 * HOUR), equalTo(8L));
    }

    public void testAddRemoveSymmetry() {
        AtomicLong clock = new AtomicLong(HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 5, clock::get);

        counter.add(HOUR + 100, 20);
        counter.add(HOUR + 100, 10);
        assertThat(counter.sum(HOUR, 2 * HOUR), equalTo(30L));

        counter.remove(HOUR + 100, 25);
        assertThat(counter.sum(HOUR, 2 * HOUR), equalTo(5L));

        counter.remove(HOUR + 100, 100);
        assertThat(counter.sum(HOUR, 2 * HOUR), equalTo(0L));
    }

    public void testSumTriggeredRolloverWhenIdle() {
        AtomicLong clock = new AtomicLong(0);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, clock::get);
        counter.add(0, 4);

        clock.set(2 * HOUR);
        assertThat(counter.sum(2 * HOUR, 3 * HOUR), equalTo(0L));
        counter.add(2 * HOUR, 1);
        assertThat(counter.sum(2 * HOUR, 3 * HOUR), equalTo(1L));
    }

    public void testConcurrentAdds() throws Exception {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 24, clock::get);
        long ts = clock.get();
        int threads = 4;
        int iterations = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            counter.add(ts, 1);
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
            assertThat(counter.sum(ts, ts + HOUR), equalTo((long) threads * iterations));
        } finally {
            terminate(executor);
        }
    }

    public void testConcurrentAddAndSum() throws Exception {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 24, clock::get);
        long windowStart = 10 * HOUR;
        long windowEnd = 15 * HOUR;
        int addThreads = 2;
        int sumThreads = 2;
        int iterations = 300;
        ExecutorService executor = Executors.newFixedThreadPool(addThreads + sumThreads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(addThreads + sumThreads);

            for (int t = 0; t < addThreads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            long ts = windowStart + (i % 5) * HOUR;
                            counter.add(ts, 1);
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
                            long sum = counter.sum(windowStart, windowEnd);
                            assertThat(sum, org.hamcrest.Matchers.greaterThanOrEqualTo(0L));
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
            assertThat(counter.sum(windowStart, windowEnd), equalTo((long) addThreads * iterations));
        } finally {
            terminate(executor);
        }
    }

    public void testConcurrentAddRemove() throws Exception {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 24, clock::get);
        int threads = 4;
        int iterations = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            long ts = 10 * HOUR + (i % 5) * HOUR;
                            counter.add(ts, 1);
                            counter.remove(ts, 1);
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
            assertThat(counter.sum(10 * HOUR, 15 * HOUR), equalTo(0L));
        } finally {
            terminate(executor);
        }
    }

    public void testInvalidConstructorArgs() {
        AtomicLong clock = new AtomicLong(0);
        expectThrows(IllegalArgumentException.class, () -> new TimeSlottedCounter(TimeValue.ZERO, 1, clock::get));
        expectThrows(IllegalArgumentException.class, () -> new TimeSlottedCounter(TimeValue.timeValueHours(1), 0, clock::get));
    }

    /**
     * When wall clock crosses a slot boundary, add must roll the ring (not clamp to the previous head).
     */
    public void testAddAtSlotBoundaryAdvancesRing() {
        long headSlot = 11 * HOUR;
        AtomicLong clock = new AtomicLong(headSlot + 59 * 60 * 1000);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, clock::get);
        counter.add(headSlot, 1);

        clock.set(headSlot + HOUR + 5 * 60 * 1000);
        counter.add(clock.get(), 2);

        assertThat(counter.sum(headSlot + HOUR, headSlot + 2 * HOUR), equalTo(2L));
        assertThat(counter.sum(headSlot, headSlot + HOUR), equalTo(1L));
    }

    public void testManySequentialRolls() {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 5, clock::get);

        long tailSlot = 6 * HOUR;
        long headSlot = 10 * HOUR;
        counter.add(tailSlot, 7);
        counter.add(headSlot, 11);

        clock.set(110 * HOUR);
        counter.advanceToNow();

        assertThat(counter.sum(headSlot, headSlot + HOUR), equalTo(0L));
        assertThat(counter.sum(109 * HOUR, 110 * HOUR), equalTo(0L));
        assertThat(counter.sum(106 * HOUR, 110 * HOUR), equalTo(18L));

        counter.add(0, 3);
        assertThat(counter.sum(106 * HOUR, 110 * HOUR), equalTo(21L));
    }

    public void testRegisteredWindowsStayConsistent() {
        AtomicLong clock = new AtomicLong(30 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 1000, clock::get);
        TimeSlottedCounterWindow recent = counter.registerRelativeWindow(0, 7 * DAY);
        TimeSlottedCounterWindow prior = counter.registerRelativeWindow(7 * DAY, 30 * DAY);

        counter.add(clock.get() - 2 * DAY, 11);
        counter.add(clock.get() - 10 * DAY, 22);
        counter.remove(clock.get() - 2 * DAY, 3);

        assertThat(recent.sum(), equalTo(recent.sumUncached()));
        assertThat(prior.sum(), equalTo(prior.sumUncached()));
        assertThat(recent.sum(), equalTo(8L));
        assertThat(prior.sum(), equalTo(22L));
    }
}
