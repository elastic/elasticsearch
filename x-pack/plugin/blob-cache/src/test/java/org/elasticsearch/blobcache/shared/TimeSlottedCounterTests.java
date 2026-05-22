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

    public void testFutureTimestampsClampToHeadWithoutRoll() {
        AtomicLong clock = new AtomicLong(0);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, clock::get);

        counter.add(0, 4);
        clock.set(5 * HOUR);
        counter.add(clock.get(), 6);
        // Head slot is still hour 0; future adds accumulate in that bucket
        assertThat(counter.sum(0, HOUR), equalTo(10L));
        assertThat(counter.sum(5 * HOUR, 6 * HOUR), equalTo(0L));
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
}
