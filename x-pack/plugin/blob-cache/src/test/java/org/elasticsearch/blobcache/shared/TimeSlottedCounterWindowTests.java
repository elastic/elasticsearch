/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class TimeSlottedCounterWindowTests extends ESTestCase {

    private static final long HOUR = TimeValue.timeValueHours(1).millis();
    private static final long DAY = TimeValue.timeValueDays(1).millis();

    public void testMatchesUncachedLast24Hours() {
        AtomicLong clock = new AtomicLong(10 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, 24 * HOUR);

        for (int i = 0; i < 48; i++) {
            counter.add(10 * DAY - i * HOUR, i + 1);
        }

        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
    }

    public void testMatchesUncachedRelativeWindow() {
        AtomicLong clock = new AtomicLong(30 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 1000, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(DAY, 3 * DAY);

        counter.add(clock.get() - DAY, 50);
        counter.add(clock.get() - 2 * DAY, 25);

        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
    }

    public void testFastPathWithinGranularity() {
        AtomicLong clock = new AtomicLong(5 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 200, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, DAY);

        counter.add(4 * DAY + HOUR, 10);
        sliding.sum();

        clock.addAndGet(TimeValue.timeValueMinutes(30).millis());
        long second = sliding.sum();
        assertThat(second, equalTo(sliding.sumUncached()));
        assertThat(second, equalTo(10L));
    }

    public void testIncrementalRelativeWindow() {
        AtomicLong clock = new AtomicLong(10 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 48, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(DAY, 3 * DAY);

        counter.add(10 * DAY - 2 * DAY, 7);
        long first = sliding.sum();

        clock.addAndGet(2 * HOUR);
        long newEntryTime = clock.get() - DAY - HOUR;
        counter.add(newEntryTime, 3);

        long second = sliding.sum();
        assertThat(second, equalTo(sliding.sumUncached()));
        assertThat(second, equalTo(first + 3L));
    }

    public void testIncrementalUpdateOnAdd() {
        AtomicLong clock = new AtomicLong(5 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 200, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, DAY);

        counter.add(4 * DAY + HOUR, 10);
        long first = sliding.sum();
        assertThat(first, equalTo(10L));

        counter.add(4 * DAY + 2 * HOUR, 5);
        long second = sliding.sum();
        assertThat(second, equalTo(sliding.sumUncached()));
        assertThat(second, equalTo(15L));
    }

    public void testManyMutationsWithoutRescan() {
        AtomicLong clock = new AtomicLong(10 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, 7 * DAY);

        sliding.sum();
        for (int i = 0; i < 1000; i++) {
            counter.add(clock.get() - DAY - (i % 120) * HOUR, 1);
        }
        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
    }

    public void testTwoWindowsOnOneCounter() {
        AtomicLong clock = new AtomicLong(400 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 10000, 0, clock::get);
        TimeSlottedCounterWindow recent = counter.registerRelativeWindow(0, 7 * DAY);
        TimeSlottedCounterWindow prior = counter.registerRelativeWindow(7 * DAY, 365 * DAY);

        counter.add(clock.get() - 2 * DAY, 10);
        counter.add(clock.get() - 30 * DAY, 20);
        counter.add(clock.get() - 400 * DAY, 100);

        assertThat(recent.sum(), equalTo(10L));
        assertThat(recent.sum(), equalTo(recent.sumUncached()));
        assertThat(prior.sum(), equalTo(20L));
        assertThat(prior.sum(), equalTo(prior.sumUncached()));
    }

    public void testTimeOnlySlide() {
        AtomicLong clock = new AtomicLong(10 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, 3 * DAY);

        counter.add(10 * DAY - DAY, 42);
        sliding.sum();

        clock.addAndGet(2 * DAY);
        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
    }

    public void testWindowConsistentAfterHeadExceeded() {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, 1, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, 2 * HOUR);

        counter.add(9 * HOUR, 5);
        clock.set(15 * HOUR);
        counter.add(clock.get(), 3);

        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
    }

    public void testConcurrentAddAndSlidingSum() throws Exception {
        AtomicLong clock = new AtomicLong(20 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(DAY, 7 * DAY);

        int addThreads = 2;
        int sumThreads = 2;
        int iterations = 200;
        ExecutorService executor = Executors.newFixedThreadPool(addThreads + sumThreads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(addThreads + sumThreads);

            for (int t = 0; t < addThreads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            counter.add(clock.get() - DAY + i, 1);
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
                            sliding.sum();
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

            assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
        } finally {
            terminate(executor);
        }
    }

    public void testConcurrentSumCalls() throws Exception {
        AtomicLong clock = new AtomicLong(20 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(DAY, 7 * DAY);
        counter.add(clock.get() - DAY, 100);

        int threads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<Long> results = Collections.synchronizedList(new ArrayList<>());

            for (int t = 0; t < threads; t++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < 200; i++) {
                            results.add(sliding.sum());
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

            long expected = sliding.sumUncached();
            for (Long result : results) {
                assertThat(result, equalTo(expected));
            }
        } finally {
            terminate(executor);
        }
    }

    public void testWindowConsistentWhenWallClockExceedsHead() {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, 1, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, 2 * HOUR);

        counter.add(9 * HOUR, 5);
        counter.add(11 * HOUR, 2);
        sliding.sum();

        clock.set(100 * HOUR);
        counter.add(clock.get(), 7);
        counter.add(9 * HOUR, 1);

        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
        assertThat(sliding.sum(), equalTo(6L));
    }

    public void testMutateAfterTimeAdvanceWithoutPriorSum() {
        AtomicLong clock = new AtomicLong(10 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 500, 0, clock::get);
        TimeSlottedCounterWindow sliding = counter.registerRelativeWindow(0, DAY);

        long slotLeavingWindow = 10 * DAY - 12 * HOUR;
        counter.add(slotLeavingWindow, 10);
        sliding.sum();

        clock.addAndGet(DAY);
        counter.add(slotLeavingWindow, 5);
        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));

        long slotEnteringWindow = clock.get() - 12 * HOUR;
        counter.add(slotEnteringWindow, 7);
        assertThat(sliding.sum(), equalTo(sliding.sumUncached()));
    }

    public void testInvalidRegistrationOffsets() {
        AtomicLong clock = new AtomicLong(0);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 10, 0, clock::get);
        expectThrows(IllegalArgumentException.class, () -> counter.registerRelativeWindow(DAY, DAY));
        expectThrows(IllegalArgumentException.class, () -> counter.registerRelativeWindow(2 * DAY, DAY));
        expectThrows(IllegalArgumentException.class, () -> counter.registerRelativeWindow(-HOUR, DAY));
        expectThrows(IllegalArgumentException.class, () -> counter.registerRelativeWindow(0, -DAY));
    }

    public void testDegradationAfterHeadExceeded() {
        AtomicLong clock = new AtomicLong(10 * HOUR);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 3, 1, clock::get);
        TimeSlottedCounterWindow window = counter.registerRelativeWindow(0, 2 * HOUR);

        long headSlot = 11 * HOUR;
        counter.add(9 * HOUR, 5);
        counter.add(headSlot, 2);

        clock.set(15 * HOUR);
        counter.add(clock.get(), 7);

        assertThat(counter.sum(headSlot, headSlot + HOUR), equalTo(9L));
        assertThat(window.sum(), equalTo(window.sumUncached()));
    }

    public void testRegisteredWindowsStayConsistent() {
        AtomicLong clock = new AtomicLong(30 * DAY);
        TimeSlottedCounter counter = new TimeSlottedCounter(TimeValue.timeValueHours(1), 1000, 0, clock::get);
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
