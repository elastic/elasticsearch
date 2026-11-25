/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class FilteredLimitReaderRampTests extends ESTestCase {

    public void testK1DoesNotDoubleWhenRemainingAlreadyZero() {
        FilteredLimitReaderRamp<String> ramp = new FilteredLimitReaderRamp<>(() -> true, item -> false);
        ramp.setProgressPages(1);
        assertThat(ramp.submit("running", false), equalTo(FilteredLimitReaderRamp.Submit.START));
        assertThat(ramp.submit("parked", false), equalTo(FilteredLimitReaderRamp.Submit.PARK));
        FilteredLimitReaderRamp.Drain<String> drain = ramp.onProcessedPage();
        assertThat(drain.toStart(), equalTo(List.of()));
        assertThat(drain.toSkip(), equalTo(List.of("parked")));
        assertThat(ramp.cap(), equalTo(1));
        assertThat(ramp.parkedCount(), equalTo(0));
    }

    public void testEofDoubleResetsPageCounter() {
        FilteredLimitReaderRamp<String> ramp = new FilteredLimitReaderRamp<>(() -> false, item -> false);
        ramp.setProgressPages(2);
        assertThat(ramp.submit("running", false), equalTo(FilteredLimitReaderRamp.Submit.START));
        assertThat(ramp.submit("p1", false), equalTo(FilteredLimitReaderRamp.Submit.PARK));
        assertThat(ramp.submit("p2", false), equalTo(FilteredLimitReaderRamp.Submit.PARK));
        assertThat(ramp.submit("p3", false), equalTo(FilteredLimitReaderRamp.Submit.PARK));
        assertTrue(ramp.onProcessedPage().isEmpty());
        FilteredLimitReaderRamp.Drain<String> afterEof = ramp.doubleCap();
        assertThat(afterEof.toStart(), equalTo(List.of("p1")));
        assertTrue(ramp.onProcessedPage().isEmpty());
        FilteredLimitReaderRamp.Drain<String> afterNextInterval = ramp.onProcessedPage();
        assertThat(afterNextInterval.toStart(), equalTo(List.of("p2", "p3")));
    }

    public void testTwoThreadsDoubleOncePerInterval() throws Exception {
        FilteredLimitReaderRamp<String> ramp = new FilteredLimitReaderRamp<>(() -> false, item -> false);
        ramp.setProgressPages(2);
        assertThat(ramp.submit("running", false), equalTo(FilteredLimitReaderRamp.Submit.START));
        for (int i = 0; i < 6; i++) {
            assertThat(ramp.submit("p" + i, false), equalTo(FilteredLimitReaderRamp.Submit.PARK));
        }
        FilteredLimitReaderRamp.Drain<String> first = ramp.doubleCap();
        assertThat(first.toStart().size(), equalTo(1));
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicInteger started = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    try {
                        barrier.await(30, TimeUnit.SECONDS);
                        FilteredLimitReaderRamp.Drain<String> drain = ramp.onProcessedPage();
                        started.addAndGet(drain.toStart().size());
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertThat(started.get(), equalTo(2));
    }
}
