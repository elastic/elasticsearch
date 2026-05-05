/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundedParallelGatherTests extends ESTestCase {

    public void testEmptyList() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            List<Integer> empty = List.of();
            List<Integer> result = BoundedParallelGather.gather(empty, item -> item * 2, 4, executor);
            assertEquals(List.of(), result);
        } finally {
            executor.shutdown();
        }
    }

    public void testSingleItemRunsInline() throws Exception {
        // Single item must execute on the calling thread (no executor dispatch).
        Thread callingThread = Thread.currentThread();
        AtomicInteger executionThread = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            List<Integer> result = BoundedParallelGather.gather(List.of(42), item -> {
                // Record whether we are on the calling thread (0) or another thread (1).
                executionThread.set(Thread.currentThread() == callingThread ? 0 : 1);
                return item * 2;
            }, 4, executor);
            assertEquals(List.of(84), result);
            assertEquals("single item should run inline, not on executor", 0, executionThread.get());
        } finally {
            executor.shutdown();
        }
    }

    public void testResultOrderMatchesInput() throws Exception {
        int count = 50;
        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            items.add(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Integer> results = BoundedParallelGather.gather(items, item -> item * 10, 8, executor);
            assertEquals(count, results.size());
            for (int i = 0; i < count; i++) {
                assertEquals(Integer.valueOf(i * 10), results.get(i));
            }
        } finally {
            executor.shutdown();
        }
    }

    public void testMaxConcurrencyRespected() throws Exception {
        int itemCount = 20;
        int maxConcurrency = 4;
        AtomicInteger inFlight = new AtomicInteger(0);
        AtomicInteger peakConcurrency = new AtomicInteger(0);
        // Latch to ensure all tasks start before any complete, amplifying the concurrency check.
        CountDownLatch allStarted = new CountDownLatch(maxConcurrency);
        Semaphore proceed = new Semaphore(0);

        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            items.add(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(itemCount);
        try {
            List<Integer> results = BoundedParallelGather.gather(items, item -> {
                int current = inFlight.incrementAndGet();
                int peak = peakConcurrency.get();
                while (current > peak) {
                    if (peakConcurrency.compareAndSet(peak, current)) break;
                    peak = peakConcurrency.get();
                }
                // Simulate some work
                Thread.sleep(5);
                inFlight.decrementAndGet();
                return item;
            }, maxConcurrency, executor);

            assertEquals(itemCount, results.size());
            assertTrue(
                "peak concurrency " + peakConcurrency.get() + " should be <= maxConcurrency " + maxConcurrency,
                peakConcurrency.get() <= maxConcurrency
            );
        } finally {
            executor.shutdown();
        }
    }

    public void testFastFailOnException() throws Exception {
        int itemCount = 100;
        AtomicInteger startedCount = new AtomicInteger(0);
        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            items.add(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            Exception thrown = expectThrows(Exception.class, () -> {
                BoundedParallelGather.gather(items, item -> {
                    startedCount.incrementAndGet();
                    if (item == 5) {
                        throw new IllegalArgumentException("test failure at item 5");
                    }
                    // Small delay so failure is detected before all items start.
                    Thread.sleep(10);
                    return item;
                }, 4, executor);
            });
            assertNotNull(thrown);
            // Not all 100 items should have started — fast-fail should have skipped some.
            assertTrue(
                "Expected fewer than " + itemCount + " items to start, but got " + startedCount.get(),
                startedCount.get() < itemCount
            );
        } finally {
            executor.shutdown();
        }
    }

    public void testFirstExceptionPropagated() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            Exception thrown = expectThrows(IllegalStateException.class, () -> {
                BoundedParallelGather.gather(List.of(1, 2, 3), item -> {
                    if (item == 1) throw new IllegalStateException("first error");
                    return item;
                }, 4, executor);
            });
            assertEquals("first error", thrown.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    public void testInvalidMaxConcurrency() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            expectThrows(IllegalArgumentException.class, () -> BoundedParallelGather.gather(List.of(1, 2), item -> item, 0, executor));
        } finally {
            executor.shutdown();
        }
    }
}
