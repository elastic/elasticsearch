/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CountDownActionListenerTests extends ESTestCase {

    public void testNotifications() throws InterruptedException {
        AtomicBoolean called = new AtomicBoolean(false);
        ActionListener<Void> result = new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                called.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
        final int groupSize = randomIntBetween(10, 1000);
        AtomicInteger count = new AtomicInteger();
        CountDownActionListener listener = new CountDownActionListener(groupSize, result);
        int numThreads = randomIntBetween(2, 5);
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                while (count.incrementAndGet() <= groupSize) {
                    listener.onResponse(null);
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(called.get());
    }

    public void testFailed() {
        AtomicBoolean called = new AtomicBoolean(false);
        AtomicReference<Exception> excRef = new AtomicReference<>();

        ActionListener<Void> result = new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                called.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                excRef.set(e);
            }
        };
        int size = randomIntBetween(3, 4);
        CountDownActionListener listener = new CountDownActionListener(size, result);
        listener.onResponse(null);
        IOException ioException = new IOException();
        RuntimeException rtException = new RuntimeException();
        listener.onFailure(rtException);
        listener.onFailure(ioException);
        if (size == 4) {
            listener.onResponse(null);
        }
        assertNotNull(excRef.get());
        assertEquals(rtException, excRef.get());
        assertEquals(1, excRef.get().getSuppressed().length);
        assertEquals(ioException, excRef.get().getSuppressed()[0]);
        assertFalse(called.get());
    }

    public void testValidation() throws InterruptedException {
        AtomicBoolean called = new AtomicBoolean(false);
        ActionListener<Void> result = new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                called.compareAndSet(false, true);
            }

            @Override
            public void onFailure(Exception e) {
                called.compareAndSet(false, true);
            }
        };

        // can't use a groupSize of 0
        expectThrows(AssertionError.class, () -> new CountDownActionListener(0, result));

        // can't use a null listener or runnable
        expectThrows(NullPointerException.class, () -> new CountDownActionListener(1, (ActionListener<Void>) null));

        final int overage = randomIntBetween(1, 10);
        AtomicInteger assertionsTriggered = new AtomicInteger();
        final int groupSize = randomIntBetween(10, 1000);
        AtomicInteger count = new AtomicInteger();
        CountDownActionListener listener = new CountDownActionListener(groupSize, result);
        int numThreads = randomIntBetween(2, 5);
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                int c;
                while ((c = count.incrementAndGet()) <= groupSize + overage) {
                    try {
                        if (c % 10 == 1) { // a mix of failures and non-failures
                            listener.onFailure(new RuntimeException());
                        } else {
                            listener.onResponse(null);
                        }
                    } catch (AssertionError e) {
                        assertionsTriggered.incrementAndGet();
                    }
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(called.get());
        assertEquals(overage, assertionsTriggered.get());
    }

    public void testConcurrentFailures() throws InterruptedException {
        AtomicReference<Exception> finalException = new AtomicReference<>();
        int numGroups = randomIntBetween(10, 100);
        CountDownActionListener listener = new CountDownActionListener(numGroups, ActionListener.wrap(r -> {}, finalException::set));
        ExecutorService executorService = Executors.newFixedThreadPool(numGroups);
        for (int i = 0; i < numGroups; i++) {
            executorService.submit(() -> listener.onFailure(new IOException()));
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        Exception exception = finalException.get();
        assertNotNull(exception);
        assertThat(exception, instanceOf(IOException.class));
        assertEquals(numGroups - 1, exception.getSuppressed().length);
    }

    /*
     * It can happen that the same exception causes a grouped listener to be notified of the failure multiple times. Since we suppress
     * additional exceptions into the first exception, we have to guard against suppressing into the same exception, which could occur if we
     * are notified of with the same failure multiple times. This test verifies that the guard against self-suppression remains.
     */
    public void testRepeatNotificationForTheSameException() {
        final AtomicReference<Exception> finalException = new AtomicReference<>();
        final CountDownActionListener listener = new CountDownActionListener(2, ActionListener.wrap(r -> {}, finalException::set));
        final Exception e = new Exception();
        // repeat notification for the same exception
        listener.onFailure(e);
        listener.onFailure(e);
        assertThat(finalException.get(), not(nullValue()));
        assertThat(finalException.get(), equalTo(e));
    }
}
