/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrencyLimiterTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(5);
        assertEquals(5, limiter.availablePermits());
        limiter.acquire();
        assertEquals(4, limiter.availablePermits());
        limiter.release();
        assertEquals(5, limiter.availablePermits());
    }

    public void testWideFanOutAllCompletesWhenDemandExceedsPermits() throws Exception {
        // Core regression: a fan-out wider than the permit pool must all COMPLETE (queue and wait), never
        // be dropped or failed. Each acquirer holds exactly one permit and releases before the next is served, so
        // blocking is deadlock-free even when demand (32) far exceeds supply (4). This is the property that lets
        // the old fail-on-timeout go away.
        int permits = 4;
        int waiters = 32;
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(permits);
        AtomicInteger completed = new AtomicInteger(0);
        AtomicInteger peakInFlight = new AtomicInteger(0);
        AtomicInteger inFlight = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(waiters);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            Thread t = new Thread(() -> {
                try {
                    limiter.acquire();
                    try {
                        int now = inFlight.incrementAndGet();
                        peakInFlight.accumulateAndGet(now, Math::max);
                        Thread.sleep(2); // hold the permit briefly so contention is real
                        completed.incrementAndGet();
                    } finally {
                        inFlight.decrementAndGet();
                        limiter.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            threads.add(t);
            t.start();
        }
        assertTrue("all waiters must finish (none dropped or failed)", done.await(30, TimeUnit.SECONDS));
        for (Thread t : threads) {
            t.join(5000);
        }
        assertEquals("every waiter must have completed its work", waiters, completed.get());
        assertTrue("the guardrail must cap concurrency at its permit count", peakInFlight.get() <= permits);
        assertEquals("all permits released at the end", permits, limiter.availablePermits());
    }

    public void testBlocksWhenExhausted() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1);
        limiter.acquire();
        assertEquals(0, limiter.availablePermits());

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                limiter.acquire();
                acquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);

        Thread.sleep(100);
        assertFalse(acquired.get());

        limiter.release();
        blocker.join(5000);
        assertTrue(acquired.get());
    }

    public void testWaiterDoesNotTimeOutAndIsInterruptible() throws Exception {
        // The guardrail blocks rather than failing on a deadline: a waiter on an exhausted limiter keeps
        // waiting (no TimeoutException), but stays interruptible so query cancellation unblocks it.
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1);
        limiter.acquire();

        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                limiter.acquire();
            } catch (InterruptedException e) {
                caught.set(e);
            }
        });
        waiter.start();
        started.await(5, TimeUnit.SECONDS);

        // Well past the old 50ms deadline: still waiting, not failed.
        Thread.sleep(200);
        assertNull("waiter must not fail on a deadline", caught.get());

        waiter.interrupt();
        waiter.join(5000);
        assertTrue("interrupt must unblock the waiter", caught.get() instanceof InterruptedException);
    }

    public void testDisabledWithZeroPermits() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(0);
        assertFalse(limiter.isEnabled());
        assertEquals(Integer.MAX_VALUE, limiter.availablePermits());
        limiter.acquire();
        limiter.release();
    }

    public void testUnlimitedSingleton() throws Exception {
        assertFalse(ConcurrencyLimiter.UNLIMITED.isEnabled());
        assertEquals(0, ConcurrencyLimiter.UNLIMITED.maxPermits());
        ConcurrencyLimiter.UNLIMITED.acquire();
        ConcurrencyLimiter.UNLIMITED.release();
    }

    public void testMaxPermits() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(42);
        assertTrue(limiter.isEnabled());
        assertEquals(42, limiter.maxPermits());
    }
}
