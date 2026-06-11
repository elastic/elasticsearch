/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

public class ConcurrencyLimiterTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(5);
        assertEquals(5, limiter.availablePermits());
        limiter.acquire();
        assertEquals(4, limiter.availablePermits());
        limiter.release();
        assertEquals(5, limiter.availablePermits());
    }

    public void testBlocksWhenExhausted() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1, 60_000L);
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

    public void testTimeoutThrows() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1, 50L);
        limiter.acquire();

        expectThrows(TimeoutException.class, limiter::acquire);

        limiter.release();
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

    /**
     * The acquire timeout is progress-aware (mirrors
     * {@code QueryConcurrencyBudgetTests#testWaiterSurvivesPersonalTimeoutWhileHoldersMakeProgress}):
     * a waiter whose personal timeout expires must keep waiting (and eventually acquire) as long as
     * the pool keeps releasing permits at sub-timeout intervals. With one permit, a 500ms timeout,
     * and 8 queued waiters each dwelling 100ms, the last waiter queues for ~900ms — far past its
     * personal timeout — but every release lands well inside one timeout window.
     */
    public void testWaiterSurvivesPersonalTimeoutWhileHoldersMakeProgress() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1, 500L);
        limiter.acquire();

        int waiters = 8;
        CountDownLatch done = new CountDownLatch(waiters);
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (int i = 0; i < waiters; i++) {
            new Thread(() -> {
                try {
                    limiter.acquire();
                    try {
                        Thread.sleep(100);
                    } finally {
                        limiter.release();
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
                done.countDown();
            }).start();
        }

        Thread.sleep(100);
        limiter.release(); // start the drain chain; from here each waiter's release feeds the next
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(1, limiter.availablePermits());
    }

    /**
     * The stall-detection property is preserved: once releases stop, a waiter times out even if the
     * pool made progress earlier. The single release here goes to the first queued waiter (fair
     * semaphore, FIFO) which holds its permit, extending the second waiter's deadline exactly once —
     * then no further release happens for a full timeout window and the timeout must fire.
     */
    public void testTimeoutFiresAfterProgressStops() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1, 500L);
        limiter.acquire();

        CountDownLatch releaseHolder = new CountDownLatch(1);
        AtomicReference<Exception> holderFailure = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try {
                limiter.acquire();
                releaseHolder.await(30, TimeUnit.SECONDS);
                limiter.release();
            } catch (Exception e) {
                holderFailure.compareAndSet(null, e);
            }
        });
        holder.start();
        Thread.sleep(100); // make sure the holder is first in the FIFO acquire queue

        AtomicReference<Exception> caught = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                limiter.acquire();
                limiter.release();
            } catch (Exception e) {
                caught.set(e);
            }
            finished.countDown();
        });
        waiter.start();
        Thread.sleep(200);

        limiter.release(); // the holder takes the permit and never releases: progress, then stall

        assertTrue(finished.await(30, TimeUnit.SECONDS));
        assertNotNull(caught.get());
        assertTrue(caught.get() instanceof TimeoutException);
        assertThat(caught.get().getMessage(), containsString("Timed out waiting"));

        releaseHolder.countDown();
        holder.join(5000);
        assertNull(holderFailure.get());
        waiter.join(5000);
        assertEquals(1, limiter.availablePermits());
    }
}
