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

public class QueryConcurrencyBudgetTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(10);
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, allocator);
        assertEquals(5, budget.maxPermits());
        assertEquals(0, budget.inFlight());

        budget.acquire();
        assertEquals(1, budget.inFlight());

        budget.release();
        assertEquals(0, budget.inFlight());
    }

    public void testBlocksAtLimit() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        assertEquals(1, budget.inFlight());

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                budget.acquire();
                acquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);

        Thread.sleep(100);
        assertFalse(acquired.get());

        budget.release();
        blocker.join(5000);
        assertTrue(acquired.get());
        budget.release();
    }

    public void testTimeoutThrows() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 50L, null);
        budget.acquire();

        expectThrows(TimeoutException.class, budget::acquire);

        budget.release();
    }

    public void testDynamicResizeUp() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                budget.acquire();
                acquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);
        Thread.sleep(100);
        assertFalse(acquired.get());

        budget.updateMaxPermits(2);
        blocker.join(5000);
        assertTrue(acquired.get());

        budget.release();
        budget.release();
    }

    public void testDynamicResizeDown() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, null);
        budget.acquire();
        budget.acquire();
        assertEquals(2, budget.inFlight());

        budget.updateMaxPermits(1);
        assertEquals(2, budget.inFlight());
        assertEquals(1, budget.maxPermits());

        budget.release();
        budget.release();
    }

    public void testAcquireOnClosedBudget() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, null);
        budget.close();
        assertTrue(budget.isClosed());

        expectThrows(TimeoutException.class, budget::acquire);
    }

    public void testCloseWakesBlockedAcquirers() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();

        AtomicReference<Exception> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                budget.acquire();
            } catch (Exception e) {
                caught.set(e);
            }
            finished.countDown();
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);
        Thread.sleep(100);

        budget.close();
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertNotNull(caught.get());
        assertTrue(caught.get() instanceof TimeoutException);
        assertTrue(caught.get().getMessage().contains("closed"));

        budget.release();
    }

    public void testCloseDeregisters() throws Exception {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget budget = allocator.register();
        assertEquals(1, allocator.activeQueryCount());

        budget.close();
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testUnlimitedBudget() throws Exception {
        assertFalse(QueryConcurrencyBudget.UNLIMITED.isEnabled());
        assertEquals(0, QueryConcurrencyBudget.UNLIMITED.maxPermits());
        QueryConcurrencyBudget.UNLIMITED.acquire();
        QueryConcurrencyBudget.UNLIMITED.release();
    }

    public void testReleaseWithoutAcquire() {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(5, 60_000L, null);
        AssertionError e = expectThrows(AssertionError.class, budget::release);
        assertThat(e.getMessage(), containsString("release() called without a matching acquire()"));
        assertEquals(0, budget.inFlight());
    }

    public void testConcurrentAcquireRelease() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(10, 60_000L, null);
        int threadCount = 20;
        int iterations = 100;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Exception> failure = new AtomicReference<>();

        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < iterations; i++) {
                        budget.acquire();
                        Thread.yield();
                        budget.release();
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
                done.countDown();
            }).start();
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(0, budget.inFlight());
    }
}
