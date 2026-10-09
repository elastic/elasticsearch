/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class AsyncQueryConcurrencyBudgetTests extends ESTestCase {

    public void testAcquireAsyncImmediate() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(2, 60_000L, null);
        CountDownLatch done = new CountDownLatch(1);
        budget.acquireAsync(null, false, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> done.countDown(), e -> {
            throw new AssertionError(e);
        }));
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals(1, budget.inFlight());
        budget.release();
        assertEquals(0, budget.inFlight());
    }

    public void testForkRejectAfterGrantReleasesPermit() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        EsRejectedExecutionException rejected = new EsRejectedExecutionException("rejected");
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.acquireAsync(null, false, () -> false, r -> { throw rejected; })
            .addListener(ActionListener.wrap(unused -> fail("rejected"), e -> {
                error.set(e);
                failed.countDown();
            }));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        budget.release();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertSame(rejected, error.get());
        assertEquals(0, budget.inFlight());
        assertEquals(0, budget.waiterCount());
    }

    public void testAcquireAsyncWaitsThenGrantsOnRelease() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        CountDownLatch granted = new CountDownLatch(1);
        budget.acquireAsync(null, false, () -> false, Runnable::run)
            .addListener(ActionListener.wrap(unused -> granted.countDown(), e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        assertEquals(1, budget.inFlight());
        assertFalse(granted.await(20, TimeUnit.MILLISECONDS));
        budget.release();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(1, budget.inFlight());
        budget.release();
        assertEquals(0, budget.inFlight());
        assertEquals(0, budget.waiterCount());
    }

    public void testAcquireAsyncCancelFailsWaiterWithoutPermit() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        AtomicBoolean cancel = new AtomicBoolean();
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.acquireAsync(null, false, cancel::get, Runnable::run).addListener(ActionListener.wrap(unused -> fail("cancelled"), e -> {
            error.set(e);
            failed.countDown();
        }));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        cancel.set(true);
        budget.wakeAsyncWaiters();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(TaskCancelledException.class));
        assertEquals(1, budget.inFlight());
        assertEquals(0, budget.waiterCount());
        budget.release();
        assertEquals(0, budget.inFlight());
    }

    public void testAcquireAsyncChooseKeepsCloserLease() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        RowGroupIo farther = bound(budget, 6);
        RowGroupIo closer = bound(budget, 2);
        budget.acquire(farther, true);
        CountDownLatch fartherGranted = new CountDownLatch(1);
        CountDownLatch closerGranted = new CountDownLatch(1);
        AtomicBoolean fartherGotSecond = new AtomicBoolean();
        AtomicBoolean closerGot = new AtomicBoolean();
        budget.acquireAsync(farther, true, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
            fartherGotSecond.set(true);
            fartherGranted.countDown();
        }, e -> fartherGranted.countDown()));
        budget.acquireAsync(closer, true, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
            closerGot.set(true);
            closerGranted.countDown();
        }, e -> closerGranted.countDown()));
        assertBusy(() -> assertEquals(2, budget.waiterCount()));
        budget.release(farther, true);
        assertTrue(closerGranted.await(5, TimeUnit.SECONDS));
        assertTrue(closerGot.get());
        assertFalse("closer lease must unseat the farther waiter", fartherGotSecond.get());
        budget.release(closer, true);
        assertTrue(fartherGranted.await(5, TimeUnit.SECONDS));
        budget.release(farther, true);
        assertEquals(0, budget.inFlight());
        assertEquals(0, budget.waiterCount());
    }

    public void testAcquireAsyncForksGrant() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        ExecutorService grantPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "qcb-grant"));
        try {
            CountDownLatch granted = new CountDownLatch(1);
            AtomicReference<String> grantThread = new AtomicReference<>();
            budget.acquireAsync(null, false, () -> false, grantPool).addListener(ActionListener.wrap(unused -> {
                grantThread.set(Thread.currentThread().getName());
                granted.countDown();
            }, e -> granted.countDown()));
            assertBusy(() -> assertEquals(1, budget.waiterCount()));
            budget.release();
            assertTrue(granted.await(5, TimeUnit.SECONDS));
            assertEquals("qcb-grant", grantThread.get());
            budget.release();
        } finally {
            grantPool.shutdownNow();
        }
    }

    public void testCloseFailsAsyncWaiters() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.acquireAsync(null, false, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> fail("closed"), e -> {
            error.set(e);
            failed.countDown();
        }));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        budget.close();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(TimeoutException.class));
        assertEquals(0, budget.waiterCount());
    }

    private static RowGroupIo bound(QueryConcurrencyBudget budget, int unissued) {
        RowGroupIo lease = new RowGroupIo();
        lease.addUnissued(unissued);
        budget.bind(lease);
        return lease;
    }
}
