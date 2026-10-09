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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

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

    public void testThrowingExecutorStillDeliversGrantInline() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        EsRejectedExecutionException rejected = new EsRejectedExecutionException("rejected");
        CountDownLatch granted = new CountDownLatch(1);
        budget.acquireAsync(null, false, () -> false, r -> { throw rejected; })
            .addListener(ActionListener.wrap(unused -> granted.countDown(), e -> {
                throw new AssertionError(e);
            }));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        budget.release();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(1, budget.inFlight());
        assertEquals(0, budget.waiterCount());
        budget.release();
        assertEquals(0, budget.inFlight());
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

    public void testAcquireAsyncDeliversGrantInline() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<String> grantThread = new AtomicReference<>();
        budget.acquireAsync(null, false, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
            grantThread.set(Thread.currentThread().getName());
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        String releaser = Thread.currentThread().getName();
        budget.release();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(releaser, grantThread.get());
        budget.release();
    }

    public void testGrantProgressWhenIoPoolBlockedInAcquire() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(2, 5_000L, null);
        budget.acquire();
        budget.acquire();
        ExecutorService pool = Executors.newFixedThreadPool(2, r -> new Thread(r, "io-pool"));
        try {
            CountDownLatch asyncGranted = new CountDownLatch(2);
            AtomicReference<Exception> asyncError = new AtomicReference<>();
            for (int i = 0; i < 2; i++) {
                budget.acquireAsync(null, false, () -> false, pool).addListener(ActionListener.wrap(unused -> {
                    budget.release();
                    asyncGranted.countDown();
                }, e -> {
                    asyncError.set(e);
                    asyncGranted.countDown();
                }));
            }
            assertBusy(() -> assertEquals(2, budget.waiterCount()));

            CountDownLatch entered = new CountDownLatch(2);
            CountDownLatch syncDone = new CountDownLatch(2);
            AtomicReference<Exception> syncError = new AtomicReference<>();
            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    entered.countDown();
                    try {
                        budget.acquire();
                        budget.release();
                    } catch (Exception e) {
                        syncError.set(e);
                    } finally {
                        syncDone.countDown();
                    }
                });
            }
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertBusy(() -> assertEquals(4, budget.waiterCount()));

            budget.release();
            budget.release();
            assertTrue("inline grant must not wait for the blocked I/O pool", asyncGranted.await(1, TimeUnit.SECONDS));
            assertNull(asyncError.get());
            assertTrue(syncDone.await(5, TimeUnit.SECONDS));
            assertNull(syncError.get());
            assertEquals(0, budget.inFlight());
        } finally {
            terminate(pool);
        }
    }

    public void testHoldersIgnoresUndeliveredGrant() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        assertEquals(1, budget.holders());
        budget.pauseGrantDelivery();
        CountDownLatch granted = new CountDownLatch(1);
        budget.acquireAsync(null, false, () -> false, Runnable::run)
            .addListener(ActionListener.wrap(unused -> granted.countDown(), e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        budget.release();
        assertEquals(0, budget.waiterCount());
        assertEquals(1, budget.inFlight());
        assertEquals(0, budget.holders());
        assertEquals(1, granted.getCount());
        budget.resumeGrantDelivery();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(1, budget.holders());
        budget.release();
        assertEquals(0, budget.holders());
    }

    public void testAllocatorHoldersIgnoresUndeliveredGrant() throws Exception {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(2);
        QueryConcurrencyBudget budget = allocator.register();
        budget.pauseGrantDelivery();
        CountDownLatch granted = new CountDownLatch(2);
        budget.acquireAsync(null, false, () -> false, Runnable::run)
            .addListener(ActionListener.wrap(unused -> granted.countDown(), e -> granted.countDown()));
        budget.acquireAsync(null, false, () -> false, Runnable::run)
            .addListener(ActionListener.wrap(unused -> granted.countDown(), e -> granted.countDown()));
        assertEquals(2, budget.inFlight());
        assertEquals(0, budget.holders());
        assertEquals(0, allocator.holders());
        budget.resumeGrantDelivery();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(2, budget.holders());
        assertEquals(2, allocator.holders());
        budget.release();
        budget.release();
        budget.close();
    }

    public void testInlineGrantChainDoesNotRecurse() throws Exception {
        int n = 256;
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        CountDownLatch all = new CountDownLatch(n);
        AtomicInteger maxDepth = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            budget.acquireAsync(null, false, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
                maxDepth.accumulateAndGet(Thread.currentThread().getStackTrace().length, Math::max);
                budget.release();
                all.countDown();
            }, e -> all.countDown()));
        }
        assertBusy(() -> assertEquals(n, budget.waiterCount()));
        budget.release();
        assertTrue(all.await(5, TimeUnit.SECONDS));
        assertEquals(0, budget.inFlight());
        assertEquals(0, budget.holders());
        assertThat(maxDepth.get(), lessThan(200));
    }

    public void testSyncAcquireFromGrantContinuationAsserts() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(1, 60_000L, null);
        budget.acquire();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<AssertionError> asserted = new AtomicReference<>();
        budget.acquireAsync(null, false, () -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
            try {
                budget.acquire();
            } catch (AssertionError e) {
                asserted.set(e);
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                budget.release();
                done.countDown();
            }
        }, e -> { throw new AssertionError(e); }));
        budget.release();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertNotNull(asserted.get());
        assertThat(asserted.get().getMessage(), containsString("grant continuation"));
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
