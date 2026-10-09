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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class AsyncConcurrencyLimiterTests extends ESTestCase {

    public void testAcquireAsyncImmediate() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(2, false));
        CountDownLatch done = new CountDownLatch(1);
        limiter.acquireAsync(() -> false, Runnable::run).addListener(ActionListener.wrap(unused -> done.countDown(), e -> {
            throw new AssertionError(e);
        }));
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals(1, limiter.availablePermits());
        limiter.release();
        assertEquals(2, limiter.availablePermits());
    }

    public void testThrowingExecutorStillDeliversGrantInline() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        EsRejectedExecutionException rejected = new EsRejectedExecutionException("rejected");
        CountDownLatch granted = new CountDownLatch(1);
        limiter.acquireAsync(() -> false, r -> { throw rejected; }).addListener(ActionListener.wrap(unused -> granted.countDown(), e -> {
            throw new AssertionError(e);
        }));
        assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
        limiter.release();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(0, limiter.availablePermits());
        assertEquals(0, limiter.asyncWaiterCount());
        limiter.release();
        assertEquals(1, limiter.availablePermits());
    }

    public void testReleaseTransfersPermitToFifoWaiter() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        assertEquals(0, limiter.availablePermits());
        List<Integer> order = new CopyOnWriteArrayList<>();
        CountDownLatch allGranted = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            int id = i;
            limiter.acquireAsync(() -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
                order.add(id);
                limiter.release();
                allGranted.countDown();
            }, e -> allGranted.countDown()));
        }
        assertBusy(() -> assertEquals(3, limiter.asyncWaiterCount()));
        limiter.release();
        assertTrue(allGranted.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(0, 1, 2), order);
        assertEquals(1, limiter.availablePermits());
        assertEquals(0, limiter.asyncWaiterCount());
    }

    public void testAcquireAsyncCancelDoesNotTakePermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        AtomicBoolean cancel = new AtomicBoolean();
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        limiter.acquireAsync(cancel::get, Runnable::run).addListener(ActionListener.wrap(unused -> fail("cancelled"), e -> {
            error.set(e);
            failed.countDown();
        }));
        assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
        cancel.set(true);
        limiter.wakeAsyncWaiters();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(TaskCancelledException.class));
        assertEquals(0, limiter.availablePermits());
        assertEquals(0, limiter.asyncWaiterCount());
        limiter.release();
        assertEquals(1, limiter.availablePermits());
    }

    public void testAcquireAsyncDeliversGrantInline() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<String> grantThread = new AtomicReference<>();
        limiter.acquireAsync(() -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
            grantThread.set(Thread.currentThread().getName());
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
        String releaser = Thread.currentThread().getName();
        limiter.release();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(releaser, grantThread.get());
        limiter.release();
    }

    public void testGrantProgressWhenIoPoolBlockedInAcquire() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(2, false), 5_000L);
        limiter.acquire();
        limiter.acquire();
        ExecutorService pool = Executors.newFixedThreadPool(2, r -> new Thread(r, "io-pool"));
        try {
            CountDownLatch asyncGranted = new CountDownLatch(2);
            AtomicReference<Exception> asyncError = new AtomicReference<>();
            for (int i = 0; i < 2; i++) {
                limiter.acquireAsync(() -> false, pool).addListener(ActionListener.wrap(unused -> {
                    limiter.release();
                    asyncGranted.countDown();
                }, e -> {
                    asyncError.set(e);
                    asyncGranted.countDown();
                }));
            }
            assertBusy(() -> assertEquals(2, limiter.asyncWaiterCount()));

            CountDownLatch entered = new CountDownLatch(2);
            CountDownLatch syncDone = new CountDownLatch(2);
            AtomicReference<Exception> syncError = new AtomicReference<>();
            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    entered.countDown();
                    try {
                        limiter.acquire();
                        limiter.release();
                    } catch (Exception e) {
                        syncError.set(e);
                    } finally {
                        syncDone.countDown();
                    }
                });
            }
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertBusy(() -> {
                Thread[] threads = new Thread[Thread.activeCount() + 8];
                int n = Thread.enumerate(threads);
                int parked = 0;
                for (int i = 0; i < n; i++) {
                    if (threads[i] != null
                        && threads[i].getName().startsWith("io-pool")
                        && (threads[i].getState() == Thread.State.WAITING || threads[i].getState() == Thread.State.TIMED_WAITING)) {
                        parked++;
                    }
                }
                assertEquals(2, parked);
            });

            limiter.release();
            limiter.release();
            assertTrue("inline grant must not wait for the blocked I/O pool", asyncGranted.await(1, TimeUnit.SECONDS));
            assertNull(asyncError.get());
            assertTrue(syncDone.await(5, TimeUnit.SECONDS));
            assertNull(syncError.get());
            assertEquals(2, limiter.availablePermits());
        } finally {
            terminate(pool);
        }
    }

    public void testHoldersIgnoresUndeliveredGrant() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        assertEquals(1, limiter.holders());
        limiter.pauseGrantDelivery();
        CountDownLatch granted = new CountDownLatch(1);
        limiter.acquireAsync(() -> false, Runnable::run)
            .addListener(ActionListener.wrap(unused -> granted.countDown(), e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
        limiter.release();
        assertEquals(0, limiter.asyncWaiterCount());
        assertEquals(0, limiter.availablePermits());
        assertEquals(0, limiter.holders());
        assertEquals(1, granted.getCount());
        limiter.resumeGrantDelivery();
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertEquals(1, limiter.holders());
        limiter.release();
        assertEquals(0, limiter.holders());
        assertEquals(1, limiter.availablePermits());
    }

    public void testInlineGrantChainDoesNotRecurse() throws Exception {
        int n = 256;
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        CountDownLatch all = new CountDownLatch(n);
        AtomicInteger maxDepth = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            limiter.acquireAsync(() -> false, Runnable::run).addListener(ActionListener.wrap(unused -> {
                maxDepth.accumulateAndGet(Thread.currentThread().getStackTrace().length, Math::max);
                limiter.release();
                all.countDown();
            }, e -> all.countDown()));
        }
        assertBusy(() -> assertEquals(n, limiter.asyncWaiterCount()));
        limiter.release();
        assertTrue(all.await(5, TimeUnit.SECONDS));
        assertEquals(1, limiter.availablePermits());
        assertEquals(0, limiter.holders());
        assertThat(maxDepth.get(), lessThan(200));
    }

    public void testCancelledGrantDoesNotLeakPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        AtomicBoolean cancel = new AtomicBoolean();
        CountDownLatch failed = new CountDownLatch(1);
        limiter.acquireAsync(cancel::get, Runnable::run)
            .addListener(ActionListener.wrap(unused -> fail("grant should have been cancelled"), e -> failed.countDown()));
        assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
        cancel.set(true);
        limiter.release();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertEquals(1, limiter.availablePermits());
        assertEquals(0, limiter.asyncWaiterCount());
    }

    public void testUnlimitedAcquireAsyncCompletes() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        ConcurrencyLimiter.UNLIMITED.acquireAsync(() -> false, Runnable::run)
            .addListener(ActionListener.wrap(unused -> done.countDown(), e -> done.countDown()));
        assertTrue(done.await(5, TimeUnit.SECONDS));
    }
}
