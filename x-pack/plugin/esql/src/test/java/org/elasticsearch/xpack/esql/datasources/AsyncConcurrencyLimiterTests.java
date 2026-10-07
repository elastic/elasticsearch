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
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

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

    public void testForkRejectAfterGrantReleasesPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        EsRejectedExecutionException rejected = new EsRejectedExecutionException("rejected");
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        limiter.acquireAsync(() -> false, r -> { throw rejected; }).addListener(ActionListener.wrap(unused -> fail("rejected"), e -> {
            error.set(e);
            failed.countDown();
        }));
        assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
        limiter.release();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertSame(rejected, error.get());
        assertEquals(1, limiter.availablePermits());
        assertEquals(0, limiter.asyncWaiterCount());
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

    public void testAcquireAsyncForksGrant() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false));
        limiter.acquire();
        ExecutorService grantPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "cl-grant"));
        try {
            CountDownLatch granted = new CountDownLatch(1);
            AtomicReference<String> grantThread = new AtomicReference<>();
            limiter.acquireAsync(() -> false, grantPool).addListener(ActionListener.wrap(unused -> {
                grantThread.set(Thread.currentThread().getName());
                granted.countDown();
            }, e -> granted.countDown()));
            assertBusy(() -> assertEquals(1, limiter.asyncWaiterCount()));
            limiter.release();
            assertTrue(granted.await(5, TimeUnit.SECONDS));
            assertEquals("cl-grant", grantThread.get());
            limiter.release();
        } finally {
            grantPool.shutdownNow();
        }
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
