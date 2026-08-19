/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class StorageRetryCancellationTests extends ESTestCase {

    public void testUnsetScopeIsNotCancelled() {
        assertFalse(StorageRetryCancellation.isCancelled());
    }

    public void testScopeReflectsSupplier() throws Exception {
        StorageRetryCancellation.runWithCancellation(() -> true, () -> assertTrue(StorageRetryCancellation.isCancelled()));
        StorageRetryCancellation.runWithCancellation(() -> false, () -> assertFalse(StorageRetryCancellation.isCancelled()));
        // The scope is cleared on exit.
        assertFalse(StorageRetryCancellation.isCancelled());
    }

    public void testNestedScopesRestoreOuterSupplier() throws Exception {
        StorageRetryCancellation.runWithCancellation(() -> true, () -> {
            assertTrue(StorageRetryCancellation.isCancelled());
            StorageRetryCancellation.runWithCancellation(() -> false, () -> assertFalse(StorageRetryCancellation.isCancelled()));
            // Inner scope exited: the outer (cancelled) supplier is restored.
            assertTrue(StorageRetryCancellation.isCancelled());
        });
        assertFalse(StorageRetryCancellation.isCancelled());
    }

    public void testCallWithCancellationReturnsValueAndRestores() throws Exception {
        String result = StorageRetryCancellation.callWithCancellation(() -> true, () -> {
            assertTrue(StorageRetryCancellation.isCancelled());
            return "ok";
        });
        assertEquals("ok", result);
        assertFalse(StorageRetryCancellation.isCancelled());
    }

    public void testScopeIsRestoredAfterException() {
        expectThrows(RuntimeException.class, () -> StorageRetryCancellation.runWithCancellation(() -> true, () -> {
            throw new RuntimeException("boom");
        }));
        // Even when the body throws, the thread-local is cleared.
        assertFalse(StorageRetryCancellation.isCancelled());
    }

    public void testSleepReturnsNormallyWhenNotCancelled() throws Exception {
        // No ambient scope: the sleep just elapses and returns.
        StorageRetryCancellation.sleepWithCancellationChecks(10);
        // Within a not-cancelled scope it also returns.
        StorageRetryCancellation.runWithCancellation(() -> false, () -> StorageRetryCancellation.sleepWithCancellationChecks(10));
    }

    public void testSleepAbortsWhenAlreadyCancelled() {
        long startNanos = System.nanoTime();
        expectThrows(
            TaskCancelledException.class,
            () -> StorageRetryCancellation.runWithCancellation(
                () -> true,
                () -> StorageRetryCancellation.sleepWithCancellationChecks(60_000)
            )
        );
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        assertThat("an already-cancelled sleep must not block", elapsedMs, lessThan(5_000L));
    }

    public void testSleepAbortsWhenCancelledDuringSleep() {
        // The signal is NOT cancelled at the first (pre-sleep) poll, then flips true on the next in-sleep
        // poll — exercising the gap where a cancel arrives after the sleep has already started.
        AtomicInteger polls = new AtomicInteger();
        BooleanSupplier supplier = () -> polls.incrementAndGet() > 1;
        long startNanos = System.nanoTime();
        expectThrows(
            TaskCancelledException.class,
            () -> StorageRetryCancellation.runWithCancellation(supplier, () -> StorageRetryCancellation.sleepWithCancellationChecks(60_000))
        );
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        assertThat("a cancel during the sleep must abort within ~one poll interval", elapsedMs, lessThan(5_000L));
    }

    public void testSleepAbortsWhenCancelledFromAnotherThread() throws Exception {
        // Directly models the reviewer's case: the sleep starts, THEN another thread flips the signal.
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CountDownLatch sleeping = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread worker = new Thread(() -> {
            try {
                StorageRetryCancellation.runWithCancellation(cancelled::get, () -> {
                    sleeping.countDown();
                    StorageRetryCancellation.sleepWithCancellationChecks(60_000);
                });
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "storage-retry-cancellation-test");

        long startNanos = System.nanoTime();
        worker.start();
        assertTrue("worker did not start sleeping", sleeping.await(5, TimeUnit.SECONDS));
        // Flip the signal only after the sleep has begun.
        cancelled.set(true);
        worker.join(TimeUnit.SECONDS.toMillis(15));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertFalse("worker should have aborted the sleep and finished", worker.isAlive());
        assertThat(thrown.get(), instanceOf(TaskCancelledException.class));
        assertThat("a cross-thread cancel must not wait out the full delay", elapsedMs, lessThan(15_000L));
    }
}
