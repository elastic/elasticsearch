/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link ExternalIoExecutors} must restore caller {@link ThreadContext} and
 * {@link StorageRetryCancellation}, and a throwing inner {@code execute} must still deliver
 * {@link AbstractRunnable#onRejection} so {@link ThrottledIterator} refs drain.
 */
public class ExternalIoExecutorsTests extends ESTestCase {

    public void testRestoringPutsCallerHeaderOnTask() {
        String headerName = "x-test-auth-marker";
        String headerValue = "authenticated-user";
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(headerName, headerValue);
        var restorable = threadContext.newRestorableContext(true);
        AtomicReference<String> seen = new AtomicReference<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            assertNull(threadContext.getHeader(headerName));
            ExternalIoExecutors.restoring(Runnable::run, restorable, null).execute(() -> seen.set(threadContext.getHeader(headerName)));
            assertNull("restorable snapshot must not leak onto the calling thread", threadContext.getHeader(headerName));
        }
        assertEquals(headerValue, seen.get());
    }

    public void testRestoringInstallsCancellationScope() {
        AtomicBoolean sawCancelled = new AtomicBoolean();
        ExternalIoExecutors.restoring(Runnable::run, null, () -> true)
            .execute(() -> sawCancelled.set(StorageRetryCancellation.isCancelled()));
        assertTrue(sawCancelled.get());
        assertFalse(StorageRetryCancellation.isCancelled());
    }

    /**
     * A throwing {@code execute} must call {@code onRejection} then {@code onAfter} on an
     * {@link AbstractRunnable}. A lambda wrapper would throw instead and skip both.
     */
    public void testPreservingDeliversOnRejectionWhenExecuteThrows() {
        AtomicBoolean rejected = new AtomicBoolean();
        AtomicBoolean after = new AtomicBoolean();
        AtomicBoolean ran = new AtomicBoolean();
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            protected void doRun() {
                ran.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not run when onRejection is overridden: " + e);
            }

            @Override
            public void onRejection(Exception e) {
                assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("no slots"));
                rejected.set(true);
            }

            @Override
            public void onAfter() {
                after.set(true);
            }
        };
        Executor rejecting = command -> { throw new EsRejectedExecutionException("no slots", false); };
        ExternalIoExecutors.preserving(rejecting, Runnable::run).execute(task);
        assertFalse("rejected task must not run", ran.get());
        assertTrue(rejected.get());
        assertTrue(after.get());
    }

    /**
     * {@link ThrottledIterator} continuation is an {@link AbstractRunnable}. If the wrapped
     * executor throws from {@code execute}, {@code onContinuationFailure} must fire and
     * {@code onAfter} must drop the extra ref so {@code onCompletion} still runs.
     */
    public void testPreservingDrainsThrottledIteratorWhenExecuteThrows() {
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<Releasable> firstRel = new AtomicReference<>();
        Executor rejecting = command -> { throw new EsRejectedExecutionException("no slots", false); };
        Executor wrapped = ExternalIoExecutors.preserving(rejecting, Runnable::run);

        ThrottledIterator.run(List.of(1, 2).iterator(), (releasable, item) -> {
            if (item == 1) {
                firstRel.set(releasable);
            } else {
                releasable.close();
            }
        }, 1, () -> completed.set(true), wrapped, failure::set);

        assertFalse("first item still held, completion must wait", completed.get());
        assertNotNull(firstRel.get());
        firstRel.get().close();
        assertNotNull("continuation rejection must surface", failure.get());
        assertThat(failure.get().getMessage(), org.hamcrest.Matchers.containsString("no slots"));
        assertTrue("ThrottledIterator refs must drain after rejection", completed.get());
    }

    public void testPreservingForwardsForceExecution() {
        AtomicBoolean forceSeen = new AtomicBoolean();
        AtomicInteger afters = new AtomicInteger();
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            protected void doRun() {}

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }

            @Override
            public void onAfter() {
                afters.incrementAndGet();
            }
        };
        Executor inner = command -> {
            assertTrue(command instanceof AbstractRunnable);
            forceSeen.set(((AbstractRunnable) command).isForceExecution());
            command.run();
        };
        ExternalIoExecutors.preserving(inner, Runnable::run).execute(task);
        assertTrue(forceSeen.get());
        assertEquals("success path must run inner onAfter exactly once", 1, afters.get());
    }

    public void testRestoringPlainRunnableRethrowsWhenExecuteThrows() {
        Executor rejecting = command -> { throw new EsRejectedExecutionException("no slots", false); };
        EsRejectedExecutionException e = expectThrows(
            EsRejectedExecutionException.class,
            () -> ExternalIoExecutors.restoring(rejecting, null, null).execute(() -> {})
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("no slots"));
    }

    /**
     * DIRECT {@code execute} runs the task inline. A throwing {@code onFailure} must not be
     * treated as a pool rejection (that would call {@code onFailure} a second time).
     */
    public void testPreservingInlineExecutorDoesNotRejectAfterOnFailureThrows() {
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger rejections = new AtomicInteger();
        AtomicInteger afters = new AtomicInteger();
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            protected void doRun() {
                throw new IllegalStateException("task failed");
            }

            @Override
            public void onFailure(Exception e) {
                failures.incrementAndGet();
                throw new RuntimeException("onFailure throws", e);
            }

            @Override
            public void onRejection(Exception e) {
                rejections.incrementAndGet();
            }

            @Override
            public void onAfter() {
                afters.incrementAndGet();
            }
        };
        ExternalIoExecutors.preserving(Runnable::run, Runnable::run).execute(task);
        assertEquals(1, failures.get());
        assertEquals(0, rejections.get());
        assertEquals(1, afters.get());
    }

    public void testNestedPreservingStillDeliversRejection() {
        AtomicInteger rejections = new AtomicInteger();
        AtomicInteger afters = new AtomicInteger();
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            protected void doRun() {
                fail("must not run");
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }

            @Override
            public void onRejection(Exception e) {
                rejections.incrementAndGet();
            }

            @Override
            public void onAfter() {
                afters.incrementAndGet();
            }
        };
        Executor rejecting = command -> { throw new EsRejectedExecutionException("no slots", false); };
        Executor cancelWrap = ExternalIoExecutors.restoring(rejecting, null, () -> false);
        Executor cpuWrap = ExternalIoExecutors.preserving(cancelWrap, Runnable::run);
        cpuWrap.execute(task);
        assertEquals(1, rejections.get());
        assertEquals(1, afters.get());
    }
}
