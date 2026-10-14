/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalObjectChangedException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Covers the retry of a query whose external object changed under it. The attempts run on the calling thread, so the
 * executor is a direct one unless a test needs it to reject.
 */
public class ObjectChangedRetryTests extends ESTestCase {

    private static final String RESULT = "rows of the new version";

    /**
     * An attempt that fails with the object-changed condition for its first {@code failures} calls, then succeeds. The counter
     * records how many attempts were started.
     */
    private static Consumer<ActionListener<String>> attemptFailing(AtomicInteger attempts, int failures, ExternalException failure) {
        return listener -> {
            if (attempts.incrementAndGet() <= failures) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(RESULT);
            }
        };
    }

    private static ExternalObjectChangedException objectChanged() {
        return new ExternalObjectChangedException(StoragePath.of("s3://bucket/part-0.parquet"));
    }

    /** Runs the retry and returns what the listener received: either the result or the failure. */
    private static Outcome run(
        Consumer<ActionListener<String>> attempt,
        ThreadContext threadContext,
        Executor executor,
        boolean retryable,
        boolean cancelled
    ) {
        AtomicReference<String> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ObjectChangedRetry.run(
            attempt,
            ActionListener.wrap(result::set, failure::set),
            threadContext,
            executor,
            () -> cancelled,
            retryable
        );
        return new Outcome(result.get(), failure.get());
    }

    private record Outcome(String result, Exception failure) {}

    public void testRetriesOnceAndReturnsTheSecondAttempt() {
        AtomicInteger attempts = new AtomicInteger();
        Outcome outcome = run(attemptFailing(attempts, 1, objectChanged()), new ThreadContext(Settings.EMPTY), Runnable::run, true, false);
        assertThat(outcome.result(), equalTo(RESULT));
        assertThat(outcome.failure(), nullValue());
        assertThat(attempts.get(), equalTo(2));
    }

    public void testSecondObjectChangedFailureIsReturnedWithoutThirdAttempt() {
        AtomicInteger attempts = new AtomicInteger();
        ExternalObjectChangedException second = objectChanged();
        Consumer<ActionListener<String>> attempt = listener -> {
            if (attempts.incrementAndGet() == 1) {
                listener.onFailure(objectChanged());
            } else {
                listener.onFailure(second);
            }
        };
        Outcome outcome = run(attempt, new ThreadContext(Settings.EMPTY), Runnable::run, true, false);
        assertThat(outcome.result(), nullValue());
        assertThat("the second failure is the one the client sees", outcome.failure(), sameInstance(second));
        assertThat("a bounded retry makes exactly two attempts", attempts.get(), equalTo(2));
    }

    public void testOtherFailureIsNotRetried() {
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("not an object change");
        Consumer<ActionListener<String>> attempt = listener -> {
            attempts.incrementAndGet();
            listener.onFailure(failure);
        };
        Outcome outcome = run(attempt, new ThreadContext(Settings.EMPTY), Runnable::run, true, false);
        assertThat(outcome.failure(), sameInstance(failure));
        assertThat(attempts.get(), equalTo(1));
    }

    public void testAsyncQueryIsNotRetried() {
        AtomicInteger attempts = new AtomicInteger();
        ExternalObjectChangedException failure = objectChanged();
        Outcome outcome = run(attemptFailing(attempts, 1, failure), new ThreadContext(Settings.EMPTY), Runnable::run, false, false);
        assertThat(outcome.failure(), sameInstance(failure));
        assertThat(attempts.get(), equalTo(1));
    }

    public void testCancelledQueryIsNotRetried() {
        AtomicInteger attempts = new AtomicInteger();
        ExternalObjectChangedException failure = objectChanged();
        Outcome outcome = run(attemptFailing(attempts, 1, failure), new ThreadContext(Settings.EMPTY), Runnable::run, true, true);
        assertThat(outcome.failure(), sameInstance(failure));
        assertThat(attempts.get(), equalTo(1));
    }

    /**
     * The search pool rejects the retry: its single worker is busy and its queue has no room. An Elasticsearch pool reports the
     * rejection to an {@link AbstractRunnable} through {@code onRejection} rather than throwing it. The client must still see the
     * object-changed failure, and the rejection is kept as a suppressed exception for diagnosis.
     */
    public void testRejectedRetryOnARealPoolReturnsTheOriginalFailure() throws Exception {
        EsThreadPoolExecutor pool = EsExecutors.newFixed(
            "test-retry-reject",
            1,
            0,
            EsExecutors.daemonThreadFactory("test", "retry-reject"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        CountDownLatch workerBusy = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            pool.execute(() -> {
                workerBusy.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue("blocking task did not start", workerBusy.await(10, TimeUnit.SECONDS));

            AtomicInteger attempts = new AtomicInteger();
            ExternalObjectChangedException failure = objectChanged();
            Outcome outcome = run(attemptFailing(attempts, 1, failure), new ThreadContext(Settings.EMPTY), pool, true, false);
            assertThat(outcome.failure(), sameInstance(failure));
            assertThat("the rejected retry must not run", attempts.get(), equalTo(1));
            assertThat(failure.getSuppressed().length, equalTo(1));
            assertThat(failure.getSuppressed()[0], instanceOf(EsRejectedExecutionException.class));
        } finally {
            release.countDown();
            terminate(pool);
        }
    }

    /**
     * An executor that throws the rejection directly, rather than reporting it through {@code onRejection}, must also leave the
     * client with the object-changed failure.
     */
    public void testRejectedRetryThrownByTheExecutorReturnsTheOriginalFailure() {
        AtomicInteger attempts = new AtomicInteger();
        ExternalObjectChangedException failure = objectChanged();
        Outcome outcome = run(attemptFailing(attempts, 1, failure), new ThreadContext(Settings.EMPTY), command -> {
            throw new EsRejectedExecutionException("search pool is full", false);
        }, true, false);
        assertThat(outcome.failure(), sameInstance(failure));
        assertThat(attempts.get(), equalTo(1));
        assertThat(failure.getSuppressed().length, equalTo(1));
        assertThat(failure.getSuppressed()[0], instanceOf(EsRejectedExecutionException.class));
    }

    /**
     * Warnings from the failed attempt must not reach the response: the retry starts from the thread context captured before the
     * first attempt.
     */
    public void testRetryDropsResponseHeadersOfTheFailedAttempt() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AtomicInteger attempts = new AtomicInteger();
        Consumer<ActionListener<String>> attempt = listener -> {
            if (attempts.incrementAndGet() == 1) {
                threadContext.addResponseHeader("Warning", "from the failed attempt");
                listener.onFailure(objectChanged());
            } else {
                listener.onResponse(RESULT);
            }
        };
        Outcome outcome = run(attempt, threadContext, Runnable::run, true, false);
        assertThat(outcome.result(), equalTo(RESULT));
        List<String> warnings = threadContext.getResponseHeaders().get("Warning");
        assertThat("the failed attempt's warning must not reach the response", warnings, nullValue());
    }
}
