/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RetryableActionTests extends ESTestCase {

    private DeterministicTaskQueue taskQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue();
    }

    public void testRetryableActionNoRetries() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueSeconds(30),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                listener.onResponse(true);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return true;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertEquals(1, executedCount.get());
        assertTrue(future.actionGet());
    }

    public void testRetryableActionWillRetry() {
        int expectedRetryCount = randomIntBetween(1, 8);
        final AtomicInteger remainingFailedCount = new AtomicInteger(expectedRetryCount);
        final AtomicInteger retryCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueSeconds(30),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (remainingFailedCount.getAndDecrement() == 0) {
                    listener.onResponse(true);
                } else {
                    if (randomBoolean()) {
                        listener.onFailure(new EsRejectedExecutionException());
                    } else {
                        throw new EsRejectedExecutionException();
                    }
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                retryCount.getAndIncrement();
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        for (int i = 0; i < expectedRetryCount; ++i) {
            assertTrue(taskQueue.hasDeferredTasks());
            final long deferredExecutionTime = taskQueue.getLatestDeferredExecutionTime();
            final long millisBound = 10 << i;
            assertThat(deferredExecutionTime, lessThanOrEqualTo(millisBound + previousDeferredTime));
            previousDeferredTime = deferredExecutionTime;
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertEquals(expectedRetryCount, retryCount.get());
        assertTrue(future.actionGet());
    }

    public void testRetryableActionTimeout() {
        final AtomicInteger retryCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(randomFrom(1, 10, randomIntBetween(100, 2000))),
            TimeValue.timeValueSeconds(1),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (randomBoolean()) {
                    listener.onFailure(new EsRejectedExecutionException());
                } else {
                    throw new EsRejectedExecutionException();
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                retryCount.getAndIncrement();
                return e instanceof EsRejectedExecutionException;
            }
        };
        long begin = taskQueue.getCurrentTimeMillis();
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        while (previousDeferredTime < 1000) {
            assertTrue(taskQueue.hasDeferredTasks());
            previousDeferredTime = taskQueue.getLatestDeferredExecutionTime();
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertFalse(taskQueue.hasDeferredTasks());
        assertFalse(taskQueue.hasRunnableTasks());

        expectThrows(EsRejectedExecutionException.class, future::actionGet);

        long end = taskQueue.getCurrentTimeMillis();
        // max 20% greater than the timeout.
        assertThat(end - begin, lessThanOrEqualTo(1200L));
    }

    public void testTimeoutOfZeroMeansNoRetry() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueSeconds(0),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                throw new EsRejectedExecutionException();
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertEquals(1, executedCount.get());
        expectThrows(EsRejectedExecutionException.class, future::actionGet);
    }

    public void testFailedBecauseNotRetryable() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueSeconds(30),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                throw new IllegalStateException();
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertEquals(1, executedCount.get());
        expectThrows(IllegalStateException.class, future::actionGet);
    }

    public void testRetryableActionCancelled() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueSeconds(30),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (executedCount.incrementAndGet() == 1) {
                    throw new EsRejectedExecutionException();
                } else {
                    listener.onResponse(true);
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();

        retryableAction.cancel(new ElasticsearchException("Cancelled"));
        taskQueue.runAllRunnableTasks();

        // A second run will not occur because it is cancelled
        assertEquals(1, executedCount.get());
        expectThrows(ElasticsearchException.class, future::actionGet);
    }

    public void testMaxDelayBound() {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<>(
            logger,
            taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(50),
            TimeValue.timeValueSeconds(1),
            future,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (randomBoolean()) {
                    listener.onFailure(new EsRejectedExecutionException());
                } else {
                    throw new EsRejectedExecutionException();
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        while (previousDeferredTime < 1000) {
            assertTrue(taskQueue.hasDeferredTasks());
            long latestDeferredExecutionTime = taskQueue.getLatestDeferredExecutionTime();
            assertThat(latestDeferredExecutionTime - previousDeferredTime, lessThanOrEqualTo(50L));
            previousDeferredTime = latestDeferredExecutionTime;
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertFalse(taskQueue.hasDeferredTasks());
        assertFalse(taskQueue.hasRunnableTasks());

        expectThrows(EsRejectedExecutionException.class, future::actionGet);
    }
}
