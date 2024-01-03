/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimeoutReleasableListenerTests extends ESTestCase {

    public void testSuccessBeforeTimeout() throws Exception {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var delegateFuture = new PlainActionFuture<Releasable>();
        final var timeoutListener = createTimeoutListener(threadPool, delegateFuture);

        final var released = new AtomicBoolean();
        final var releasable = Releasables.assertOnce(() -> assertTrue(released.compareAndSet(false, true)));
        deterministicTaskQueue.scheduleNow(() -> timeoutListener.onResponse(releasable));
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(delegateFuture.isDone());
        assertSame(releasable, delegateFuture.get());
        assertFalse(released.get());

        // completing again with another releasable means the releasable is closed
        timeoutListener.onResponse(releasable);
        assertTrue(released.get());
    }

    public void testFailureBeforeTimeout() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var delegateFuture = new PlainActionFuture<Releasable>();
        final var timeoutListener = createTimeoutListener(threadPool, delegateFuture);

        deterministicTaskQueue.scheduleNow(() -> timeoutListener.onFailure(new ElasticsearchException("simulated")));
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(delegateFuture.isDone());
        assertEquals("simulated", expectThrows(ExecutionException.class, ElasticsearchException.class, delegateFuture::get).getMessage());

        final var released = new AtomicBoolean();
        final var releasable = Releasables.assertOnce(() -> assertTrue(released.compareAndSet(false, true)));
        timeoutListener.onResponse(releasable);
        assertTrue(released.get());
    }

    public void testTimeout() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var delegateFuture = new PlainActionFuture<Releasable>();
        final var timeoutListener = createTimeoutListener(threadPool, delegateFuture);

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), Matchers.lessThanOrEqualTo((long) MAX_TIMEOUT_MILLIS));

        assertTrue(delegateFuture.isDone());
        assertThat(
            expectThrows(ExecutionException.class, ElasticsearchTimeoutException.class, delegateFuture::get).getMessage(),
            Matchers.startsWith("timed out after")
        );

        final var released = new AtomicBoolean();
        final var releasable = Releasables.assertOnce(() -> assertTrue(released.compareAndSet(false, true)));
        timeoutListener.onResponse(releasable);
        assertTrue(released.get());
    }

    public void testConcurrency() {
        final var threadPool = new TestThreadPool("test");
        try {
            final var delegateFuture = new PlainActionFuture<Releasable>();
            final var timeoutListener = createTimeoutListener(threadPool, delegateFuture);
            final var completionLatch = new CountDownLatch(1);
            try (var refs = new RefCountingRunnable(completionLatch::countDown)) {
                for (int i = between(0, 10); i > 0; i--) {
                    final var ref = refs.acquire();
                    threadPool.schedule(() -> {
                        if (randomBoolean()) {
                            timeoutListener.onResponse(ref);
                        } else {
                            try (ref) {
                                timeoutListener.onFailure(new ElasticsearchException("simulated"));
                            }
                        }
                    },
                        TimeValue.timeValueMillis(between(1, 20)),
                        threadPool.executor(randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC))
                    );
                }
            }

            try {
                FutureUtils.get(delegateFuture, 10, TimeUnit.SECONDS).close();
            } catch (ElasticsearchException e) {
                assertTrue(e instanceof ElasticsearchTimeoutException || e.getMessage().equals("simulated"));
            }

            safeAwait(completionLatch);
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static final int MAX_TIMEOUT_MILLIS = 20;

    private static TimeoutReleasableListener createTimeoutListener(ThreadPool threadPool, ActionListener<Releasable> listener) {
        return TimeoutReleasableListener.create(
            ActionListener.assertOnce(listener),
            TimeValue.timeValueMillis(between(1, MAX_TIMEOUT_MILLIS)),
            threadPool,
            threadPool.executor(randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC))
        );
    }
}
