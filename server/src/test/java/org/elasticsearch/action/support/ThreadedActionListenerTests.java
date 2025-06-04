/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadedActionListenerTests extends ESTestCase {

    public void testRejectionHandling() throws InterruptedException {
        final var listenerCount = between(1, 1000);
        final var countdownLatch = new CountDownLatch(listenerCount);
        final var threadPool = new TestThreadPool(
            "test",
            Settings.EMPTY,
            new FixedExecutorBuilder(
                Settings.EMPTY,
                "fixed-bounded-queue",
                between(1, 3),
                10,
                "fbq",
                randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
            ),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                "fixed-unbounded-queue",
                between(1, 3),
                -1,
                "fnq",
                randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
            ),
            new ScalingExecutorBuilder("scaling-drop-if-shutdown", between(1, 3), between(3, 5), TimeValue.timeValueSeconds(1), false),
            new ScalingExecutorBuilder("scaling-reject-if-shutdown", between(1, 3), between(3, 5), TimeValue.timeValueSeconds(1), true)
        );
        final var closeFlag = new AtomicBoolean();
        try {
            final var pools = randomNonEmptySubsetOf(
                List.of("fixed-bounded-queue", "fixed-unbounded-queue", "scaling-drop-if-shutdown", "scaling-reject-if-shutdown")
            );
            final var shutdownUnsafePools = Set.of("fixed-bounded-queue", "scaling-drop-if-shutdown");

            threadPool.generic().execute(() -> {
                for (int i = 0; i < listenerCount; i++) {
                    final var pool = randomFrom(pools);
                    final var listener = new ThreadedActionListener<Void>(
                        threadPool.executor(pool),
                        (pool.equals("fixed-bounded-queue") || pool.startsWith("scaling")) && rarely(),
                        ActionListener.runAfter(new ActionListener<>() {
                            @Override
                            public void onResponse(Void ignored) {}

                            @Override
                            public void onFailure(Exception e) {
                                assertNull(e.getCause());
                                if (e instanceof EsRejectedExecutionException esRejectedExecutionException) {
                                    assertTrue(esRejectedExecutionException.isExecutorShutdown());
                                    if (e.getSuppressed().length == 0) {
                                        return;
                                    }
                                    assertEquals(1, e.getSuppressed().length);
                                    if (e.getSuppressed()[0] instanceof ElasticsearchException elasticsearchException) {
                                        e = elasticsearchException;
                                        assertNull(e.getCause());
                                    } else {
                                        fail(e);
                                    }
                                }

                                if (e instanceof ElasticsearchException) {
                                    assertEquals("simulated", e.getMessage());
                                    assertEquals(0, e.getSuppressed().length);
                                } else {
                                    fail(e);
                                }

                            }
                        }, countdownLatch::countDown)
                    );
                    synchronized (closeFlag) {
                        if (closeFlag.get() && shutdownUnsafePools.contains(pool)) {
                            // closing, so tasks submitted to this pool may just be dropped
                            countdownLatch.countDown();
                        } else if (randomBoolean()) {
                            listener.onResponse(null);
                        } else {
                            listener.onFailure(new ElasticsearchException("simulated"));
                        }
                    }
                    Thread.yield();
                }
            });
        } finally {
            synchronized (closeFlag) {
                assertTrue(closeFlag.compareAndSet(false, true));
                threadPool.shutdown();
            }
            assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertTrue(countdownLatch.await(10, TimeUnit.SECONDS));
    }

    public void testToString() {
        var deterministicTaskQueue = new DeterministicTaskQueue();

        assertEquals(
            "ThreadedActionListener[DeterministicTaskQueue/forkingExecutor/NoopActionListener]",
            new ThreadedActionListener<Void>(deterministicTaskQueue.getThreadPool().generic(), randomBoolean(), ActionListener.noop())
                .toString()
        );

        assertEquals(
            "ThreadedActionListener[DeterministicTaskQueue/forkingExecutor/NoopActionListener]/onResponse",
            safeAwait(listener -> new ThreadedActionListener<Void>(deterministicTaskQueue.getThreadPool(s -> {
                listener.onResponse(s.toString());
                return s;
            }).generic(), randomBoolean(), ActionListener.noop()).onResponse(null))
        );

        assertEquals(
            "ThreadedActionListener[DeterministicTaskQueue/forkingExecutor/NoopActionListener]/onFailure",
            safeAwait(listener -> new ThreadedActionListener<Void>(deterministicTaskQueue.getThreadPool(s -> {
                listener.onResponse(s.toString());
                return s;
            }).generic(), randomBoolean(), ActionListener.noop()).onFailure(new ElasticsearchException("test")))
        );
    }

}
