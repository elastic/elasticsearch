/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

public class ThrottledIteratorTests extends ESTestCase {
    private static final String CONSTRAINED = "constrained";
    private static final String RELAXED = "relaxed";

    public void testConcurrency() throws InterruptedException {
        final var maxConstrainedThreads = between(1, 3);
        final var maxRelaxedThreads = between(1, 100);
        final var constrainedQueue = between(3, 6);
        final var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, CONSTRAINED, maxConstrainedThreads, constrainedQueue, CONSTRAINED, false),
            new ScalingExecutorBuilder(RELAXED, 1, maxRelaxedThreads, TimeValue.timeValueSeconds(30), true)
        );
        try {
            final var items = between(1, 10000); // large enough that inadvertent recursion will trigger a StackOverflowError
            final var itemStartLatch = new CountDownLatch(items);
            final var completedItems = new AtomicInteger();
            final var maxConcurrency = between(1, (constrainedQueue + maxConstrainedThreads) * 2);
            final var itemPermits = new Semaphore(maxConcurrency);
            final var completionLatch = new CountDownLatch(1);
            final BooleanSupplier forkSupplier = randomFrom(
                () -> false,
                ESTestCase::randomBoolean,
                LuceneTestCase::rarely,
                LuceneTestCase::usually,
                () -> true
            );
            final var blockPermits = new Semaphore(between(0, Math.min(maxRelaxedThreads, maxConcurrency) - 1));

            ThrottledIterator.run(IntStream.range(0, items).boxed().iterator(), (releasable, item) -> {
                try (var refs = new RefCountingRunnable(releasable::close)) {
                    assertTrue(itemPermits.tryAcquire());
                    if (forkSupplier.getAsBoolean()) {
                        var ref = refs.acquire();
                        final var executor = randomFrom(CONSTRAINED, RELAXED);
                        threadPool.executor(executor).execute(new AbstractRunnable() {

                            @Override
                            public void onRejection(Exception e) {
                                assertEquals(CONSTRAINED, executor);
                                itemStartLatch.countDown();
                            }

                            @Override
                            protected void doRun() {
                                itemStartLatch.countDown();
                                if (RELAXED.equals(executor) && randomBoolean() && blockPermits.tryAcquire()) {
                                    // simulate at most (maxConcurrency-1) long-running operations, to demonstrate that they don't
                                    // hold up the processing of the other operations
                                    try {
                                        assertTrue(itemStartLatch.await(30, TimeUnit.SECONDS));
                                    } catch (InterruptedException e) {
                                        throw new AssertionError("unexpected", e);
                                    } finally {
                                        blockPermits.release();
                                    }
                                }
                            }

                            @Override
                            public void onAfter() {
                                itemPermits.release();
                                ref.close();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                throw new AssertionError("unexpected", e);
                            }
                        });
                    } else {
                        itemStartLatch.countDown();
                        itemPermits.release();
                    }
                }
            }, maxConcurrency, completedItems::incrementAndGet, completionLatch::countDown);

            assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
            assertEquals(items, completedItems.get());
            assertTrue(itemPermits.tryAcquire(maxConcurrency));
            assertTrue(itemStartLatch.await(0, TimeUnit.SECONDS));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }
}
