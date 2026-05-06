/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.instanceOf;

public class ThrottledIteratorTests extends ESTestCase {
    private static final String CONSTRAINED = "constrained";
    private static final String RELAXED = "relaxed";

    public void testConcurrency() throws InterruptedException {
        final var maxConstrainedThreads = between(1, 3);
        final var maxRelaxedThreads = between(1, 100);
        final var constrainedQueue = between(3, 6);
        final var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                CONSTRAINED,
                maxConstrainedThreads,
                constrainedQueue,
                CONSTRAINED,
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            ),
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

            final var iterator = new SerialAccessAssertingIterator<>(IntStream.range(0, items).boxed().iterator());
            final BiConsumer<Releasable, Integer> itemConsumer = (releasable, item) -> {
                try (var refs = new RefCountingRunnable(() -> {
                    completedItems.incrementAndGet();
                    releasable.close();
                })) {
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
                                        fail(e);
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
                                fail(e);
                            }
                        });
                    } else {
                        itemStartLatch.countDown();
                        itemPermits.release();
                    }
                }
            };
            if (randomBoolean()) {
                ThrottledIterator.run(
                    iterator,
                    itemConsumer,
                    maxConcurrency,
                    completionLatch::countDown,
                    threadPool.executor(RELAXED),
                    ESTestCase::fail
                );
            } else {
                ThrottledIterator.run(iterator, itemConsumer, maxConcurrency, completionLatch::countDown);
            }

            assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
            assertEquals(items, completedItems.get());
            assertTrue(itemPermits.tryAcquire(maxConcurrency));
            assertTrue(itemStartLatch.await(0, TimeUnit.SECONDS));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testRunContinuesOnContinuationExecutorAfterPermitRelease() throws Exception {
        final var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, CONSTRAINED, 1, 10, CONSTRAINED, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
        );
        try {
            final List<String> processingThreads = new CopyOnWriteArrayList<>();
            final AtomicReference<Releasable> firstItemReleasable = new AtomicReference<>();
            final var firstItemSeen = new CountDownLatch(1);
            final var completionLatch = new CountDownLatch(1);
            final var completionCalls = new AtomicInteger();
            final var callerThreadName = Thread.currentThread().getName();
            final var releaseThreadName = "permit-release-thread";

            ThrottledIterator.run(List.of(0, 1, 2).iterator(), (releasable, item) -> {
                processingThreads.add(Thread.currentThread().getName());
                if (item == 0) {
                    firstItemReleasable.set(releasable);
                    firstItemSeen.countDown();
                } else {
                    releasable.close();
                }
            }, 1, () -> {
                completionCalls.incrementAndGet();
                completionLatch.countDown();
            }, threadPool.executor(CONSTRAINED), e -> {}); // Continuation executor

            assertTrue(firstItemSeen.await(10, TimeUnit.SECONDS));
            assertEquals(List.of(callerThreadName), processingThreads);

            Thread releaser = new Thread(() -> firstItemReleasable.get().close(), releaseThreadName);
            releaser.start();
            releaser.join();

            assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
            assertEquals(1, completionCalls.get());
            assertEquals(3, processingThreads.size());
            assertEquals(callerThreadName, processingThreads.get(0));
            assertTrue(processingThreads.get(1), processingThreads.get(1).contains("[constrained]"));
            assertTrue(processingThreads.get(2), processingThreads.get(2).contains("[constrained]"));
            assertNotEquals(releaseThreadName, processingThreads.get(1));
            assertNotEquals(releaseThreadName, processingThreads.get(2));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testIteratorExceptionPropagatesWithDirectExecutor() {
        final var completed = new AtomicBoolean();
        final var releasableRef = new AtomicReference<Releasable>();
        final var processedItems = new CopyOnWriteArrayList<Integer>();

        Iterator<Integer> throwingIterator = new Iterator<>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < 3;
            }

            @Override
            public Integer next() {
                int current = index++;
                if (current == 1) {
                    throw new RuntimeException("iterator failure on item 1");
                }
                return current;
            }
        };

        ThrottledIterator.run(throwingIterator, (releasable, item) -> {
            processedItems.add(item);
            if (item == 0) {
                releasableRef.set(releasable);
            } else {
                releasable.close();
            }
        }, 1, () -> completed.set(true));

        assertEquals(List.of(0), processedItems);
        assertFalse(completed.get());

        RuntimeException e = expectThrows(RuntimeException.class, () -> releasableRef.get().close());
        assertEquals("iterator failure on item 1", e.getMessage());

        assertTrue(completed.get());
        assertEquals(List.of(0), processedItems);
    }

    public void testExecutorRejectionStillCompletes() {
        final var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, CONSTRAINED, 1, 10, CONSTRAINED, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
        );
        final var continuationExecutor = threadPool.executor(CONSTRAINED);
        assertTrue(ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS));

        final var completed = new AtomicBoolean();
        final var releasableRef = new AtomicReference<Releasable>();
        final var processedItems = new CopyOnWriteArrayList<Integer>();
        final var surfacedFailure = new AtomicReference<Exception>();

        ThrottledIterator.run(List.of(0, 1, 2).iterator(), (releasable, item) -> {
            processedItems.add(item);
            if (item == 0) {
                releasableRef.set(releasable);
            } else {
                releasable.close();
            }
        }, 1, () -> completed.set(true), continuationExecutor, e -> surfacedFailure.compareAndSet(null, e));

        assertEquals(List.of(0), processedItems);
        assertFalse(completed.get());

        releasableRef.get().close();

        assertTrue(completed.get());
        assertEquals(List.of(0), processedItems);
        assertNotNull("rejection must be surfaced to the hook", surfacedFailure.get());
        assertThat(surfacedFailure.get(), instanceOf(EsRejectedExecutionException.class));
    }

    private static class SerialAccessAssertingIterator<T> implements Iterator<T> {

        private final AtomicBoolean isAccessing = new AtomicBoolean();
        private final Iterator<T> delegate;

        private SerialAccessAssertingIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try (var ignored = withSerialAccessCheck()) {
                return delegate.hasNext();
            }
        }

        @Override
        public T next() {
            try (var ignored = withSerialAccessCheck()) {
                return delegate.next();
            }
        }

        @Override
        public void remove() {
            assert false : "not called";
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            assert false : "not called";
        }

        private Releasable withSerialAccessCheck() {
            assertTrue(isAccessing.compareAndSet(false, true));
            return () -> assertTrue(isAccessing.compareAndSet(true, false));
        }
    }
}
