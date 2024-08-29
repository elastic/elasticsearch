/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionHandler;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.hamcrest.Matcher;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.elasticsearch.threadpool.ThreadPool.getMaxSnapshotThreadPoolSize;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ScalingThreadPoolTests extends ESThreadPoolTestCase {

    public void testScalingThreadPoolConfiguration() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final Settings.Builder builder = Settings.builder();

        final int core;
        if (randomBoolean()) {
            core = randomIntBetween(0, 8);
            builder.put("thread_pool." + threadPoolName + ".core", core);
        } else {
            core = "generic".equals(threadPoolName) ? 4 : 1; // the defaults
        }

        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        final int maxBasedOnNumberOfProcessors;
        final int processors;
        if (randomBoolean()) {
            processors = randomIntBetween(1, availableProcessors);
            maxBasedOnNumberOfProcessors = expectedSize(threadPoolName, processors);
            builder.put("node.processors", processors);
        } else {
            maxBasedOnNumberOfProcessors = expectedSize(threadPoolName, availableProcessors);
            processors = availableProcessors;
        }

        final int expectedMax;
        if (maxBasedOnNumberOfProcessors < core || randomBoolean()) {
            expectedMax = randomIntBetween(Math.max(1, core), 16);
            builder.put("thread_pool." + threadPoolName + ".max", expectedMax);
        } else {
            expectedMax = threadPoolName.equals(ThreadPool.Names.SNAPSHOT)
                ? getMaxSnapshotThreadPoolSize(processors)
                : maxBasedOnNumberOfProcessors;
        }

        final long keepAlive;
        if (randomBoolean()) {
            keepAlive = randomIntBetween(1, 300);
            builder.put("thread_pool." + threadPoolName + ".keep_alive", keepAlive + "s");
        } else {
            keepAlive = "generic".equals(threadPoolName) || ThreadPool.Names.SNAPSHOT_META.equals(threadPoolName) ? 30 : 300; // the
                                                                                                                              // defaults
        }

        runScalingThreadPoolTest(builder.build(), (clusterSettings, threadPool) -> {
            final Executor executor = threadPool.executor(threadPoolName);
            assertThat(executor, instanceOf(EsThreadPoolExecutor.class));
            final EsThreadPoolExecutor esThreadPoolExecutor = (EsThreadPoolExecutor) executor;
            final ThreadPool.Info info = info(threadPool, threadPoolName);

            assertThat(info.getName(), equalTo(threadPoolName));
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.SCALING));

            assertThat(info.getKeepAlive().seconds(), equalTo(keepAlive));
            assertThat(esThreadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS), equalTo(keepAlive));

            assertNull(info.getQueueSize());
            assertThat(esThreadPoolExecutor.getQueue().remainingCapacity(), equalTo(Integer.MAX_VALUE));

            assertThat(info.getMin(), equalTo(core));
            assertThat(esThreadPoolExecutor.getCorePoolSize(), equalTo(core));
            assertThat(info.getMax(), equalTo(expectedMax));
            assertThat(esThreadPoolExecutor.getMaximumPoolSize(), equalTo(expectedMax));
        });

    }

    private int expectedSize(final String threadPoolName, final int numberOfProcessors) {
        final Map<String, Function<Integer, Integer>> sizes = new HashMap<>();
        sizes.put(ThreadPool.Names.GENERIC, n -> ThreadPool.boundedBy(4 * n, 128, 512));
        sizes.put(ThreadPool.Names.MANAGEMENT, n -> ThreadPool.boundedBy(n, 1, 5));
        sizes.put(ThreadPool.Names.FLUSH, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.REFRESH, ThreadPool::halfAllocatedProcessorsMaxTen);
        sizes.put(ThreadPool.Names.WARMER, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT_META, n -> Math.min(n * 3, 50));
        sizes.put(ThreadPool.Names.FETCH_SHARD_STARTED, ThreadPool::twiceAllocatedProcessors);
        sizes.put(ThreadPool.Names.FETCH_SHARD_STORE, ThreadPool::twiceAllocatedProcessors);
        return sizes.get(threadPoolName).apply(numberOfProcessors);
    }

    public void testScalingThreadPoolIsBounded() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final int size = randomIntBetween(32, 512);
        final Settings settings = Settings.builder().put("thread_pool." + threadPoolName + ".max", size).build();
        runScalingThreadPoolTest(settings, (clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final int numberOfTasks = 2 * size;
            final CountDownLatch taskLatch = new CountDownLatch(numberOfTasks);
            for (int i = 0; i < numberOfTasks; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            final ThreadPoolStats.Stats stats = stats(threadPool, threadPoolName);
            assertThat(stats.queue(), equalTo(numberOfTasks - size));
            assertThat(stats.largest(), equalTo(size));
            latch.countDown();
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testScalingThreadPoolThreadsAreTerminatedAfterKeepAlive() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final int min = "generic".equals(threadPoolName) ? 4 : 1;
        final Settings settings = Settings.builder()
            .put("thread_pool." + threadPoolName + ".max", 128)
            .put("thread_pool." + threadPoolName + ".keep_alive", "1ms")
            .build();
        runScalingThreadPoolTest(settings, ((clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch taskLatch = new CountDownLatch(128);
            for (int i = 0; i < 128; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            int threads = stats(threadPool, threadPoolName).threads();
            assertEquals(128, threads);
            latch.countDown();
            // this while loop is the core of this test; if threads
            // are correctly idled down by the pool, the number of
            // threads in the pool will drop to the min for the pool
            // but if threads are not correctly idled down by the pool,
            // this test will just timeout waiting for them to idle
            // down
            do {
                spinForAtLeastOneMillisecond();
            } while (stats(threadPool, threadPoolName).threads() > min);
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void testScalingThreadPoolRejectAfterShutdown() throws Exception {
        final boolean rejectAfterShutdown = randomBoolean();
        final int min = randomIntBetween(1, 4);
        final int max = randomIntBetween(min, 16);

        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling(
            getTestName().toLowerCase(Locale.ROOT),
            min,
            max,
            randomLongBetween(0, 100),
            TimeUnit.MILLISECONDS,
            rejectAfterShutdown,
            EsExecutors.daemonThreadFactory(getTestName().toLowerCase(Locale.ROOT)),
            new ThreadContext(Settings.EMPTY)
        );
        try {
            final AtomicLong executed = new AtomicLong();
            final AtomicLong rejected = new AtomicLong();
            final AtomicLong failed = new AtomicLong();

            final CountDownLatch latch = new CountDownLatch(max);
            final CountDownLatch block = new CountDownLatch(1);
            for (int i = 0; i < max; i++) {
                execute(scalingExecutor, () -> {
                    try {
                        latch.countDown();
                        block.await();
                    } catch (InterruptedException e) {
                        fail(e.toString());
                    }
                }, executed, rejected, failed);
            }
            latch.await();

            assertThat(scalingExecutor.getCompletedTaskCount(), equalTo(0L));
            assertThat(scalingExecutor.getActiveCount(), equalTo(max));
            assertThat(scalingExecutor.getQueue().size(), equalTo(0));

            final int queued = randomIntBetween(1, 100);
            for (int i = 0; i < queued; i++) {
                execute(scalingExecutor, () -> {}, executed, rejected, failed);
            }

            assertThat(scalingExecutor.getCompletedTaskCount(), equalTo(0L));
            assertThat(scalingExecutor.getActiveCount(), equalTo(max));
            assertThat(scalingExecutor.getQueue().size(), equalTo(queued));

            scalingExecutor.shutdown();

            final int queuedAfterShutdown = randomIntBetween(1, 100);
            for (int i = 0; i < queuedAfterShutdown; i++) {
                execute(scalingExecutor, () -> {}, executed, rejected, failed);
            }
            assertThat(scalingExecutor.getQueue().size(), rejectAfterShutdown ? equalTo(queued) : equalTo(queued + queuedAfterShutdown));

            block.countDown();

            assertBusy(() -> assertTrue(scalingExecutor.isTerminated()));
            assertThat(scalingExecutor.getActiveCount(), equalTo(0));
            assertThat(scalingExecutor.getQueue().size(), equalTo(0));
            assertThat(failed.get(), equalTo(0L));

            final Matcher<Long> executionsMatcher = rejectAfterShutdown
                ? equalTo((long) max + queued)
                : allOf(greaterThanOrEqualTo((long) max + queued), lessThanOrEqualTo((long) max + queued + queuedAfterShutdown));
            assertThat(scalingExecutor.getCompletedTaskCount(), executionsMatcher);
            assertThat(executed.get(), executionsMatcher);

            final EsRejectedExecutionHandler handler = (EsRejectedExecutionHandler) scalingExecutor.getRejectedExecutionHandler();
            Matcher<Long> rejectionsMatcher = rejectAfterShutdown ? equalTo((long) queuedAfterShutdown) : equalTo(0L);
            assertThat(handler.rejected(), rejectionsMatcher);
            assertThat(rejected.get(), rejectionsMatcher);

            final int queuedAfterTermination = randomIntBetween(1, 100);
            for (int i = 0; i < queuedAfterTermination; i++) {
                execute(scalingExecutor, () -> {}, executed, rejected, failed);
            }

            assertThat(scalingExecutor.getCompletedTaskCount(), executionsMatcher);
            assertThat(executed.get(), executionsMatcher);

            rejectionsMatcher = rejectAfterShutdown ? equalTo((long) queuedAfterShutdown + queuedAfterTermination) : equalTo(0L);
            assertThat(handler.rejected(), rejectionsMatcher);
            assertThat(rejected.get(), rejectionsMatcher);

            assertThat(scalingExecutor.getQueue().size(), rejectAfterShutdown ? equalTo(0) : equalTo(queuedAfterTermination));
            assertThat(failed.get(), equalTo(0L));

            if (rejectAfterShutdown) {
                final EsRejectedExecutionException exception = expectThrows(
                    EsRejectedExecutionException.class,
                    () -> scalingExecutor.execute(() -> {
                        throw new AssertionError("should be rejected");
                    })
                );
                assertThat(exception.getLocalizedMessage(), allOf(containsString("rejected execution of "), containsString("(shutdown)")));
                assertThat(exception.isExecutorShutdown(), equalTo(true));
            }

        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testScalingThreadPoolRejectDuringShutdown() throws Exception {
        final int min = 1;
        final int max = randomIntBetween(min, 3);

        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling(
            getTestName().toLowerCase(Locale.ROOT),
            min,
            max,
            randomLongBetween(0, 100),
            TimeUnit.MILLISECONDS,
            true,
            EsExecutors.daemonThreadFactory(getTestName().toLowerCase(Locale.ROOT)),
            new ThreadContext(Settings.EMPTY)
        );
        try {
            final AtomicLong executed = new AtomicLong();
            final AtomicLong rejected = new AtomicLong();
            final AtomicLong failed = new AtomicLong();

            final CountDownLatch latch = new CountDownLatch(max);
            final CountDownLatch block = new CountDownLatch(1);
            for (int i = 0; i < max; i++) {
                execute(scalingExecutor, () -> {
                    try {
                        latch.countDown();
                        block.await();
                    } catch (InterruptedException e) {
                        fail(e.toString());
                    }
                }, executed, rejected, failed);
            }
            latch.await();

            assertThat(scalingExecutor.getCompletedTaskCount(), equalTo(0L));
            assertThat(scalingExecutor.getActiveCount(), equalTo(max));
            assertThat(scalingExecutor.getQueue().size(), equalTo(0));

            final CyclicBarrier barrier = new CyclicBarrier(randomIntBetween(1, 5) + 1);
            final Thread[] threads = new Thread[barrier.getParties()];

            for (int t = 0; t < barrier.getParties(); t++) {
                if (t == 0) {
                    threads[t] = new Thread(() -> {
                        try {
                            safeAwait(barrier);
                            scalingExecutor.shutdown();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                } else {
                    threads[t] = new Thread(() -> {
                        try {
                            safeAwait(barrier);
                            execute(scalingExecutor, () -> {}, executed, rejected, failed);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                }
                threads[t].start();
            }
            block.countDown();

            for (Thread thread : threads) {
                thread.join();
            }

            assertBusy(() -> assertTrue(scalingExecutor.isTerminated()));
            assertThat(scalingExecutor.getCompletedTaskCount(), greaterThanOrEqualTo((long) max));
            final long maxCompletedTasks = (long) max + barrier.getParties() - 1L;
            assertThat(scalingExecutor.getCompletedTaskCount(), lessThanOrEqualTo(maxCompletedTasks));
            assertThat(scalingExecutor.getCompletedTaskCount() + rejected.get(), equalTo(maxCompletedTasks));
            assertThat(scalingExecutor.getQueue().size(), equalTo(0));
            assertThat(scalingExecutor.getActiveCount(), equalTo(0));

        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    private static void execute(
        final Executor executor,
        final CheckedRunnable<Exception> runnable,
        final AtomicLong executed,
        final AtomicLong rejected,
        final AtomicLong failed
    ) {
        if (randomBoolean()) {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    runnable.run();
                    executed.incrementAndGet();
                }

                @Override
                public void onFailure(Exception e) {
                    failed.incrementAndGet();
                }

                @Override
                public void onRejection(Exception e) {
                    rejected.incrementAndGet();
                }
            });
        } else {
            try {
                executor.execute(() -> {
                    try {
                        runnable.run();
                        executed.incrementAndGet();
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    }
                });
            } catch (EsRejectedExecutionException e) {
                rejected.incrementAndGet();
            } catch (Exception e) {
                failed.incrementAndGet();
            }
        }
    }

    public void runScalingThreadPoolTest(final Settings settings, final BiConsumer<ClusterSettings, ThreadPool> consumer)
        throws InterruptedException {
        ThreadPool threadPool = null;
        try {
            final String test = Thread.currentThread().getStackTrace()[2].getMethodName();
            final Settings nodeSettings = Settings.builder().put(settings).put("node.name", test).build();
            threadPool = new ThreadPool(nodeSettings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
            final ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            consumer.accept(clusterSettings, threadPool);
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }
}
