/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DEFAULT;
import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DO_NOT_TRACK;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for EsExecutors and its components like EsAbortPolicy.
 */
public class EsExecutorsTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private TimeUnit randomTimeUnit() {
        return TimeUnit.values()[between(0, TimeUnit.values().length - 1)];
    }

    private String getName() {
        return getClass().getName() + "/" + getTestName();
    }

    public void testFixedForcedExecution() throws Exception {
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            getName(),
            1,
            1,
            EsExecutors.daemonThreadFactory("test"),
            threadContext,
            randomFrom(DEFAULT, DO_NOT_TRACK)
        );
        final CountDownLatch wait = new CountDownLatch(1);

        final CountDownLatch exec1Wait = new CountDownLatch(1);
        final AtomicBoolean executed1 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executed1.set(true);
                exec1Wait.countDown();
            }
        });

        final CountDownLatch exec2Wait = new CountDownLatch(1);
        final AtomicBoolean executed2 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                executed2.set(true);
                exec2Wait.countDown();
            }
        });

        final AtomicBoolean executed3 = new AtomicBoolean();
        final CountDownLatch exec3Wait = new CountDownLatch(1);
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                executed3.set(true);
                exec3Wait.countDown();
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        wait.countDown();

        exec1Wait.await();
        exec2Wait.await();
        exec3Wait.await();

        assertThat(executed1.get(), equalTo(true));
        assertThat(executed2.get(), equalTo(true));
        assertThat(executed3.get(), equalTo(true));

        executor.shutdownNow();
    }

    public void testFixedRejected() throws Exception {
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            getName(),
            1,
            1,
            EsExecutors.daemonThreadFactory("test"),
            threadContext,
            randomFrom(DEFAULT, DO_NOT_TRACK)
        );
        final CountDownLatch wait = new CountDownLatch(1);

        final CountDownLatch exec1Wait = new CountDownLatch(1);
        final AtomicBoolean executed1 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executed1.set(true);
                exec1Wait.countDown();
            }
        });

        final CountDownLatch exec2Wait = new CountDownLatch(1);
        final AtomicBoolean executed2 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                executed2.set(true);
                exec2Wait.countDown();
            }
        });

        final AtomicBoolean executed3 = new AtomicBoolean();
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    executed3.set(true);
                }
            });
            fail("should be rejected...");
        } catch (EsRejectedExecutionException e) {
            // all is well
        }

        wait.countDown();

        exec1Wait.await();
        exec2Wait.await();

        assertThat(executed1.get(), equalTo(true));
        assertThat(executed2.get(), equalTo(true));
        assertThat(executed3.get(), equalTo(false));

        terminate(executor);
    }

    public void testScaleUp() {
        final int min = between(1, 3);
        final int max = between(min + 1, 6);
        final CyclicBarrier barrier = new CyclicBarrier(max + 1);

        ThreadPoolExecutor pool = EsExecutors.newScaling(
            getClass().getName() + "/" + getTestName(),
            min,
            max,
            between(1, 100),
            randomTimeUnit(),
            randomBoolean(),
            EsExecutors.daemonThreadFactory("test"),
            threadContext
        );
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(() -> {
                latch.countDown();
                safeAwait(barrier);
                safeAwait(barrier);
            });

            // wait until thread executes this task
            // otherwise, a task might be queued
            safeAwait(latch);
        }

        safeAwait(barrier);
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        safeAwait(barrier);
        terminate(pool);
    }

    public void testScaleDown() throws Exception {
        final int min = between(1, 3);
        final int max = between(min + 1, 6);
        final CyclicBarrier barrier = new CyclicBarrier(max + 1);

        final ThreadPoolExecutor pool = EsExecutors.newScaling(
            getClass().getName() + "/" + getTestName(),
            min,
            max,
            between(1, 100),
            TimeUnit.MILLISECONDS,
            randomBoolean(),
            EsExecutors.daemonThreadFactory("test"),
            threadContext
        );
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(() -> {
                latch.countDown();
                safeAwait(barrier);
                safeAwait(barrier);
            });

            // wait until thread executes this task
            // otherwise, a task might be queued
            safeAwait(latch);
        }

        safeAwait(barrier);
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        safeAwait(barrier);
        assertBusy(() -> {
            assertThat("wrong active count", pool.getActiveCount(), equalTo(0));
            assertThat("idle threads didn't shrink below max. (" + pool.getPoolSize() + ")", pool.getPoolSize(), lessThan(max));
        });
        terminate(pool);
    }

    public void testRejectionMessageAndShuttingDownFlag() throws InterruptedException {
        int pool = between(1, 10);
        int queue = between(0, 100);
        int actions = queue + pool;
        final CountDownLatch latch = new CountDownLatch(1);
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            getName(),
            pool,
            queue,
            EsExecutors.daemonThreadFactory("dummy"),
            threadContext,
            randomFrom(DEFAULT, DO_NOT_TRACK)
        );
        try {
            for (int i = 0; i < actions; i++) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            try {
                executor.execute(new Runnable() {

                    @Override
                    public void run() {
                        // Doesn't matter is going to be rejected
                    }

                    @Override
                    public String toString() {
                        return "dummy runnable";
                    }
                });
                fail("Didn't get a rejection when we expected one.");
            } catch (EsRejectedExecutionException e) {
                assertFalse("Thread pool registering as terminated when it isn't", e.isExecutorShutdown());
                String message = e.getMessage();
                assertThat(message, containsString("dummy runnable"));
                assertThat(
                    message,
                    either(containsString("on EsThreadPoolExecutor[name = " + getName())).or(
                        containsString("on TaskExecutionTimeTrackingEsThreadPoolExecutor[name = " + getName())
                    )
                );
                assertThat(message, containsString("queue capacity = " + queue));
                assertThat(message, containsString("[Running"));
                /*
                 * While you'd expect all threads in the pool to be active when the queue gets long enough to cause rejections this isn't
                 * always the case. Sometimes you'll see "active threads = <pool - 1>", presumably because one of those threads has finished
                 * its current task but has yet to pick up another task. You too can reproduce this by adding the @Repeat annotation to this
                 * test with something like 10000 iterations. I suspect you could see "active threads = <any natural number <= to pool>". So
                 * that is what we assert.
                 */
                @SuppressWarnings({ "rawtypes", "unchecked" })
                Matcher<String>[] activeThreads = new Matcher[pool + 1];
                for (int p = 0; p <= pool; p++) {
                    activeThreads[p] = containsString("active threads = " + p);
                }
                assertThat(message, anyOf(activeThreads));
                assertThat(message, containsString("queued tasks = " + queue));
                assertThat(message, containsString("completed tasks = 0"));
            }
        } finally {
            latch.countDown();
            terminate(executor);
        }
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    // Doesn't matter is going to be rejected
                }

                @Override
                public String toString() {
                    return "dummy runnable";
                }
            });
            fail("Didn't get a rejection when we expected one.");
        } catch (EsRejectedExecutionException e) {
            assertTrue("Thread pool not registering as terminated when it is", e.isExecutorShutdown());
            String message = e.getMessage();
            assertThat(message, containsString("dummy runnable"));
            assertThat(
                message,
                either(containsString("on EsThreadPoolExecutor[name = " + getName())).or(
                    containsString("on TaskExecutionTimeTrackingEsThreadPoolExecutor[name = " + getName())
                )
            );
            assertThat(message, containsString("queue capacity = " + queue));
            assertThat(message, containsString("[Terminated"));
            assertThat(message, containsString("active threads = 0"));
            assertThat(message, containsString("queued tasks = 0"));
            assertThat(message, containsString("completed tasks = " + actions));
        }
    }

    public void testInheritContext() throws InterruptedException {
        int pool = between(1, 10);
        int queue = between(0, 100);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);

        threadContext.putHeader("foo", "bar");
        final Integer one = Integer.valueOf(1);
        threadContext.putTransient("foo", one);
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            getName(),
            pool,
            queue,
            EsExecutors.daemonThreadFactory("dummy"),
            threadContext,
            randomFrom(DEFAULT, DO_NOT_TRACK)
        );
        try {
            executor.execute(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                assertEquals(threadContext.getHeader("foo"), "bar");
                assertSame(threadContext.getTransient("foo"), one);
                assertNull(threadContext.getHeader("bar"));
                assertNull(threadContext.getTransient("bar"));
                executed.countDown();
            });
            threadContext.putTransient("bar", "boom");
            threadContext.putHeader("bar", "boom");
            latch.countDown();
            executed.await();

        } finally {
            latch.countDown();
            terminate(executor);
        }
    }

    public void testGetTasks() throws InterruptedException {
        int pool = between(1, 10);
        int queue = between(0, 100);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            getName(),
            pool,
            queue,
            EsExecutors.daemonThreadFactory("dummy"),
            threadContext,
            randomFrom(DEFAULT, DO_NOT_TRACK)
        );
        try {
            Runnable r = () -> {
                latch.countDown();
                try {
                    executed.await();
                } catch (InterruptedException e) {
                    fail();
                }
            };
            executor.execute(r);
            latch.await();
            executor.getTasks().forEach((runnable) -> assertSame(runnable, r));
            executed.countDown();

        } finally {
            latch.countDown();
            terminate(executor);
        }
    }

    public void testNodeProcessorsBound() {
        final Setting<Processors> processorsSetting = EsExecutors.NODE_PROCESSORS_SETTING;
        final int available = Runtime.getRuntime().availableProcessors();
        final double processors = randomDoubleBetween(available + Math.ulp(available), Float.MAX_VALUE, true);
        final Settings settings = Settings.builder().put(processorsSetting.getKey(), processors).build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processorsSetting.get(settings));
        final String expected = String.format(
            Locale.ROOT,
            "Failed to parse value [%s] for setting [%s] must be <= %d",
            processors,
            processorsSetting.getKey(),
            available
        );
        assertThat(e, hasToString(containsString(expected)));
    }

    public void testNodeProcessorsIsRoundedUpWhenUsingFloats() {
        assertThat(
            EsExecutors.allocatedProcessors(Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), Double.MIN_VALUE).build()),
            is(equalTo(1))
        );

        assertThat(
            EsExecutors.allocatedProcessors(Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 0.2).build()),
            is(equalTo(1))
        );

        assertThat(
            EsExecutors.allocatedProcessors(Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 1.2).build()),
            is(equalTo(2))
        );

        assertThat(
            EsExecutors.allocatedProcessors(
                Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), Runtime.getRuntime().availableProcessors()).build()
            ),
            is(equalTo(Runtime.getRuntime().availableProcessors()))
        );
    }

    public void testNodeProcessorsFloatValidation() {
        final Setting<Processors> processorsSetting = EsExecutors.NODE_PROCESSORS_SETTING;

        {
            final Settings settings = Settings.builder().put(processorsSetting.getKey(), 0.0).build();
            expectThrows(IllegalArgumentException.class, () -> processorsSetting.get(settings));
        }

        {
            final Settings settings = Settings.builder().put(processorsSetting.getKey(), Double.NaN).build();
            expectThrows(IllegalArgumentException.class, () -> processorsSetting.get(settings));
        }

        {
            final Settings settings = Settings.builder().put(processorsSetting.getKey(), Double.POSITIVE_INFINITY).build();
            expectThrows(IllegalArgumentException.class, () -> processorsSetting.get(settings));
        }

        {
            final Settings settings = Settings.builder().put(processorsSetting.getKey(), Double.NEGATIVE_INFINITY).build();
            expectThrows(IllegalArgumentException.class, () -> processorsSetting.get(settings));
        }

        {
            final Settings settings = Settings.builder().put(processorsSetting.getKey(), -1.5).build();
            expectThrows(IllegalArgumentException.class, () -> processorsSetting.get(settings));
        }
    }

    // This test must complete to ensure that our basic infrastructure is working as expected.
    // Specifically that ExecutorScalingQueue, which subclasses LinkedTransferQueue, correctly
    // tracks tasks submitted to the executor.
    public void testBasicTaskExecution() {
        final var executorService = EsExecutors.newScaling(
            "test",
            0,
            between(1, 5),
            60,
            TimeUnit.SECONDS,
            randomBoolean(),
            EsExecutors.daemonThreadFactory("test"),
            new ThreadContext(Settings.EMPTY)
        );
        try {
            final var countDownLatch = new CountDownLatch(between(1, 10));
            class TestTask extends AbstractRunnable {
                @Override
                protected void doRun() {
                    countDownLatch.countDown();
                    if (countDownLatch.getCount() > 0) {
                        executorService.execute(TestTask.this);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }

            executorService.execute(new TestTask());
            safeAwait(countDownLatch);
        } finally {
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        }
    }

    public void testScalingDropOnShutdown() {
        final var executor = EsExecutors.newScaling(
            getName(),
            0,
            between(1, 5),
            60,
            TimeUnit.SECONDS,
            false,
            EsExecutors.daemonThreadFactory(getName()),
            new ThreadContext(Settings.EMPTY)
        );
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        executor.execute(() -> fail("should not run")); // no-op
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail("should not call onFailure");
            }

            @Override
            protected void doRun() {
                fail("should not call doRun");
            }

            @Override
            public boolean isForceExecution() {
                return randomBoolean();
            }

            @Override
            public void onRejection(Exception e) {
                fail("should not call onRejection");
            }

            @Override
            public void onAfter() {
                fail("should not call onAfter");
            }
        });
    }

    public void testScalingRejectOnShutdown() {
        runRejectOnShutdownTest(
            EsExecutors.newScaling(
                getName(),
                0,
                between(1, 5),
                60,
                TimeUnit.SECONDS,
                true,
                EsExecutors.daemonThreadFactory(getName()),
                new ThreadContext(Settings.EMPTY)
            )
        );
    }

    public void testFixedBoundedRejectOnShutdown() {
        runRejectOnShutdownTest(
            EsExecutors.newFixed(
                getName(),
                between(1, 5),
                between(1, 5),
                EsExecutors.daemonThreadFactory(getName()),
                threadContext,
                randomFrom(DEFAULT, DO_NOT_TRACK)
            )
        );
    }

    public void testFixedUnboundedRejectOnShutdown() {
        runRejectOnShutdownTest(
            EsExecutors.newFixed(
                getName(),
                between(1, 5),
                -1,
                EsExecutors.daemonThreadFactory(getName()),
                threadContext,
                randomFrom(DEFAULT, DO_NOT_TRACK)
            )
        );
    }

    public void testParseExecutorName() throws InterruptedException {
        final var executorName = randomAlphaOfLength(10);
        final String nodeName = rarely() ? null : randomIdentifier();
        final ThreadFactory threadFactory;
        final boolean isSystem;
        if (nodeName == null) {
            isSystem = false;
            threadFactory = EsExecutors.daemonThreadFactory(Settings.EMPTY, executorName);
        } else if (randomBoolean()) {
            isSystem = false;
            threadFactory = EsExecutors.daemonThreadFactory(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
                executorName
            );
        } else {
            isSystem = randomBoolean();
            threadFactory = EsExecutors.daemonThreadFactory(nodeName, executorName, isSystem);
        }

        final var thread = threadFactory.newThread(() -> {});
        try {
            assertThat(EsExecutors.executorName(thread.getName()), equalTo(executorName));
            assertThat(EsExecutors.executorName(thread), equalTo(executorName));
            assertThat(EsExecutors.executorName("TEST-" + thread.getName()), is(nullValue()));
            assertThat(EsExecutors.executorName("LuceneTestCase" + thread.getName()), is(nullValue()));
            assertThat(EsExecutors.executorName("LuceneTestCase" + thread.getName()), is(nullValue()));
            assertThat(((EsExecutors.EsThread) thread).isSystem(), equalTo(isSystem));
        } finally {
            thread.join();
        }
    }

    public void testScalingWithTaskTimeTracking() {
        final int min = between(1, 3);
        final int max = between(min + 1, 6);

        {
            ThreadPoolExecutor pool = EsExecutors.newScaling(
                getClass().getName() + "/" + getTestName(),
                min,
                max,
                between(1, 100),
                randomTimeUnit(),
                randomBoolean(),
                EsExecutors.daemonThreadFactory("test"),
                threadContext,
                new EsExecutors.TaskTrackingConfig(randomBoolean(), randomDoubleBetween(0.01, 0.1, true))
            );
            assertThat(pool, instanceOf(TaskExecutionTimeTrackingEsThreadPoolExecutor.class));
        }

        {
            ThreadPoolExecutor pool = EsExecutors.newScaling(
                getClass().getName() + "/" + getTestName(),
                min,
                max,
                between(1, 100),
                randomTimeUnit(),
                randomBoolean(),
                EsExecutors.daemonThreadFactory("test"),
                threadContext
            );
            assertThat(pool, instanceOf(EsThreadPoolExecutor.class));
        }

        {
            ThreadPoolExecutor pool = EsExecutors.newScaling(
                getClass().getName() + "/" + getTestName(),
                min,
                max,
                between(1, 100),
                randomTimeUnit(),
                randomBoolean(),
                EsExecutors.daemonThreadFactory("test"),
                threadContext,
                DO_NOT_TRACK
            );
            assertThat(pool, instanceOf(EsThreadPoolExecutor.class));
        }
    }

    private static void runRejectOnShutdownTest(ExecutorService executor) {
        for (int i = between(0, 10); i > 0; i--) {
            final var delayMillis = between(0, 100);
            executor.execute(ActionRunnable.wrap(ActionListener.noop(), l -> safeSleep(delayMillis)));
        }
        try {
            executor.shutdown();
            assertShutdownAndRejectingTasks(executor);
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
        assertShutdownAndRejectingTasks(executor);
    }

    private static void assertShutdownAndRejectingTasks(Executor executor) {
        final var rejected = new AtomicBoolean();
        final var shouldBeRejected = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail("should not call onFailure");
            }

            @Override
            protected void doRun() {
                fail("should not call doRun");
            }

            @Override
            public boolean isForceExecution() {
                return randomBoolean();
            }

            @Override
            public void onRejection(Exception e) {
                assertTrue(asInstanceOf(EsRejectedExecutionException.class, e).isExecutorShutdown());
                assertTrue(rejected.compareAndSet(false, true));
            }
        };
        assertTrue(expectThrows(EsRejectedExecutionException.class, () -> executor.execute(shouldBeRejected::doRun)).isExecutorShutdown());
        executor.execute(shouldBeRejected);
        assertTrue(rejected.get());
    }

    public void testScalingWithEmptyCore() {
        testScalingWithEmptyCoreAndMaxSingleThread(
            EsExecutors.newScaling(
                getTestName(),
                0,
                1,
                0,
                TimeUnit.MILLISECONDS,
                true,
                EsExecutors.daemonThreadFactory(getTestName()),
                threadContext
            )
        );
    }

    public void testScalingWithEmptyCoreAndKeepAlive() {
        testScalingWithEmptyCoreAndMaxSingleThread(
            EsExecutors.newScaling(
                getTestName(),
                0,
                1,
                1,
                TimeUnit.MILLISECONDS,
                true,
                EsExecutors.daemonThreadFactory(getTestName()),
                threadContext
            )
        );
    }

    public void testScalingWithEmptyCoreAndLargerMaxSize() {
        testScalingWithEmptyCoreAndMaxMultipleThreads(
            EsExecutors.newScaling(
                getTestName(),
                0,
                between(2, 5),
                0,
                TimeUnit.MILLISECONDS,
                true,
                EsExecutors.daemonThreadFactory(getTestName()),
                threadContext
            )
        );
    }

    public void testScalingWithEmptyCoreAndKeepAliveAndLargerMaxSize() {
        testScalingWithEmptyCoreAndMaxMultipleThreads(
            EsExecutors.newScaling(
                getTestName(),
                0,
                between(2, 5),
                1,
                TimeUnit.MILLISECONDS,
                true,
                EsExecutors.daemonThreadFactory(getTestName()),
                threadContext
            )
        );
    }

    public void testScalingWithEmptyCoreAndWorkerPoolProbing() {
        // the executor is created directly here, newScaling doesn't use ExecutorScalingQueue & probing if max pool size = 1.
        testScalingWithEmptyCoreAndMaxSingleThread(
            new EsThreadPoolExecutor(
                getTestName(),
                0,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new EsExecutors.ExecutorScalingQueue<>(),
                EsExecutors.daemonThreadFactory(getTestName()),
                new EsExecutors.ForceQueuePolicy(true, true),
                threadContext
            )
        );
    }

    public void testScalingWithEmptyCoreAndKeepAliveAndWorkerPoolProbing() {
        // the executor is created directly here, newScaling doesn't use ExecutorScalingQueue & probing if max pool size = 1.
        testScalingWithEmptyCoreAndMaxSingleThread(
            new EsThreadPoolExecutor(
                getTestName(),
                0,
                1,
                1,
                TimeUnit.MILLISECONDS,
                new EsExecutors.ExecutorScalingQueue<>(),
                EsExecutors.daemonThreadFactory(getTestName()),
                new EsExecutors.ForceQueuePolicy(true, true),
                threadContext
            )
        );
    }

    private void testScalingWithEmptyCoreAndMaxSingleThread(EsThreadPoolExecutor testSubject) {
        try {
            final var keepAliveNanos = testSubject.getKeepAliveTime(TimeUnit.NANOSECONDS);

            class Task extends AbstractRunnable {
                private final CountDownLatch doneLatch;
                private int remaining;

                Task(int iterations, CountDownLatch doneLatch) {
                    this.remaining = iterations;
                    this.doneLatch = doneLatch;
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }

                @Override
                protected void doRun() {
                    if (--remaining == 0) {
                        doneLatch.countDown();
                    } else {
                        new Thread(() -> {
                            if (keepAliveNanos > 0) {
                                waitUntilKeepAliveTime(keepAliveNanos);
                            }
                            testSubject.execute(Task.this);
                        }).start();
                    }
                }
            }

            for (int i = 0; i < 20; i++) {
                final var doneLatch = new CountDownLatch(1);
                testSubject.execute(new Task(between(1, 500), doneLatch));
                safeAwait(doneLatch, TimeValue.ONE_MINUTE);
            }
        } finally {
            ThreadPool.terminate(testSubject, 1, TimeUnit.SECONDS);
        }
    }

    private void testScalingWithEmptyCoreAndMaxMultipleThreads(EsThreadPoolExecutor testSubject) {
        final var keepAliveNanos = testSubject.getKeepAliveTime(TimeUnit.NANOSECONDS);
        // Use max pool size with one additional scheduler task if a keep alive time is set.
        final var schedulerTasks = testSubject.getMaximumPoolSize() + (keepAliveNanos > 0 ? 1 : 0);

        class TaskScheduler {
            final SubscribableListener<Void> result = new SubscribableListener<>();
            final ExecutorService scheduler;
            final CyclicBarrier cyclicBarrier;
            final Semaphore taskCompletions;
            private int remaining;

            TaskScheduler(ExecutorService scheduler, int iterations) {
                this.scheduler = scheduler;
                this.taskCompletions = new Semaphore(0);
                this.cyclicBarrier = new CyclicBarrier(schedulerTasks, () -> remaining--);
                this.remaining = iterations;
            }

            public void start() {
                // The scheduler tasks are running on the dedicated scheduler thread pool. Each task submits
                // a test task on the EsThreadPoolExecutor (`testSubject`) releasing one `taskCompletions` permit.
                final Runnable schedulerTask = () -> {
                    try {
                        while (remaining > 0) {
                            // Wait for all scheduler threads to be ready for the next attempt.
                            var first = cyclicBarrier.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS) == schedulerTasks - 1;
                            if (first && keepAliveNanos > 0) {
                                // The task submitted by the first scheduler task (after reaching the keep alive time) is the task
                                // that might starve without any worker available unless an additional worker probe is submitted.
                                waitUntilKeepAliveTime(keepAliveNanos);
                            }
                            // Test EsThreadPoolExecutor by submitting a task that releases one permit.
                            testSubject.execute(taskCompletions::release);
                            if (first) {
                                // Let the first scheduler task (by arrival on the barrier) wait for all permits.
                                var success = taskCompletions.tryAcquire(
                                    schedulerTasks,
                                    SAFE_AWAIT_TIMEOUT.millis(),
                                    TimeUnit.MILLISECONDS
                                );
                                if (success == false) {
                                    var msg = Strings.format(
                                        "timed out waiting for [%s] of [%s] tasks to complete [queue size: %s, workers: %s] ",
                                        schedulerTasks - taskCompletions.availablePermits(),
                                        schedulerTasks,
                                        testSubject.getQueue().size(),
                                        testSubject.getPoolSize()
                                    );
                                    result.onFailure(new TimeoutException(msg));
                                    return;
                                }
                            }
                        }
                    } catch (Exception e) {
                        result.onFailure(e);
                        return;
                    }
                    result.onResponse(null);
                };
                // Run scheduler tasks on the dedicated scheduler thread pool.
                for (int i = 0; i < schedulerTasks; i++) {
                    scheduler.execute(schedulerTask);
                }
            }
        }

        try (var scheduler = Executors.newFixedThreadPool(schedulerTasks)) {
            for (int i = 0; i < 100; i++) {
                TaskScheduler taskScheduler = new TaskScheduler(scheduler, between(10, 200));
                taskScheduler.start();
                safeAwait(taskScheduler.result);
            }
        } finally {
            ThreadPool.terminate(testSubject, 1, TimeUnit.SECONDS);
        }
    }

    private void waitUntilKeepAliveTime(long keepAliveNanos) {
        var targetNanoTime = System.nanoTime() + keepAliveNanos + between(-1_000, 1_000);
        while (System.nanoTime() < targetNanoTime) {
            Thread.yield();
        }
    }
}
