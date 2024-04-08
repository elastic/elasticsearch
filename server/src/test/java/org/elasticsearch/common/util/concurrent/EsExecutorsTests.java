/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DEFAULT;
import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DO_NOT_TRACK;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

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
}
