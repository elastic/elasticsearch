/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class EvilThreadPoolTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(EvilThreadPoolTests.class.getName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testExecutionErrorOnDefaultThreadPoolTypes() throws InterruptedException {
        for (String executor : ThreadPool.THREAD_POOL_TYPES.keySet()) {
            checkExecutionError(getExecuteRunner(threadPool.executor(executor)));
            checkExecutionError(getSubmitRunner(threadPool.executor(executor)));
            checkExecutionError(getScheduleRunner(executor));
        }
    }

    public void testExecutionErrorOnDirectExecutorService() throws InterruptedException {
        checkExecutionError(getExecuteRunner(EsExecutors.DIRECT_EXECUTOR_SERVICE));
        checkExecutionError(getSubmitRunner(EsExecutors.DIRECT_EXECUTOR_SERVICE));
    }

    public void testExecutionErrorOnFixedESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor fixedExecutor = EsExecutors.newFixed(
            "test",
            1,
            1,
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext(),
            randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
        );
        try {
            checkExecutionError(getExecuteRunner(fixedExecutor));
            checkExecutionError(getSubmitRunner(fixedExecutor));
        } finally {
            ThreadPool.terminate(fixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnScalingESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling(
            "test",
            1,
            1,
            10,
            TimeUnit.SECONDS,
            randomBoolean(),
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext()
        );
        try {
            checkExecutionError(getExecuteRunner(scalingExecutor));
            checkExecutionError(getSubmitRunner(scalingExecutor));
        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnSinglePrioritizingThreadPoolExecutor() throws InterruptedException {
        final PrioritizedEsThreadPoolExecutor prioritizedExecutor = EsExecutors.newSinglePrioritizing(
            "test",
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext(),
            threadPool.scheduler()
        );
        try {
            checkExecutionError(getExecuteRunner(prioritizedExecutor));
            checkExecutionError(getSubmitRunner(prioritizedExecutor));
            // bias towards timeout
            checkExecutionError(r -> prioritizedExecutor.execute(delayMillis(r, 10), TimeValue.ZERO, r));
            // race whether timeout or success (but typically biased towards success)
            checkExecutionError(r -> prioritizedExecutor.execute(r, TimeValue.ZERO, r));
            // bias towards no timeout.
            checkExecutionError(r -> prioritizedExecutor.execute(r, TimeValue.timeValueMillis(10), r));
        } finally {
            ThreadPool.terminate(prioritizedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnScheduler() throws InterruptedException {
        final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY, "test-scheduler");
        try {
            checkExecutionError(getExecuteRunner(scheduler));
            checkExecutionError(getSubmitRunner(scheduler));
            checkExecutionError(r -> scheduler.schedule(r, randomFrom(0, 1), TimeUnit.MILLISECONDS));
        } finally {
            Scheduler.terminate(scheduler, 10, TimeUnit.SECONDS);
        }
    }

    private void checkExecutionError(Consumer<Runnable> runner) throws InterruptedException {
        logger.info("checking error for {}", runner);
        final Runnable runnable;
        if (randomBoolean()) {
            runnable = () -> { throw new Error("future error"); };
        } else {
            runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {

                }

                @Override
                protected void doRun() {
                    throw new Error("future error");
                }
            };
        }
        runExecutionTest(runner, runnable, true, o -> {
            assertTrue(o.isPresent());
            assertThat(o.get(), instanceOf(Error.class));
            assertThat(o.get(), hasToString(containsString("future error")));
        });
    }

    public void testExecutionExceptionOnDefaultThreadPoolTypes() throws InterruptedException {
        for (String executor : ThreadPool.THREAD_POOL_TYPES.keySet()) {
            checkExecutionException(getExecuteRunner(threadPool.executor(executor)), true);

            // here, it's ok for the exception not to bubble up. Accessing the future will yield the exception
            checkExecutionException(getSubmitRunner(threadPool.executor(executor)), false);

            checkExecutionException(getScheduleRunner(executor), true);
        }
    }

    public void testExecutionExceptionOnDirectExecutorService() throws InterruptedException {
        checkExecutionException(getExecuteRunner(EsExecutors.DIRECT_EXECUTOR_SERVICE), true);
        checkExecutionException(getSubmitRunner(EsExecutors.DIRECT_EXECUTOR_SERVICE), false);
    }

    public void testExecutionExceptionOnFixedESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor fixedExecutor = EsExecutors.newFixed(
            "test",
            1,
            1,
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext(),
            randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
        );
        try {
            checkExecutionException(getExecuteRunner(fixedExecutor), true);
            checkExecutionException(getSubmitRunner(fixedExecutor), false);
        } finally {
            ThreadPool.terminate(fixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnScalingESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling(
            "test",
            1,
            1,
            10,
            TimeUnit.SECONDS,
            randomBoolean(),
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext()
        );
        try {
            checkExecutionException(getExecuteRunner(scalingExecutor), true);
            checkExecutionException(getSubmitRunner(scalingExecutor), false);
        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnSinglePrioritizingThreadPoolExecutor() throws InterruptedException {
        final PrioritizedEsThreadPoolExecutor prioritizedExecutor = EsExecutors.newSinglePrioritizing(
            "test",
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext(),
            threadPool.scheduler()
        );
        try {
            checkExecutionException(getExecuteRunner(prioritizedExecutor), true);
            checkExecutionException(getSubmitRunner(prioritizedExecutor), false);

            // bias towards timeout
            checkExecutionException(r -> prioritizedExecutor.execute(delayMillis(r, 10), TimeValue.ZERO, r), true);
            // race whether timeout or success (but typically biased towards success)
            checkExecutionException(r -> prioritizedExecutor.execute(r, TimeValue.ZERO, r), true);
            // bias towards no timeout.
            checkExecutionException(r -> prioritizedExecutor.execute(r, TimeValue.timeValueMillis(10), r), true);
        } finally {
            ThreadPool.terminate(prioritizedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnScheduler() throws InterruptedException {
        final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY, "test-scheduler");
        try {
            checkExecutionException(getExecuteRunner(scheduler), true);
            // while submit does return a Future, we choose to log exceptions anyway,
            // since this is the semi-internal SafeScheduledThreadPoolExecutor that is being used,
            // which also logs exceptions for schedule calls.
            checkExecutionException(getSubmitRunner(scheduler), true);
            checkExecutionException(r -> scheduler.schedule(r, randomFrom(0, 1), TimeUnit.MILLISECONDS), true);
        } finally {
            Scheduler.terminate(scheduler, 10, TimeUnit.SECONDS);
        }
    }

    private Runnable delayMillis(Runnable r, int ms) {
        return () -> {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            r.run();
        };
    }

    private void checkExecutionException(Consumer<Runnable> runner, boolean expectException) throws InterruptedException {
        final Runnable runnable;
        final boolean willThrow;
        if (randomBoolean()) {
            logger.info("checking direct exception for {}", runner);
            runnable = () -> { throw new IllegalStateException("future exception"); };
            willThrow = expectException;
        } else {
            logger.info("checking abstract runnable exception for {}", runner);
            runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {

                }

                @Override
                protected void doRun() {
                    throw new IllegalStateException("future exception");
                }
            };
            willThrow = false;
        }
        runExecutionTest(runner, runnable, willThrow, o -> {
            assertEquals(willThrow, o.isPresent());
            if (willThrow) {
                if (o.get() instanceof Error error) throw error;
                assertThat(o.get(), instanceOf(IllegalStateException.class));
                assertThat(o.get(), hasToString(containsString("future exception")));
            }
        });
    }

    Consumer<Runnable> getExecuteRunner(ExecutorService executor) {
        return new Consumer<Runnable>() {
            @Override
            public void accept(Runnable runnable) {
                executor.execute(runnable);
            }

            @Override
            public String toString() {
                return "executor(" + executor + ").execute()";
            }
        };
    }

    Consumer<Runnable> getSubmitRunner(ExecutorService executor) {
        return new Consumer<Runnable>() {
            @Override
            public void accept(Runnable runnable) {
                executor.submit(runnable);
            }

            @Override
            public String toString() {
                return "executor(" + executor + ").submit()";
            }
        };
    }

    Consumer<Runnable> getScheduleRunner(String executor) {
        return new Consumer<Runnable>() {
            @Override
            public void accept(Runnable runnable) {
                threadPool.schedule(runnable, randomFrom(TimeValue.ZERO, TimeValue.timeValueMillis(1)), executor);
            }

            @Override
            public String toString() {
                return "schedule(" + executor + ")";
            }
        };
    }

    private void runExecutionTest(
        final Consumer<Runnable> runner,
        final Runnable runnable,
        final boolean expectThrowable,
        final Consumer<Optional<Throwable>> consumer
    ) throws InterruptedException {
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        final CountDownLatch uncaughtExceptionHandlerLatch = new CountDownLatch(1);

        try {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                assertTrue(expectThrowable);
                assertTrue("Only one message allowed", throwableReference.compareAndSet(null, e));
                uncaughtExceptionHandlerLatch.countDown();
            });

            final CountDownLatch supplierLatch = new CountDownLatch(1);

            try {
                runner.accept(() -> {
                    try {
                        runnable.run();
                    } finally {
                        supplierLatch.countDown();
                    }
                });
            } catch (Throwable t) {
                consumer.accept(Optional.of(t));
                return;
            }

            supplierLatch.await();

            if (expectThrowable) {
                uncaughtExceptionHandlerLatch.await();
            }

            consumer.accept(Optional.ofNullable(throwableReference.get()));
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

}
