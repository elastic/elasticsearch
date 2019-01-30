/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.threadpool;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.After;
import org.junit.Before;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

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
        final ExecutorService directExecutorService = EsExecutors.newDirectExecutorService();
        checkExecutionError(getExecuteRunner(directExecutorService));
        checkExecutionError(getSubmitRunner(directExecutorService));
    }

    public void testExecutionErrorOnFixedESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor fixedExecutor = EsExecutors.newFixed("test", 1, 1,
            EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionError(getExecuteRunner(fixedExecutor));
            checkExecutionError(getSubmitRunner(fixedExecutor));
        } finally {
            ThreadPool.terminate(fixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnScalingESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling("test", 1, 1,
            10, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionError(getExecuteRunner(scalingExecutor));
            checkExecutionError(getSubmitRunner(scalingExecutor));
        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnAutoQueueFixedESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor autoQueueFixedExecutor = EsExecutors.newAutoQueueFixed("test", 1, 1,
            1, 1, 1, TimeValue.timeValueSeconds(10), EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionError(getExecuteRunner(autoQueueFixedExecutor));
            checkExecutionError(getSubmitRunner(autoQueueFixedExecutor));
        } finally {
            ThreadPool.terminate(autoQueueFixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnSinglePrioritizingThreadPoolExecutor() throws InterruptedException {
        final PrioritizedEsThreadPoolExecutor prioritizedExecutor = EsExecutors.newSinglePrioritizing("test",
            EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext(), threadPool.scheduler());
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
        final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);
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
            runnable = () -> {
                throw new Error("future error");
            };
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
        runExecutionTest(
            runner,
            runnable,
            true,
            o -> {
                assertTrue(o.isPresent());
                assertThat(o.get(), instanceOf(Error.class));
                assertThat(o.get(), hasToString(containsString("future error")));
            },
            Object::toString);
    }

    public void testExecutionExceptionOnDefaultThreadPoolTypes() throws InterruptedException {
        for (String executor : ThreadPool.THREAD_POOL_TYPES.keySet()) {
            final boolean expectExceptionOnExecute =
                // fixed_auto_queue_size wraps stuff into TimedRunnable, which is an AbstractRunnable
                // TODO: this is dangerous as it will silently swallow exceptions, and possibly miss calling a response listener
                ThreadPool.THREAD_POOL_TYPES.get(executor) != ThreadPool.ThreadPoolType.FIXED_AUTO_QUEUE_SIZE;
            checkExecutionException(getExecuteRunner(threadPool.executor(executor)), expectExceptionOnExecute);

            // here, it's ok for the exception not to bubble up. Accessing the future will yield the exception
            checkExecutionException(getSubmitRunner(threadPool.executor(executor)), false);

            final boolean expectExceptionOnSchedule =
                // fixed_auto_queue_size wraps stuff into TimedRunnable, which is an AbstractRunnable
                // TODO: this is dangerous as it will silently swallow exceptions, and possibly miss calling a response listener
                ThreadPool.THREAD_POOL_TYPES.get(executor) != ThreadPool.ThreadPoolType.FIXED_AUTO_QUEUE_SIZE;
            checkExecutionException(getScheduleRunner(executor), expectExceptionOnSchedule);
        }
    }

    public void testExecutionExceptionOnDirectExecutorService() throws InterruptedException {
        final ExecutorService directExecutorService = EsExecutors.newDirectExecutorService();
        checkExecutionException(getExecuteRunner(directExecutorService), true);
        checkExecutionException(getSubmitRunner(directExecutorService), false);
    }

    public void testExecutionExceptionOnFixedESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor fixedExecutor = EsExecutors.newFixed("test", 1, 1,
            EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionException(getExecuteRunner(fixedExecutor), true);
            checkExecutionException(getSubmitRunner(fixedExecutor), false);
        } finally {
            ThreadPool.terminate(fixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnScalingESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling("test", 1, 1,
            10, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionException(getExecuteRunner(scalingExecutor), true);
            checkExecutionException(getSubmitRunner(scalingExecutor), false);
        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnAutoQueueFixedESThreadPoolExecutor() throws InterruptedException {
        final EsThreadPoolExecutor autoQueueFixedExecutor = EsExecutors.newAutoQueueFixed("test", 1, 1,
            1, 1, 1, TimeValue.timeValueSeconds(10), EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            // fixed_auto_queue_size wraps stuff into TimedRunnable, which is an AbstractRunnable
            // TODO: this is dangerous as it will silently swallow exceptions, and possibly miss calling a response listener
            checkExecutionException(getExecuteRunner(autoQueueFixedExecutor), false);
            checkExecutionException(getSubmitRunner(autoQueueFixedExecutor), false);
        } finally {
            ThreadPool.terminate(autoQueueFixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnSinglePrioritizingThreadPoolExecutor() throws InterruptedException {
        final PrioritizedEsThreadPoolExecutor prioritizedExecutor = EsExecutors.newSinglePrioritizing("test",
            EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext(), threadPool.scheduler());
        try {
            checkExecutionException(getExecuteRunner(prioritizedExecutor), true);
            checkExecutionException(getSubmitRunner(prioritizedExecutor), false);

            Function<Runnable, String> logMessageFunction = r -> PrioritizedEsThreadPoolExecutor.class.getName();
            // bias towards timeout
            checkExecutionException(r -> prioritizedExecutor.execute(delayMillis(r, 10), TimeValue.ZERO, r), true, logMessageFunction);
            // race whether timeout or success (but typically biased towards success)
            checkExecutionException(r -> prioritizedExecutor.execute(r, TimeValue.ZERO, r), true, logMessageFunction);
            // bias towards no timeout.
            checkExecutionException(r -> prioritizedExecutor.execute(r, TimeValue.timeValueMillis(10), r), true, logMessageFunction);
        } finally {
            ThreadPool.terminate(prioritizedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnScheduler() throws InterruptedException {
        final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);
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
        checkExecutionException(runner, expectException, Object::toString);
    }
    private void checkExecutionException(Consumer<Runnable> runner,
                                         boolean expectException,
                                         Function<Runnable, String> logMessageFunction) throws InterruptedException {
        final Runnable runnable;
        final boolean willThrow;
        if (randomBoolean()) {
            logger.info("checking direct exception for {}", runner);
            runnable = () -> {
                throw new IllegalStateException("future exception");
            };
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
        runExecutionTest(
            runner,
            runnable,
            willThrow,
            o -> {
                assertEquals(willThrow, o.isPresent());
                if (willThrow) {
                    if (o.get() instanceof Error) throw (Error) o.get();
                    assertThat(o.get(), instanceOf(IllegalStateException.class));
                    assertThat(o.get(), hasToString(containsString("future exception")));
                }
            },
            logMessageFunction);
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
        final Consumer<Optional<Throwable>> consumer,
        final Function<Runnable, String> logMessageFunction) throws InterruptedException {
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

            Runnable job = () -> {
                try {
                    runnable.run();
                } finally {
                    supplierLatch.countDown();
                }
            };

            // snoop on logging to also handle the cases where exceptions are simply logged in Scheduler.
            final Logger schedulerLogger = LogManager.getLogger(Scheduler.SafeScheduledThreadPoolExecutor.class);
            final MockLogAppender appender = new MockLogAppender();
            appender.addExpectation(
                new MockLogAppender.LoggingExpectation() {
                    @Override
                    public void match(LogEvent event) {
                        if (event.getLevel() == Level.WARN) {
                            assertThat("no other warnings than those expected",
                                event.getMessage().getFormattedMessage(),
                                startsWith("failed to schedule " + logMessageFunction.apply(job)));
                            assertTrue(expectThrowable);
                            assertTrue("only one message allowed", throwableReference.compareAndSet(null, event.getThrown()));
                            uncaughtExceptionHandlerLatch.countDown();
                        }
                    }

                    @Override
                    public void assertMatched() {
                    }
                });

            appender.start();
            Loggers.addAppender(schedulerLogger, appender);
            try {
                try {
                    runner.accept(job);
                } catch (Throwable t) {
                    consumer.accept(Optional.of(t));
                    return;
                }

                supplierLatch.await();

                if (expectThrowable) {
                    uncaughtExceptionHandlerLatch.await();
                }
            } finally {
                Loggers.removeAppender(schedulerLogger, appender);
                appender.stop();
            }

            consumer.accept(Optional.ofNullable(throwableReference.get()));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

}
