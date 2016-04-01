/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SelfReschedulingRunnableTests extends ESTestCase {

    public void testSelfReschedulingRunnableReschedules() throws Exception {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final AbstractRunnable runnable = mock(AbstractRunnable.class);
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        reschedulingRunnable.start();
        final int iterations = randomIntBetween(4, 24);
        for (int i = 0; i < iterations; i++) {
            // pretend we are the scheduler running it
            reschedulingRunnable.run();
        }

        verify(threadPool, times(iterations + 1)).schedule(timeValue, name, reschedulingRunnable);
        verifyZeroInteractions(logger);
    }

    public void testThrowingRunnableReschedules() throws Exception {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Throwable throwable) {
            }

            @Override
            protected void doRun() throws Exception {
                throw randomFrom(new UnsupportedOperationException(),
                        new ElasticsearchSecurityException(""),
                        new IllegalArgumentException(),
                        new NullPointerException());
            }
        };
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        reschedulingRunnable.start();
        final int iterations = randomIntBetween(4, 24);
        for (int i = 0; i < iterations; i++) {
            // pretend we are the scheduler running it
            reschedulingRunnable.run();
        }

        verify(threadPool, times(iterations + 1)).schedule(timeValue, name, reschedulingRunnable);
        verifyZeroInteractions(logger);
    }

    public void testDoesNotRescheduleUntilExecutionFinished() throws Exception {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch pauseLatch = new CountDownLatch(1);
        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Throwable throwable) {
            }

            @Override
            protected void doRun() throws Exception {
                startLatch.countDown();
                pauseLatch.await();
            }
        };
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        final SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        reschedulingRunnable.start();
        verify(threadPool).schedule(timeValue, name, reschedulingRunnable);
        Thread thread = new Thread() {
            @Override
            public void run() {
                reschedulingRunnable.run();
            }
        };
        thread.start();
        startLatch.await();

        verify(threadPool).schedule(timeValue, name, reschedulingRunnable);
        pauseLatch.countDown();
        thread.join();

        verify(threadPool, times(2)).schedule(timeValue, name, reschedulingRunnable);
    }

    public void testStopCancelsScheduledFuture() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ScheduledFuture future = mock(ScheduledFuture.class);
        final AbstractRunnable runnable = mock(AbstractRunnable.class);
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        when(threadPool.schedule(timeValue, name, reschedulingRunnable)).thenReturn(future);
        reschedulingRunnable.start();
        reschedulingRunnable.run();
        reschedulingRunnable.stop();

        verify(threadPool, times(2)).schedule(timeValue, name, reschedulingRunnable);
        verify(future).cancel(false);
    }

    public void testDoubleStartThrowsException() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final AbstractRunnable runnable = mock(AbstractRunnable.class);
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        reschedulingRunnable.start();
        try {
            reschedulingRunnable.start();
            fail("calling start before stopping is not allowed");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("start should not be called again"));
        }
    }

    public void testDoubleStopThrowsException() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final AbstractRunnable runnable = mock(AbstractRunnable.class);
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        reschedulingRunnable.start();
        reschedulingRunnable.stop();
        try {
            reschedulingRunnable.stop();
            fail("calling stop while not running is not allowed");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("stop called but not started or stop called twice"));
        }
    }

    public void testStopWithoutStartThrowsException() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final AbstractRunnable runnable = mock(AbstractRunnable.class);
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
        try {
            reschedulingRunnable.stop();
            fail("calling stop while not running is not allowed");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("stop called but not started or stop called twice"));
        }
    }

    public void testStopPreventsRunning() throws Exception {
        final ThreadPool threadPool = new ThreadPool("test-stop-self-schedule");
        final AtomicInteger failureCounter = new AtomicInteger(0);
        final AtomicInteger runCounter = new AtomicInteger(0);
        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Throwable throwable) {
                failureCounter.incrementAndGet();
            }

            @Override
            protected void doRun() throws Exception {
                runCounter.incrementAndGet();
            }
        };
        final ESLogger logger = mock(ESLogger.class);
        // arbitrary run time
        final TimeValue timeValue = TimeValue.timeValueSeconds(2L);
        final String name = Names.GENERIC;

        try {
            SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
            reschedulingRunnable.start();

            ScheduledFuture future = reschedulingRunnable.getScheduledFuture();
            assertThat(future, notNullValue());
            assertThat(future.isDone(), is(false));
            assertThat(future.isCancelled(), is(false));

            reschedulingRunnable.stop();

            assertThat(reschedulingRunnable.getScheduledFuture(), nullValue());
            assertThat(future.isCancelled(), is(true));
            assertThat(future.isDone(), is(true));

            boolean ran = awaitBusy(() -> runCounter.get() > 0 || failureCounter.get() > 0, 3L, TimeUnit.SECONDS);

            assertThat(ran, is(false));
        } finally {
            threadPool.shutdownNow();
        }
    }

    public void testStopPreventsRescheduling() throws Exception {
        final ThreadPool threadPool = new ThreadPool("test-stop-self-schedule");
        final CountDownLatch threadRunningLatch = new CountDownLatch(randomIntBetween(1, 16));
        final CountDownLatch stopCalledLatch = new CountDownLatch(1);
        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Throwable throwable) {
                throw new IllegalStateException("we should never be in this method!");
            }

            @Override
            protected void doRun() throws Exception {
                // notify we are running
                threadRunningLatch.countDown();
                if (threadRunningLatch.getCount() > 0) {
                    // let it keep running
                    return;
                }

                stopCalledLatch.await();
            }
        };
        final ESLogger logger = mock(ESLogger.class);
        final TimeValue timeValue = TimeValue.timeValueMillis(1L);
        final String name = Names.GENERIC;

        try {
            SelfReschedulingRunnable reschedulingRunnable = new SelfReschedulingRunnable(runnable, threadPool, timeValue, name, logger);
            reschedulingRunnable.start();
            threadRunningLatch.await();

            // call stop
            reschedulingRunnable.stop();
            stopCalledLatch.countDown();
            assertThat(reschedulingRunnable.getScheduledFuture(), nullValue());

            final ScheduledThreadPoolExecutor scheduledThreadPooleExecutor = (ScheduledThreadPoolExecutor) threadPool.scheduler();
            boolean somethingQueued = awaitBusy(() -> scheduledThreadPooleExecutor.getQueue().isEmpty() == false, 1L, TimeUnit.SECONDS);
            assertThat(somethingQueued, is(false));
        } finally {
            threadPool.shutdownNow();
        }
    }
}
