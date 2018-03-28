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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.threadpool.Scheduler.ReschedulingRunnable;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Unit tests for the scheduling of tasks with a fixed delay
 */
public class ScheduleWithFixedDelayTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "fixed delay tests").build());
    }

    @After
    public void shutdown() throws Exception {
        terminate(threadPool);
    }

    public void testDoesNotRescheduleUntilExecutionFinished() throws Exception {
        final TimeValue delay = TimeValue.timeValueMillis(100L);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch pauseLatch = new CountDownLatch(1);
        ThreadPool threadPool = mock(ThreadPool.class);
        final Runnable runnable = () ->  {
            // notify that the runnable is started
            startLatch.countDown();
            try {
                // wait for other thread to un-pause
                pauseLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        ReschedulingRunnable reschedulingRunnable = new ReschedulingRunnable(runnable, delay, Names.GENERIC, threadPool,
                (e) -> {}, (e) -> {});
        // this call was made during construction of the runnable
        verify(threadPool, times(1)).schedule(delay, Names.GENERIC, reschedulingRunnable);

        // create a thread and start the runnable
        Thread runThread = new Thread() {
            @Override
            public void run() {
                reschedulingRunnable.run();
            }
        };
        runThread.start();

        // wait for the runnable to be started and ensure the runnable hasn't used the threadpool again
        startLatch.await();
        verifyNoMoreInteractions(threadPool);

        // un-pause the runnable and allow it to complete execution
        pauseLatch.countDown();
        runThread.join();

        // validate schedule was called again
        verify(threadPool, times(2)).schedule(delay, Names.GENERIC, reschedulingRunnable);
    }

    public void testThatRunnableIsRescheduled() throws Exception {
        final CountDownLatch latch = new CountDownLatch(scaledRandomIntBetween(2, 16));
        final Runnable countingRunnable = () -> {
            if (rarely()) {
                throw new ElasticsearchException("sometimes we throw before counting down");
            }

            latch.countDown();

            if (randomBoolean()) {
                throw new ElasticsearchException("this shouldn't cause the test to fail!");
            }
        };

        Cancellable cancellable = threadPool.scheduleWithFixedDelay(countingRunnable, TimeValue.timeValueMillis(10L), Names.GENERIC);
        assertNotNull(cancellable);

        // wait for the number of successful count down operations
        latch.await();

        // cancel
        cancellable.cancel();
        assertTrue(cancellable.isCancelled());
    }

    public void testCancellingRunnable() throws Exception {
        final boolean shouldThrow = randomBoolean();
        final AtomicInteger counter = new AtomicInteger(scaledRandomIntBetween(2, 16));
        final CountDownLatch doneLatch = new CountDownLatch(1);
        final AtomicReference<Cancellable> cancellableRef = new AtomicReference<>();
        final AtomicBoolean runAfterDone = new AtomicBoolean(false);
        final Runnable countingRunnable = () -> {

            if (doneLatch.getCount() == 0) {
                runAfterDone.set(true);
                logger.warn("this runnable ran after it was cancelled");
            }

            final Cancellable cancellable = cancellableRef.get();
            if (cancellable == null) {
                // wait for the cancellable to be present before we really start so we can accurately know we cancelled
                return;
            }

            // rarely throw an exception before counting down
            if (shouldThrow && rarely()) {
                throw new RuntimeException("throw before count down");
            }

            final int count = counter.decrementAndGet();

            // see if we have counted down to zero or below yet. the exception throwing could make us count below zero
            if (count <= 0) {
                cancellable.cancel();
                doneLatch.countDown();
            }

            // rarely throw an exception after execution
            if (shouldThrow && rarely()) {
                throw new RuntimeException("throw at end");
            }
        };
        Cancellable cancellable = threadPool.scheduleWithFixedDelay(countingRunnable, TimeValue.timeValueMillis(10L), Names.GENERIC);
        cancellableRef.set(cancellable);
        // wait for the runnable to finish
        doneLatch.await();

        // the runnable should have cancelled itself
        assertTrue(cancellable.isCancelled());
        assertFalse(runAfterDone.get());

        // rarely wait and make sure the runnable didn't run at the next interval
        if (rarely()) {
            assertFalse(awaitBusy(runAfterDone::get, 1L, TimeUnit.SECONDS));
        }
    }

    public void testBlockingCallOnSchedulerThreadFails() throws Exception {
        final BaseFuture<Object> future = new BaseFuture<Object>() {};
        final TestFuture resultsFuture = new TestFuture();
        final boolean getWithTimeout = randomBoolean();

        final Runnable runnable = () -> {
            try {
                Object obj;
                if (getWithTimeout) {
                    obj = future.get(1L, TimeUnit.SECONDS);
                } else {
                    obj = future.get();
                }
                resultsFuture.futureDone(obj);
            } catch (Throwable t) {
                resultsFuture.futureDone(t);
            }
        };

        Cancellable cancellable = threadPool.scheduleWithFixedDelay(runnable, TimeValue.timeValueMillis(10L), Names.SAME);
        Object resultingObject = resultsFuture.get();
        assertNotNull(resultingObject);
        assertThat(resultingObject, instanceOf(Throwable.class));
        Throwable t = (Throwable) resultingObject;
        assertThat(t, instanceOf(AssertionError.class));
        assertThat(t.getMessage(), containsString("Blocking"));
        assertFalse(cancellable.isCancelled());
    }

    public void testBlockingCallOnNonSchedulerThreadAllowed() throws Exception {
        final TestFuture future = new TestFuture();
        final TestFuture resultsFuture = new TestFuture();
        final boolean rethrow = randomBoolean();
        final boolean getWithTimeout = randomBoolean();

        final Runnable runnable = () -> {
            try {
                Object obj;
                if (getWithTimeout) {
                    obj = future.get(1, TimeUnit.MINUTES);
                } else {
                    obj = future.get();
                }
                resultsFuture.futureDone(obj);
            } catch (Throwable t) {
                resultsFuture.futureDone(t);
                if (rethrow) {
                    throw new RuntimeException(t);
                }
            }
        };

        final Cancellable cancellable = threadPool.scheduleWithFixedDelay(runnable, TimeValue.timeValueMillis(10L), Names.GENERIC);
        assertFalse(resultsFuture.isDone());

        final Object o = new Object();
        future.futureDone(o);

        final Object resultingObject = resultsFuture.get();
        assertThat(resultingObject, sameInstance(o));
        assertFalse(cancellable.isCancelled());
    }

    public void testOnRejectionCausesCancellation() throws Exception {
        final TimeValue delay = TimeValue.timeValueMillis(10L);
        terminate(threadPool);
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "fixed delay tests").build()) {
            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String executor, Runnable command) {
                if (command instanceof ReschedulingRunnable) {
                    ((ReschedulingRunnable) command).onRejection(new EsRejectedExecutionException());
                } else {
                    fail("this should only be called with a rescheduling runnable in this test");
                }
                return null;
            }
        };
        Runnable runnable = () -> {};
        ReschedulingRunnable reschedulingRunnable = new ReschedulingRunnable(runnable, delay, Names.GENERIC,
                threadPool, (e) -> {}, (e) -> {});
        assertTrue(reschedulingRunnable.isCancelled());
    }

    public void testRunnableRunsAtMostOnceAfterCancellation() throws Exception {
        final int iterations = scaledRandomIntBetween(1, 12);
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch doneLatch = new CountDownLatch(iterations);
        final Runnable countingRunnable = () -> {
            counter.incrementAndGet();
            doneLatch.countDown();
        };

        final Cancellable cancellable = threadPool.scheduleWithFixedDelay(countingRunnable, TimeValue.timeValueMillis(10L), Names.GENERIC);
        doneLatch.await();
        cancellable.cancel();
        final int counterValue = counter.get();
        assertThat(counterValue, isOneOf(iterations, iterations + 1));

        if (rarely()) {
            awaitBusy(() -> {
                final int value = counter.get();
                return value == iterations || value == iterations + 1;
            }, 50L, TimeUnit.MILLISECONDS);
        }
    }

    static final class TestFuture extends BaseFuture<Object> {
        boolean futureDone(Object value) {
            return set(value);
        }
    }
}
