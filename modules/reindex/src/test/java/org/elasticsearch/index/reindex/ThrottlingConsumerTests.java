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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class ThrottlingConsumerTests extends ESTestCase {

    private DeterministicSchedulerThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new DeterministicSchedulerThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testThrottling() {
        AtomicReference<Long> throttledValue = new AtomicReference<>();
        // we start random, maybe we even overflow, which is fine.
        AtomicLong time = new AtomicLong(randomLong());
        ThrottlingConsumer<Long> throttler
            = new ThrottlingConsumer<>(wrap(throttledValue::set), TimeValue.timeValueNanos(10), time::get, threadPool);

        assertNull(throttledValue.get());
        threadPool.validate(false, null, false);

        for (int i = 0; i < randomIntBetween(0, 10); ++i) {
            throttler.accept(randomLong());
            assertNull(throttledValue.get());
            threadPool.validate(true, TimeValue.timeValueNanos(10), false);
        }

        time.addAndGet(randomLongBetween(0, 20));
        long expectedValue = randomLong();
        throttler.accept(expectedValue);
        assertNull(throttledValue.get());

        threadPool.runScheduledTask();

        threadPool.validate(false, null, false);
        assertThat(throttledValue.get(), equalTo(expectedValue));

        long timePassed = randomLongBetween(5, 15);
        time.addAndGet(timePassed);

        expectedValue = randomLong();
        throttler.accept(expectedValue);

        threadPool.validate(true, TimeValue.timeValueNanos(Math.max(10-timePassed, 0)), false);
        threadPool.runScheduledTask();

        threadPool.validate(false, null, false);
        assertThat(throttledValue.get(), equalTo(expectedValue));

        // maybe provoke overflow.
        time.addAndGet(randomLongBetween(10, Long.MAX_VALUE));

        expectedValue = randomLong();
        throttler.accept(expectedValue);

        threadPool.validate(true, TimeValue.timeValueNanos(0), false);
        threadPool.runScheduledTask();

        threadPool.validate(false, null, false);
        assertThat(throttledValue.get(), equalTo(expectedValue));
    }

    public void testCloseNormal() {
        AtomicLong time = new AtomicLong(randomLong());
        ThrottlingConsumer<Long> throttler
            = new ThrottlingConsumer<>(wrap(l -> fail()), TimeValue.timeValueNanos(10), time::get, threadPool);

        AtomicBoolean closed = new AtomicBoolean();
        throttler.close(() -> assertTrue(closed.compareAndSet(false, true)));
        assertTrue(closed.get());

        throttler.accept(randomLong());
        threadPool.validate(false, null, false);
    }

    public void testCloseCancel() {
        AtomicLong time = new AtomicLong(randomLong());
        ThrottlingConsumer<Long> throttler
            = new ThrottlingConsumer<>(wrap(l -> fail()), TimeValue.timeValueNanos(10), time::get, threadPool);

        throttler.accept(randomLong());
        threadPool.validate(true, TimeValue.timeValueNanos(10), false);

        AtomicBoolean closed = new AtomicBoolean();
        throttler.close(() -> assertTrue(closed.compareAndSet(false, true)));
        assertTrue(closed.get());

        threadPool.validate(true, TimeValue.timeValueNanos(10), true);
    }

    public void testCloseNotifyWhenOutboundActive() throws Exception {
        AtomicLong time = new AtomicLong(randomLong());
        CyclicBarrier rendezvous = new CyclicBarrier(2);
        Consumer<Long> waitingConsumer = x -> {
            try {
                rendezvous.await();
                rendezvous.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new AssertionError(e);
            }
        };
        ThrottlingConsumer<Long> throttler
            = new ThrottlingConsumer<>(wrap(waitingConsumer), TimeValue.timeValueNanos(10), time::get, threadPool);

        throttler.accept(randomLong());
        threadPool.validate(true, TimeValue.timeValueNanos(10), false);

        Future<?> future = threadPool.generic().submit(() -> threadPool.runScheduledTask());

        rendezvous.await();

        AtomicBoolean closed = new AtomicBoolean();
        throttler.close(() -> assertTrue(closed.compareAndSet(false, true)));
        assertFalse(closed.get());

        rendezvous.await();
        future.get(10, TimeUnit.SECONDS);

        assertTrue(closed.get());

        threadPool.validate(false, null, false);
    }

    public void testCloseNotifyWhenOutboundActiveAsyncConsumer() {
        AtomicLong time = new AtomicLong(randomLong());
        AtomicReference<Runnable> whenDoneReference = new AtomicReference<>();
        BiConsumer<Long, Runnable> deferredResponseConsumer = (x, runnable) -> {
            assertTrue(whenDoneReference.compareAndSet(null, runnable));
        };
        ThrottlingConsumer<Long> throttler
            = new ThrottlingConsumer<>(deferredResponseConsumer, TimeValue.timeValueNanos(10), time::get, threadPool);

        throttler.accept(randomLong());
        threadPool.validate(true, TimeValue.timeValueNanos(10), false);
        threadPool.runScheduledTask();
        assertNotNull(whenDoneReference.get());

        AtomicBoolean closed = new AtomicBoolean();
        throttler.close(() -> assertTrue(closed.compareAndSet(false, true)));
        assertFalse(closed.get());

        whenDoneReference.get().run();

        assertTrue(closed.get());
        threadPool.validate(false, null, false);
    }

    private <T> BiConsumer<T, Runnable> wrap(Consumer<T> consumer) {
        return (t, whenDone) -> {
            try {
                consumer.accept(t);
            } finally {
                whenDone.run();
            }
        };
    }

    private static class DeterministicSchedulerThreadPool extends TestThreadPool {
        private Runnable command;
        private TimeValue delay;
        private boolean cancelled;

        private DeterministicSchedulerThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
            super(name, customBuilders);
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
            assertNull(this.command);
            assertNull(this.delay);
            this.command = command;
            this.delay = delay;
            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int compareTo(Delayed o) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean cancel() {
                    assertFalse(cancelled);
                    cancelled = true;
                    return true;
                }

                @Override
                public boolean isCancelled() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public void runScheduledTask() {
            assertNotNull(command);
            command.run();
            command = null;
            delay = null;
        }

        public void validate(boolean hasCommand, TimeValue delay, boolean cancelled) {
            assertEquals(hasCommand, this.command != null);
            assertEquals(delay, this.delay);
            assertEquals(cancelled, this.cancelled);
        }
    }
}
