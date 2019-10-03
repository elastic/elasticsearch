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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;

public class CooldownEsThreadPoolExecutorTests extends ESTestCase {

    public void testCooldownBetweenRuns() throws Exception {
        try (ThreadContext context = new ThreadContext(Settings.EMPTY)) {
            long start = 10_000_000;
            AtomicLong now = new AtomicLong(start);
            AtomicReference<BiConsumer<Long, Long>> pauseFn = new AtomicReference<>((a, b) -> {});
            AtomicReference<Runnable> afterFn = new AtomicReference<>(() -> {});
            PauseOverridingExecutor executor = new PauseOverridingExecutor("cooldown", TimeValue.timeValueMinutes(1),
                now::get, new SizeBlockingQueue<>(ConcurrentCollections.newBlockingQueue(), 10),
                EsExecutors.daemonThreadFactory("queuetest"), context,
                (a, b) -> pauseFn.get().accept(a, b),
                () -> afterFn.get().run());

            // The first execution should run immediately with no sleeping
            CountDownLatch run = new CountDownLatch(2);
            pauseFn.set((a, b) -> fail("should not pause at all"));
            logger.info("--> first run");
            afterFn.set(run::countDown);
            executor.execute(() -> {
                // This task takes 5 seconds
                now.addAndGet(TimeValue.timeValueSeconds(5).millis());
                run.countDown();
            });
            run.await(5, TimeUnit.SECONDS);
            assertThat(executor.getLastExecutionFinished(), equalTo(start + TimeValue.timeValueSeconds(5).millis()));

            // Advance the clock 40 seconds
            now.addAndGet(TimeValue.timeValueSeconds(40).millis());
            AtomicBoolean pauseRun = new AtomicBoolean(false);
            pauseFn.set((time, pauseTime) -> {
                pauseRun.set(true);
                assertThat(time, equalTo(start + TimeValue.timeValueSeconds(45).millis()));
                // We should need to pause for 20 seconds
                assertThat(pauseTime, equalTo(TimeValue.timeValueSeconds(20).millis()));
                // Simulate pausing for 20 seconds
                now.addAndGet(TimeValue.timeValueSeconds(20).millis());
            });
            CountDownLatch task = new CountDownLatch(2);
            logger.info("--> second run");
            afterFn.set(task::countDown);
            executor.execute(() -> {
                // The fake task takes 25 seconds
                now.addAndGet(TimeValue.timeValueSeconds(25).millis());
                task.countDown();
            });
            task.await(5, TimeUnit.SECONDS);
            assertTrue(pauseRun.get());
            assertThat(executor.getLastExecutionFinished(), equalTo(start + TimeValue.timeValueSeconds(5 + 40 + 20 + 25).millis()));

            // last run time should now be 5 + 40 + 20 + 25 = 90
            // set now to 95 seconds past start
            now.set(start + TimeValue.timeValueSeconds(95).millis());
            pauseRun.set(false);
            pauseFn.set((time, pauseTime) -> {
                pauseRun.set(true);
                assertThat(time, equalTo(start + TimeValue.timeValueSeconds(95).millis()));
                // 60 - (95 - 90) = 55 second pause
                assertThat(pauseTime, equalTo(TimeValue.timeValueSeconds(55).millis()));
                // Simulate pausing for 55 seconds
                now.getAndAdd(TimeValue.timeValueSeconds(55).millis());
            });
            CountDownLatch task2 = new CountDownLatch(2);
            logger.info("--> third run");
            afterFn.set(task2::countDown);
            executor.execute(() -> {
                // Task task 75 seconds
                now.getAndAdd(TimeValue.timeValueSeconds(75).millis());
                task2.countDown();
            });
            task2.await(5, TimeUnit.SECONDS);
            assertTrue(pauseRun.get());
            assertThat(executor.getLastExecutionFinished(),
                equalTo(start + TimeValue.timeValueSeconds(95 + 55 + 75).millis()));

            now.addAndGet(TimeValue.timeValueMinutes(2).millis());
            pauseFn.set((a, b) -> fail("should not have to pause"));
            CountDownLatch task3 = new CountDownLatch(2);
            afterFn.set(task3::countDown);
            logger.info("--> fourth run");
            executor.execute(() -> {
                now.getAndAdd(TimeValue.timeValueMinutes(5).millis());
                task3.countDown();
            });
            task3.await(5, TimeUnit.SECONDS);
            assertThat(executor.getLastExecutionFinished(),
                equalTo(start + TimeValue.timeValueSeconds(95 + 55 + 75).millis() +
                    TimeValue.timeValueMinutes(2 + 5).millis()));

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
        }
    }

    private class PauseOverridingExecutor extends CooldownEsThreadPoolExecutor {
        private final BiConsumer<Long, Long> overridenPause;
        private final Runnable afterExecute;

        PauseOverridingExecutor(String name, TimeValue cooldown, LongSupplier nowSupplier, BlockingQueue<Runnable> workQueue,
                                ThreadFactory threadFactory, ThreadContext contextHolder,
                                BiConsumer<Long, Long> pause, Runnable afterExecute) {
            super(name, cooldown, nowSupplier, workQueue, threadFactory, contextHolder);
            this.overridenPause = pause;
            this.afterExecute = afterExecute;
        }

        @Override
        void pause(long now, long cooldownRemainingMillis) {
            logger.info("--> fake pause for [{}/{}], now: [{}]",
                TimeValue.timeValueMillis(cooldownRemainingMillis),
                TimeValue.timeValueMillis(cooldownRemainingMillis).seconds(),
                now);
            overridenPause.accept(now, cooldownRemainingMillis);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            afterExecute.run();
        }
    }
}
