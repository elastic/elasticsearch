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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class SchedulerTests extends ESTestCase {

    public void testCancelOnThreadPool() {
        ThreadPool threadPool = new TestThreadPool("test");
        AtomicLong executed = new AtomicLong();
        try {
            ThreadPool.THREAD_POOL_TYPES.keySet().forEach(type ->
                scheduleAndCancel(threadPool, executed, type));
            assertEquals(0, executed.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private void scheduleAndCancel(ThreadPool threadPool, AtomicLong executed, String type) {
        Scheduler.ScheduledCancellable scheduled = threadPool.schedule(executed::incrementAndGet, TimeValue.timeValueSeconds(20), type);
        assertEquals(1, schedulerQueueSize(threadPool));
        assertFalse(scheduled.isCancelled());
        assertTrue(scheduled.cancel());
        assertTrue(scheduled.isCancelled());
        assertEquals("Cancel must auto-remove", 0, schedulerQueueSize(threadPool));
    }

    private int schedulerQueueSize(ThreadPool threadPool) {
        return ((Scheduler.SafeScheduledThreadPoolExecutor) threadPool.scheduler()).getQueue().size();
    }

    public void testCancelOnScheduler() {
        ScheduledThreadPoolExecutor executor = Scheduler.initScheduler(Settings.EMPTY);
        Scheduler scheduler = (command, delay, name) ->
            Scheduler.wrapAsScheduledCancellable(executor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));

        AtomicLong executed = new AtomicLong();
        try {
            Scheduler.ScheduledCancellable scheduled =
                scheduler.schedule(executed::incrementAndGet, TimeValue.timeValueSeconds(20), ThreadPool.Names.SAME);
            assertEquals(1, executor.getQueue().size());
            assertFalse(scheduled.isCancelled());
            assertTrue(scheduled.cancel());
            assertTrue(scheduled.isCancelled());
            assertEquals("Cancel must auto-remove", 0, executor.getQueue().size());
            assertEquals(0, executed.get());
        } finally {
            Scheduler.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }


    public void testDelay() throws InterruptedException {
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            List<Scheduler.ScheduledCancellable> jobs = LongStream.range(20,30)
                .mapToObj(delay -> threadPool.schedule(() -> {},
                    TimeValue.timeValueSeconds(delay),
                    ThreadPool.Names.SAME))
                .collect(Collectors.toCollection(ArrayList::new));

            Collections.reverse(jobs);

            List<Long> initialDelays = verifyJobDelays(jobs);
            Thread.sleep(50);
            List<Long> laterDelays = verifyJobDelays(jobs);

            assertThat(laterDelays,
                Matchers.contains(initialDelays.stream().map(Matchers::lessThan).collect(Collectors.toList())));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private List<Long> verifyJobDelays(List<Scheduler.ScheduledCancellable> jobs) {
        List<Long> delays = new ArrayList<>(jobs.size());
        Scheduler.ScheduledCancellable previous = null;
        for (Scheduler.ScheduledCancellable job : jobs) {
            if (previous != null) {
                long previousDelay = previous.getDelay(TimeUnit.MILLISECONDS);
                long delay = job.getDelay(TimeUnit.MILLISECONDS);
                assertThat(delay, Matchers.lessThan(previousDelay));
                assertThat(job, Matchers.lessThan(previous));
            }
            assertThat(job.getDelay(TimeUnit.SECONDS), Matchers.greaterThan(1L));
            assertThat(job.getDelay(TimeUnit.SECONDS), Matchers.lessThanOrEqualTo(30L));

            delays.add(job.getDelay(TimeUnit.NANOSECONDS));
            previous = job;
        }

        return delays;
    }

    // simple test for successful scheduling, exceptions tested more thoroughly in EvilThreadPoolTests
    public void testScheduledOnThreadPool() throws InterruptedException {
        ThreadPool threadPool = new TestThreadPool("test");
        CountDownLatch missingExecutions = new CountDownLatch(ThreadPool.THREAD_POOL_TYPES.keySet().size());
        try {
            ThreadPool.THREAD_POOL_TYPES.keySet()
                .forEach(type ->
                    threadPool.schedule(missingExecutions::countDown, TimeValue.timeValueMillis(randomInt(5)), type));

            assertTrue(missingExecutions.await(30, TimeUnit.SECONDS));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    // simple test for successful scheduling, exceptions tested more thoroughly in EvilThreadPoolTests
    public void testScheduledOnScheduler() throws InterruptedException {
        ScheduledThreadPoolExecutor executor = Scheduler.initScheduler(Settings.EMPTY);
        Scheduler scheduler = (command, delay, name) ->
            Scheduler.wrapAsScheduledCancellable(executor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));

        CountDownLatch missingExecutions = new CountDownLatch(1);
        try {
            scheduler.schedule(missingExecutions::countDown, TimeValue.timeValueMillis(randomInt(5)), ThreadPool.Names.SAME);
            assertTrue(missingExecutions.await(30, TimeUnit.SECONDS));
        } finally {
            Scheduler.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    public void testScheduleAtFixedRate() throws InterruptedException {
        ScheduledThreadPoolExecutor executor = Scheduler.initScheduler(Settings.EMPTY);
        try {
            CountDownLatch missingExecutions = new CountDownLatch(randomIntBetween(1, 10));
            executor.scheduleAtFixedRate(missingExecutions::countDown,
                randomIntBetween(1, 10), randomIntBetween(1, 10), TimeUnit.MILLISECONDS);
            assertTrue(missingExecutions.await(30, TimeUnit.SECONDS));
        } finally {
            Scheduler.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }
}
