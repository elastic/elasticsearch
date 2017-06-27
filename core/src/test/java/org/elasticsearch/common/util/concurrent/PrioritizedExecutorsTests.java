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

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PrioritizedExecutorsTests extends ESTestCase {

    private final ThreadContext holder = new ThreadContext(Settings.EMPTY);

    public void testPriorityQueue() throws Exception {
        PriorityBlockingQueue<Priority> queue = new PriorityBlockingQueue<>();
        List<Priority> priorities = Arrays.asList(Priority.values());
        Collections.shuffle(priorities, random());

        for (Priority priority : priorities) {
            queue.add(priority);
        }

        Priority prevPriority = null;
        while (!queue.isEmpty()) {
            if (prevPriority == null) {
                prevPriority = queue.poll();
            } else {
                assertThat(queue.poll().after(prevPriority), is(true));
            }
        }
    }

    public void testSubmitPrioritizedExecutorWithRunnables() throws Exception {
        ExecutorService executor = EsExecutors.newSinglePrioritizing(getTestName(), EsExecutors.daemonThreadFactory(getTestName()), holder, null);
        List<Integer> results = new ArrayList<>(8);
        CountDownLatch awaitingLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(8);
        executor.submit(new AwaitingJob(awaitingLatch));
        executor.submit(new Job(7, Priority.LANGUID, results, finishedLatch));
        executor.submit(new Job(5, Priority.LOW, results, finishedLatch));
        executor.submit(new Job(2, Priority.HIGH, results, finishedLatch));
        executor.submit(new Job(6, Priority.LOW, results, finishedLatch)); // will execute after the first LOW (fifo)
        executor.submit(new Job(1, Priority.URGENT, results, finishedLatch));
        executor.submit(new Job(4, Priority.NORMAL, results, finishedLatch));
        executor.submit(new Job(3, Priority.HIGH, results, finishedLatch)); // will execute after the first HIGH (fifo)
        executor.submit(new Job(0, Priority.IMMEDIATE, results, finishedLatch));
        awaitingLatch.countDown();
        finishedLatch.await();

        assertThat(results.size(), equalTo(8));
        assertThat(results.get(0), equalTo(0));
        assertThat(results.get(1), equalTo(1));
        assertThat(results.get(2), equalTo(2));
        assertThat(results.get(3), equalTo(3));
        assertThat(results.get(4), equalTo(4));
        assertThat(results.get(5), equalTo(5));
        assertThat(results.get(6), equalTo(6));
        assertThat(results.get(7), equalTo(7));
        terminate(executor);
    }

    public void testExecutePrioritizedExecutorWithRunnables() throws Exception {
        ExecutorService executor = EsExecutors.newSinglePrioritizing(getTestName(), EsExecutors.daemonThreadFactory(getTestName()), holder, null);
        List<Integer> results = new ArrayList<>(8);
        CountDownLatch awaitingLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(8);
        executor.execute(new AwaitingJob(awaitingLatch));
        executor.execute(new Job(7, Priority.LANGUID, results, finishedLatch));
        executor.execute(new Job(5, Priority.LOW, results, finishedLatch));
        executor.execute(new Job(2, Priority.HIGH, results, finishedLatch));
        executor.execute(new Job(6, Priority.LOW, results, finishedLatch)); // will execute after the first LOW (fifo)
        executor.execute(new Job(1, Priority.URGENT, results, finishedLatch));
        executor.execute(new Job(4, Priority.NORMAL, results, finishedLatch));
        executor.execute(new Job(3, Priority.HIGH, results, finishedLatch)); // will execute after the first HIGH (fifo)
        executor.execute(new Job(0, Priority.IMMEDIATE, results, finishedLatch));
        awaitingLatch.countDown();
        finishedLatch.await();

        assertThat(results.size(), equalTo(8));
        assertThat(results.get(0), equalTo(0));
        assertThat(results.get(1), equalTo(1));
        assertThat(results.get(2), equalTo(2));
        assertThat(results.get(3), equalTo(3));
        assertThat(results.get(4), equalTo(4));
        assertThat(results.get(5), equalTo(5));
        assertThat(results.get(6), equalTo(6));
        assertThat(results.get(7), equalTo(7));
        terminate(executor);
    }

    public void testSubmitPrioritizedExecutorWithCallables() throws Exception {
        ExecutorService executor = EsExecutors.newSinglePrioritizing(getTestName(), EsExecutors.daemonThreadFactory(getTestName()), holder, null);
        List<Integer> results = new ArrayList<>(8);
        CountDownLatch awaitingLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(8);
        executor.submit(new AwaitingJob(awaitingLatch));
        executor.submit(new CallableJob(7, Priority.LANGUID, results, finishedLatch));
        executor.submit(new CallableJob(5, Priority.LOW, results, finishedLatch));
        executor.submit(new CallableJob(2, Priority.HIGH, results, finishedLatch));
        executor.submit(new CallableJob(6, Priority.LOW, results, finishedLatch)); // will execute after the first LOW (fifo)
        executor.submit(new CallableJob(1, Priority.URGENT, results, finishedLatch));
        executor.submit(new CallableJob(4, Priority.NORMAL, results, finishedLatch));
        executor.submit(new CallableJob(3, Priority.HIGH, results, finishedLatch)); // will execute after the first HIGH (fifo)
        executor.submit(new CallableJob(0, Priority.IMMEDIATE, results, finishedLatch));
        awaitingLatch.countDown();
        finishedLatch.await();

        assertThat(results.size(), equalTo(8));
        assertThat(results.get(0), equalTo(0));
        assertThat(results.get(1), equalTo(1));
        assertThat(results.get(2), equalTo(2));
        assertThat(results.get(3), equalTo(3));
        assertThat(results.get(4), equalTo(4));
        assertThat(results.get(5), equalTo(5));
        assertThat(results.get(6), equalTo(6));
        assertThat(results.get(7), equalTo(7));
        terminate(executor);
    }

    public void testSubmitPrioritizedExecutorWithMixed() throws Exception {
        ExecutorService executor = EsExecutors.newSinglePrioritizing(getTestName(), EsExecutors.daemonThreadFactory(getTestName()), holder, null);
        List<Integer> results = new ArrayList<>(8);
        CountDownLatch awaitingLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(8);
        executor.submit(new AwaitingJob(awaitingLatch));
        executor.submit(new CallableJob(7, Priority.LANGUID, results, finishedLatch));
        executor.submit(new Job(5, Priority.LOW, results, finishedLatch));
        executor.submit(new CallableJob(2, Priority.HIGH, results, finishedLatch));
        executor.submit(new Job(6, Priority.LOW, results, finishedLatch)); // will execute after the first LOW (fifo)
        executor.submit(new CallableJob(1, Priority.URGENT, results, finishedLatch));
        executor.submit(new Job(4, Priority.NORMAL, results, finishedLatch));
        executor.submit(new CallableJob(3, Priority.HIGH, results, finishedLatch)); // will execute after the first HIGH (fifo)
        executor.submit(new Job(0, Priority.IMMEDIATE, results, finishedLatch));
        awaitingLatch.countDown();
        finishedLatch.await();

        assertThat(results.size(), equalTo(8));
        assertThat(results.get(0), equalTo(0));
        assertThat(results.get(1), equalTo(1));
        assertThat(results.get(2), equalTo(2));
        assertThat(results.get(3), equalTo(3));
        assertThat(results.get(4), equalTo(4));
        assertThat(results.get(5), equalTo(5));
        assertThat(results.get(6), equalTo(6));
        assertThat(results.get(7), equalTo(7));
        terminate(executor);
    }

    public void testTimeout() throws Exception {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(EsExecutors.daemonThreadFactory(getTestName()));
        PrioritizedEsThreadPoolExecutor executor = EsExecutors.newSinglePrioritizing(getTestName(), EsExecutors.daemonThreadFactory(getTestName()), holder, timer);
        final CountDownLatch invoked = new CountDownLatch(1);
        final CountDownLatch block = new CountDownLatch(1);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    invoked.countDown();
                    block.await();
                } catch (InterruptedException e) {
                    fail();
                }
            }

            @Override
            public String toString() {
                return "the blocking";
            }
        });
        invoked.await();
        PrioritizedEsThreadPoolExecutor.Pending[] pending = executor.getPending();
        assertThat(pending.length, equalTo(1));
        assertThat(pending[0].task.toString(), equalTo("the blocking"));
        assertThat(pending[0].executing, equalTo(true));

        final AtomicBoolean executeCalled = new AtomicBoolean();
        final CountDownLatch timedOut = new CountDownLatch(1);
        executor.execute(new Runnable() {
                             @Override
                             public void run() {
                                 executeCalled.set(true);
                             }

                             @Override
                             public String toString() {
                                 return "the waiting";
                             }
                         }, TimeValue.timeValueMillis(100) /* enough timeout to catch them in the pending list... */, new Runnable() {
                    @Override
                    public void run() {
                        timedOut.countDown();
                    }
                }
        );

        pending = executor.getPending();
        assertThat(pending.length, equalTo(2));
        assertThat(pending[0].task.toString(), equalTo("the blocking"));
        assertThat(pending[0].executing, equalTo(true));
        assertThat(pending[1].task.toString(), equalTo("the waiting"));
        assertThat(pending[1].executing, equalTo(false));

        assertThat(timedOut.await(2, TimeUnit.SECONDS), equalTo(true));
        block.countDown();
        Thread.sleep(100); // sleep a bit to double check that execute on the timed out update task is not called...
        assertThat(executeCalled.get(), equalTo(false));
        assertTrue(terminate(timer, executor));
    }

    public void testTimeoutCleanup() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test");
        final ScheduledThreadPoolExecutor timer = (ScheduledThreadPoolExecutor) threadPool.scheduler();
        final AtomicBoolean timeoutCalled = new AtomicBoolean();
        PrioritizedEsThreadPoolExecutor executor = EsExecutors.newSinglePrioritizing(getTestName(), EsExecutors.daemonThreadFactory(getTestName()), holder, timer);
        final CountDownLatch invoked = new CountDownLatch(1);
        executor.execute(new Runnable() {
                             @Override
                             public void run() {
                                 invoked.countDown();
                             }
                         }, TimeValue.timeValueHours(1), new Runnable() {
                    @Override
                    public void run() {
                        // We should never get here
                        timeoutCalled.set(true);
                    }
                }
        );
        invoked.await();

        // the timeout handler is added post execution (and quickly cancelled). We have allow for this
        // and use assert busy
        assertBusy(() -> assertThat(timer.getQueue().size(), equalTo(0)), 5, TimeUnit.SECONDS);
        assertThat(timeoutCalled.get(), equalTo(false));
        assertTrue(terminate(executor));
        assertTrue(terminate(threadPool));
    }

    static class AwaitingJob extends PrioritizedRunnable {

        private final CountDownLatch latch;

        private AwaitingJob(CountDownLatch latch) {
            super(Priority.URGENT);
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class Job extends PrioritizedRunnable {

        private final int result;
        private final List<Integer> results;
        private final CountDownLatch latch;

        Job(int result, Priority priority, List<Integer> results, CountDownLatch latch) {
            super(priority);
            this.result = result;
            this.results = results;
            this.latch = latch;
        }

        @Override
        public void run() {
            results.add(result);
            latch.countDown();
        }
    }

    static class CallableJob extends PrioritizedCallable<Integer> {

        private final int result;
        private final List<Integer> results;
        private final CountDownLatch latch;

        CallableJob(int result, Priority priority, List<Integer> results, CountDownLatch latch) {
            super(priority);
            this.result = result;
            this.results = results;
            this.latch = latch;
        }

        @Override
        public Integer call() throws Exception {
            results.add(result);
            latch.countDown();
            return result;
        }

    }
}
