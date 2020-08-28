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

package org.elasticsearch.client.sniff;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientTestCase;
import org.elasticsearch.client.sniff.Sniffer.DefaultScheduler;
import org.elasticsearch.client.sniff.Sniffer.Scheduler;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SnifferTests extends RestClientTestCase {

    /**
     * Tests the {@link Sniffer#sniff()} method in isolation. Verifies that it uses the {@link NodesSniffer} implementation
     * to retrieve nodes and set them (when not empty) to the provided {@link RestClient} instance.
     */
    public void testSniff() throws IOException {
        Node initialNode = new Node(new HttpHost("localhost", 9200));
        try (RestClient restClient = RestClient.builder(initialNode).build()) {
            Scheduler noOpScheduler = new Scheduler() {
                @Override
                public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                    return mock(Future.class);
                }

                @Override
                public void shutdown() {

                }
            };
            CountingNodesSniffer nodesSniffer = new CountingNodesSniffer();
            int iters = randomIntBetween(5, 30);
            try (Sniffer sniffer = new Sniffer(restClient, nodesSniffer, noOpScheduler, 1000L, -1)){
                {
                    assertEquals(1, restClient.getNodes().size());
                    Node node = restClient.getNodes().get(0);
                    assertEquals("localhost", node.getHost().getHostName());
                    assertEquals(9200, node.getHost().getPort());
                }
                int emptyList = 0;
                int failures = 0;
                int runs = 0;
                List<Node> lastNodes = Collections.singletonList(initialNode);
                for (int i = 0; i < iters; i++) {
                    try {
                        runs++;
                        sniffer.sniff();
                        if (nodesSniffer.failures.get() > failures) {
                            failures++;
                            fail("should have failed given that nodesSniffer says it threw an exception");
                        } else if (nodesSniffer.emptyList.get() > emptyList) {
                            emptyList++;
                            assertEquals(lastNodes, restClient.getNodes());
                        } else {
                            assertNotEquals(lastNodes, restClient.getNodes());
                            List<Node> expectedNodes = CountingNodesSniffer.buildNodes(runs);
                            assertEquals(expectedNodes, restClient.getNodes());
                            lastNodes = restClient.getNodes();
                        }
                    } catch(IOException e) {
                        if (nodesSniffer.failures.get() > failures) {
                            failures++;
                            assertEquals("communication breakdown", e.getMessage());
                        }
                    }
                }
                assertEquals(nodesSniffer.emptyList.get(), emptyList);
                assertEquals(nodesSniffer.failures.get(), failures);
                assertEquals(nodesSniffer.runs.get(), runs);
            }
        }
    }

    /**
     * Test multiple sniffing rounds by mocking the {@link Scheduler} as well as the {@link NodesSniffer}.
     * Simulates the ordinary behaviour of {@link Sniffer} when sniffing on failure is not enabled.
     * The {@link CountingNodesSniffer} doesn't make any network connection but may throw exception or return no nodes, which makes
     * it possible to verify that errors are properly handled and don't affect subsequent runs and their scheduling.
     * The {@link Scheduler} implementation submits rather than scheduling tasks, meaning that it doesn't respect the requested sniff
     * delays while allowing to assert that the requested delays for each requested run and the following one are the expected values.
     */
    public void testOrdinarySniffRounds() throws Exception {
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        CountingNodesSniffer nodesSniffer = new CountingNodesSniffer();
        final int iters = randomIntBetween(30, 100);
        final Set<Future<?>> futures = new CopyOnWriteArraySet<>();
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger(iters);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<Future<?>> lastFuture = new AtomicReference<>();
        final AtomicReference<Sniffer.Task> lastTask = new AtomicReference<>();
        Scheduler scheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                assertEquals(sniffInterval, task.nextTaskDelay);
                int numberOfRuns = runs.getAndDecrement();
                if (numberOfRuns == iters) {
                    //the first call is to schedule the first sniff round from the Sniffer constructor, with delay O
                    assertEquals(0L, delayMillis);
                    assertEquals(sniffInterval, task.nextTaskDelay);
                } else {
                    //all of the subsequent times "schedule" is called with delay set to the configured sniff interval
                    assertEquals(sniffInterval, delayMillis);
                    assertEquals(sniffInterval, task.nextTaskDelay);
                    if (numberOfRuns == 0) {
                        completionLatch.countDown();
                        return null;
                    }
                }
                //we submit rather than scheduling to make the test quick and not depend on time
                Future<?> future = executor.submit(task);
                futures.add(future);
                if (numberOfRuns == 1) {
                    lastFuture.set(future);
                    lastTask.set(task);
                }
                return future;
            }

            @Override
            public void shutdown() {
                //the executor is closed externally, shutdown is tested separately
            }
        };
        try {
            new Sniffer(restClient, nodesSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            assertTrue("timeout waiting for sniffing rounds to be completed", completionLatch.await(1000, TimeUnit.MILLISECONDS));
            assertEquals(iters, futures.size());
            //the last future is the only one that may not be completed yet, as the count down happens
            //while scheduling the next round which is still part of the execution of the runnable itself.
            assertTrue(lastTask.get().hasStarted());
            lastFuture.get().get();
            for (Future<?> future : futures) {
                assertTrue(future.isDone());
                future.get();
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(1000, TimeUnit.MILLISECONDS));
        }
        int totalRuns = nodesSniffer.runs.get();
        assertEquals(iters, totalRuns);
        int setNodesRuns = totalRuns - nodesSniffer.failures.get() - nodesSniffer.emptyList.get();
        verify(restClient, times(setNodesRuns)).setNodes(anyCollectionOf(Node.class));
        verifyNoMoreInteractions(restClient);
    }

    /**
     * Test that {@link Sniffer#close()} shuts down the underlying {@link Scheduler}, and that such calls are idempotent.
     * Also verifies that the next scheduled round gets cancelled.
     */
    public void testClose() {
        final Future<?> future = mock(Future.class);
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        final AtomicInteger shutdown = new AtomicInteger(0);
        final AtomicBoolean initialized = new AtomicBoolean(false);
        Scheduler scheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                if (initialized.compareAndSet(false, true)) {
                    //run from the same thread so the sniffer gets for sure initialized and the scheduled task gets cancelled on close
                    task.run();
                }
                return future;
            }

            @Override
            public void shutdown() {
                shutdown.incrementAndGet();
            }
        };

        Sniffer sniffer = new Sniffer(restClient, new MockNodesSniffer(), scheduler, sniffInterval, sniffAfterFailureDelay);
        assertEquals(0, shutdown.get());
        int iters = randomIntBetween(3, 10);
        for (int i = 1; i <= iters; i++) {
            sniffer.close();
            verify(future, times(i)).cancel(false);
            assertEquals(i, shutdown.get());
        }
    }

    public void testSniffOnFailureNotInitialized() {
        RestClient restClient = mock(RestClient.class);
        CountingNodesSniffer nodesSniffer = new CountingNodesSniffer();
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        final AtomicInteger scheduleCalls = new AtomicInteger(0);
        Scheduler scheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                scheduleCalls.incrementAndGet();
                return null;
            }

            @Override
            public void shutdown() {
            }
        };

        Sniffer sniffer = new Sniffer(restClient, nodesSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
        for (int i = 0; i < 10; i++) {
            sniffer.sniffOnFailure();
        }
        assertEquals(1, scheduleCalls.get());
        int totalRuns = nodesSniffer.runs.get();
        assertEquals(0, totalRuns);
        int setNodesRuns = totalRuns - nodesSniffer.failures.get() - nodesSniffer.emptyList.get();
        verify(restClient, times(setNodesRuns)).setNodes(anyCollectionOf(Node.class));
        verifyNoMoreInteractions(restClient);
    }

    /**
     * Test behaviour when a bunch of onFailure sniffing rounds are triggered in parallel. Each run will always
     * schedule a subsequent afterFailure round. Also, for each onFailure round that starts, the net scheduled round
     * (either afterFailure or ordinary) gets cancelled.
     */
    public void testSniffOnFailure() throws Exception {
        RestClient restClient = mock(RestClient.class);
        CountingNodesSniffer nodesSniffer = new CountingNodesSniffer();
        final AtomicBoolean initializing = new AtomicBoolean(true);
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        final long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        int minNumOnFailureRounds = randomIntBetween(5, 10);
        final CountDownLatch initializingLatch = new CountDownLatch(1);
        final Set<Sniffer.ScheduledTask> ordinaryRoundsTasks = new CopyOnWriteArraySet<>();
        final AtomicReference<Future<?>> initializingFuture = new AtomicReference<>();
        final Set<Sniffer.ScheduledTask> onFailureTasks = new CopyOnWriteArraySet<>();
        final Set<Sniffer.ScheduledTask> afterFailureTasks = new CopyOnWriteArraySet<>();
        final AtomicBoolean onFailureCompleted = new AtomicBoolean(false);
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            Scheduler scheduler = new Scheduler() {
                @Override
                public Future<?> schedule(final Sniffer.Task task, long delayMillis) {
                    if (initializing.compareAndSet(true, false)) {
                        assertEquals(0L, delayMillis);
                        Future<?> future = executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    task.run();
                                } finally {
                                    //we need to make sure that the sniffer is initialized, so the sniffOnFailure
                                    //call does what it needs to do. Otherwise nothing happens until initialized.
                                    initializingLatch.countDown();
                                }
                            }
                        });
                        assertTrue(initializingFuture.compareAndSet(null, future));
                        return future;
                    }
                    if (delayMillis == 0L) {
                        Future<?> future = executor.submit(task);
                        onFailureTasks.add(new Sniffer.ScheduledTask(task, future));
                        return future;
                    }
                    if (delayMillis == sniffAfterFailureDelay) {
                        Future<?> future = scheduleOrSubmit(task);
                        afterFailureTasks.add(new Sniffer.ScheduledTask(task, future));
                        return future;
                    }

                    assertEquals(sniffInterval, delayMillis);
                    assertEquals(sniffInterval, task.nextTaskDelay);

                    if (onFailureCompleted.get() && onFailureTasks.size() == afterFailureTasks.size()) {
                        completionLatch.countDown();
                        return mock(Future.class);
                    }

                    Future<?> future = scheduleOrSubmit(task);
                    ordinaryRoundsTasks.add(new Sniffer.ScheduledTask(task, future));
                    return future;
                }

                private Future<?> scheduleOrSubmit(Sniffer.Task task) {
                    if (randomBoolean()) {
                        return executor.schedule(task, randomLongBetween(0L, 200L), TimeUnit.MILLISECONDS);
                    } else {
                        return executor.submit(task);
                    }
                }

                @Override
                public void shutdown() {
                }
            };
            final Sniffer sniffer = new Sniffer(restClient, nodesSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            assertTrue("timeout waiting for sniffer to get initialized", initializingLatch.await(1000, TimeUnit.MILLISECONDS));

            ExecutorService onFailureExecutor = Executors.newFixedThreadPool(randomIntBetween(5, 20));
            Set<Future<?>> onFailureFutures = new CopyOnWriteArraySet<>();
            try {
                //with tasks executing quickly one after each other, it is very likely that the onFailure round gets skipped
                //as another round is already running. We retry till enough runs get through as that's what we want to test.
                while (onFailureTasks.size() < minNumOnFailureRounds) {
                    onFailureFutures.add(onFailureExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            sniffer.sniffOnFailure();
                        }
                    }));
                }
                assertThat(onFailureFutures.size(), greaterThanOrEqualTo(minNumOnFailureRounds));
                for (Future<?> onFailureFuture : onFailureFutures) {
                    assertNull(onFailureFuture.get());
                }
                onFailureCompleted.set(true);
            } finally {
                onFailureExecutor.shutdown();
                onFailureExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            }

            assertFalse(initializingFuture.get().isCancelled());
            assertTrue(initializingFuture.get().isDone());
            assertNull(initializingFuture.get().get());

            assertTrue("timeout waiting for sniffing rounds to be completed", completionLatch.await(1000, TimeUnit.MILLISECONDS));
            assertThat(onFailureTasks.size(), greaterThanOrEqualTo(minNumOnFailureRounds));
            assertEquals(onFailureTasks.size(), afterFailureTasks.size());

            for (Sniffer.ScheduledTask onFailureTask : onFailureTasks) {
                assertFalse(onFailureTask.future.isCancelled());
                assertTrue(onFailureTask.future.isDone());
                assertNull(onFailureTask.future.get());
                assertTrue(onFailureTask.task.hasStarted());
                assertFalse(onFailureTask.task.isSkipped());
            }

            int cancelledTasks = 0;
            int completedTasks = onFailureTasks.size() + 1;
            for (Sniffer.ScheduledTask afterFailureTask : afterFailureTasks) {
                if (assertTaskCancelledOrCompleted(afterFailureTask)) {
                    completedTasks++;
                } else {
                    cancelledTasks++;
                }
            }

            assertThat(ordinaryRoundsTasks.size(), greaterThan(0));
            for (Sniffer.ScheduledTask task : ordinaryRoundsTasks) {
                if (assertTaskCancelledOrCompleted(task)) {
                    completedTasks++;
                } else {
                    cancelledTasks++;
                }
            }
            assertEquals(onFailureTasks.size(), cancelledTasks);

            assertEquals(completedTasks, nodesSniffer.runs.get());
            int setNodesRuns = nodesSniffer.runs.get() - nodesSniffer.failures.get() - nodesSniffer.emptyList.get();
            verify(restClient, times(setNodesRuns)).setNodes(anyCollectionOf(Node.class));
            verifyNoMoreInteractions(restClient);
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000L, TimeUnit.MILLISECONDS);
        }
    }

    private static boolean assertTaskCancelledOrCompleted(Sniffer.ScheduledTask task) throws ExecutionException, InterruptedException {
        if (task.task.isSkipped()) {
            assertTrue(task.future.isCancelled());
            try {
                task.future.get();
                fail("cancellation exception should have been thrown");
            } catch(CancellationException ignore) {
            }
            return false;
        } else {
            try {
                assertNull(task.future.get());
            } catch(CancellationException ignore) {
                assertTrue(task.future.isCancelled());
            }
            assertTrue(task.future.isDone());
            assertTrue(task.task.hasStarted());
            return true;
        }
    }

    public void testTaskCancelling() throws Exception {
        RestClient restClient = mock(RestClient.class);
        NodesSniffer nodesSniffer = mock(NodesSniffer.class);
        Scheduler noOpScheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                return null;
            }

            @Override
            public void shutdown() {
            }
        };
        Sniffer sniffer = new Sniffer(restClient, nodesSniffer, noOpScheduler, 0L, 0L);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            int numIters = randomIntBetween(50, 100);
            for (int i = 0; i < numIters; i++) {
                Sniffer.Task task = sniffer.new Task(0L);
                TaskWrapper wrapper = new TaskWrapper(task);
                Future<?> future;
                if (rarely()) {
                    future = executor.schedule(wrapper, randomLongBetween(0L, 200L), TimeUnit.MILLISECONDS);
                } else {
                    future = executor.submit(wrapper);
                }
                Sniffer.ScheduledTask scheduledTask = new Sniffer.ScheduledTask(task, future);
                boolean skip = scheduledTask.skip();
                try {
                    assertNull(future.get());
                } catch(CancellationException ignore) {
                    assertTrue(future.isCancelled());
                }

                if (skip) {
                    //the task was either cancelled before starting, in which case it will never start (thanks to Future#cancel),
                    //or skipped, in which case it will run but do nothing (thanks to Task#skip).
                    //Here we want to make sure that whenever skip returns true, the task either won't run or it won't do anything,
                    //otherwise we may end up with parallel sniffing tracks given that each task schedules the following one. We need to
                    // make sure that onFailure takes scheduling over while at the same time ordinary rounds don't go on.
                    assertFalse(task.hasStarted());
                    assertTrue(task.isSkipped());
                    assertTrue(future.isCancelled());
                    assertTrue(future.isDone());
                } else {
                    //if a future is cancelled when its execution has already started, future#get throws CancellationException before
                    //completion. The execution continues though so we use a latch to try and wait for the task to be completed.
                    //Here we want to make sure that whenever skip returns false, the task will be completed, otherwise we may be
                    //missing to schedule the following round, which means no sniffing will ever happen again besides on failure sniffing.
                    assertTrue(wrapper.await());
                    //the future may or may not be cancelled but the task has for sure started and completed
                    assertTrue(task.toString(), task.hasStarted());
                    assertFalse(task.isSkipped());
                    assertTrue(future.isDone());
                }
                //subsequent cancel calls return false for sure
                int cancelCalls = randomIntBetween(1, 10);
                for (int j = 0; j < cancelCalls; j++) {
                    assertFalse(scheduledTask.skip());
                }
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Wraps a {@link Sniffer.Task} and allows to wait for its completion. This is needed to verify
     * that tasks are either never started or always completed. Calling {@link Future#get()} against a cancelled future will
     * throw {@link CancellationException} straight-away but the execution of the task will continue if it had already started,
     * in which case {@link Future#cancel(boolean)} returns true which is not very helpful.
     */
    private static final class TaskWrapper implements Runnable {
        final Sniffer.Task task;
        final CountDownLatch completionLatch = new CountDownLatch(1);

        TaskWrapper(Sniffer.Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                task.run();
            } finally {
                completionLatch.countDown();
            }
        }

        boolean await() throws InterruptedException {
            return completionLatch.await(1000, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Mock {@link NodesSniffer} implementation used for testing, which most of the times return a fixed node.
     * It rarely throws exception or return an empty list of nodes, to make sure that such situations are properly handled.
     * It also asserts that it never gets called concurrently, based on the assumption that only one sniff run can be run
     * at a given point in time.
     */
    private static class CountingNodesSniffer implements NodesSniffer {
        private final AtomicInteger runs = new AtomicInteger(0);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final AtomicInteger emptyList = new AtomicInteger(0);

        @Override
        public List<Node> sniff() throws IOException {
            int run = runs.incrementAndGet();
            if (rarely()) {
                failures.incrementAndGet();
                //check that if communication breaks, sniffer keeps on working
                throw new IOException("communication breakdown");
            }
            if (rarely()) {
                emptyList.incrementAndGet();
                return Collections.emptyList();
            }
            return buildNodes(run);
        }

        private static List<Node> buildNodes(int run) {
            int size = run % 5 + 1;
            assert size > 0;
            List<Node> nodes = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                nodes.add(new Node(new HttpHost("sniffed-" + run, 9200 + i)));
            }
            return nodes;
        }
    }

    public void testDefaultSchedulerSchedule() {
        RestClient restClient = mock(RestClient.class);
        NodesSniffer nodesSniffer = mock(NodesSniffer.class);
        Scheduler noOpScheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                return mock(Future.class);
            }

            @Override
            public void shutdown() {

            }
        };
        Sniffer sniffer = new Sniffer(restClient, nodesSniffer, noOpScheduler, 0L, 0L);
        Sniffer.Task task = sniffer.new Task(randomLongBetween(1, Long.MAX_VALUE));

        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);
        when(scheduledExecutorService.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
                .then(new Answer<ScheduledFuture<?>>() {
                    @Override
                    public ScheduledFuture<?> answer(InvocationOnMock invocationOnMock) {
                        return mockedFuture;
                    }
        });
        DefaultScheduler scheduler = new DefaultScheduler(scheduledExecutorService);
        long delay = randomLongBetween(1, Long.MAX_VALUE);
        Future<?> future = scheduler.schedule(task, delay);
        assertSame(mockedFuture, future);
        verify(scheduledExecutorService).schedule(task, delay, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(scheduledExecutorService, mockedFuture);
    }

    public void testDefaultSchedulerThreadFactory() {
        DefaultScheduler defaultScheduler = new DefaultScheduler();
        try {
            ScheduledExecutorService executorService = defaultScheduler.executor;
            assertThat(executorService, instanceOf(ScheduledThreadPoolExecutor.class));
            assertThat(executorService, instanceOf(ScheduledThreadPoolExecutor.class));
            ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) executorService;
            assertTrue(executor.getRemoveOnCancelPolicy());
            assertFalse(executor.getContinueExistingPeriodicTasksAfterShutdownPolicy());
            assertTrue(executor.getExecuteExistingDelayedTasksAfterShutdownPolicy());
            assertThat(executor.getThreadFactory(), instanceOf(Sniffer.SnifferThreadFactory.class));
            int iters = randomIntBetween(3, 10);
            for (int i = 1; i <= iters; i++) {
                Thread thread = executor.getThreadFactory().newThread(new Runnable() {
                    @Override
                    public void run() {

                    }
                });
                assertThat(thread.getName(), equalTo("es_rest_client_sniffer[T#" + i + "]"));
                assertThat(thread.isDaemon(), is(true));
            }
        } finally {
            defaultScheduler.shutdown();
        }
    }

    public void testDefaultSchedulerShutdown() throws Exception {
        ScheduledThreadPoolExecutor executor = mock(ScheduledThreadPoolExecutor.class);
        DefaultScheduler defaultScheduler = new DefaultScheduler(executor);
        defaultScheduler.shutdown();
        verify(executor).shutdown();
        verify(executor).awaitTermination(1000, TimeUnit.MILLISECONDS);
        verify(executor).shutdownNow();
        verifyNoMoreInteractions(executor);

        when(executor.awaitTermination(1000, TimeUnit.MILLISECONDS)).thenReturn(true);
        defaultScheduler.shutdown();
        verify(executor, times(2)).shutdown();
        verify(executor, times(2)).awaitTermination(1000, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(executor);
    }
}
