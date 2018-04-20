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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientTestCase;
import org.elasticsearch.client.sniff.Sniffer.DefaultScheduler;
import org.elasticsearch.client.sniff.Sniffer.Scheduler;
import org.mockito.Matchers;
import org.mockito.exceptions.verification.WantedButNotInvoked;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SnifferTests extends RestClientTestCase {

    /**
     * Tests the {@link Sniffer#sniff()} method in isolation. Verifies that it uses the {@link HostsSniffer} implementation
     * to retrieve nodes and set them (when not empty) to the provided {@link RestClient} instance.
     */
    public void testSniff() throws IOException {
        HttpHost initialHost = new HttpHost("localhost", 9200);
        try (RestClient restClient = RestClient.builder(initialHost).build()) {
            Scheduler noOpScheduler = new Scheduler() {
                @Override
                public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                    return mock(Future.class);
                }

                @Override
                public void shutdown() {

                }
            };
            CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
            int iters = randomIntBetween(5, 30);
            try (Sniffer sniffer = new Sniffer(restClient, hostsSniffer, noOpScheduler, 1000L, -1)){
                {
                    assertEquals(1, restClient.getHosts().size());
                    HttpHost httpHost = restClient.getHosts().get(0);
                    assertEquals("localhost", httpHost.getHostName());
                    assertEquals(9200, httpHost.getPort());
                }
                int emptyList = 0;
                int failures = 0;
                int runs = 0;
                List<HttpHost> lastHosts = Collections.singletonList(initialHost);
                for (int i = 0; i < iters; i++) {
                    try {
                        runs++;
                        sniffer.sniff();
                        if (hostsSniffer.failures.get() > failures) {
                            failures++;
                            fail("should have failed");
                        } else if (hostsSniffer.emptyList.get() > emptyList) {
                            emptyList++;
                            assertEquals(lastHosts, restClient.getHosts());
                        } else {
                            assertNotEquals(lastHosts, restClient.getHosts());
                            List<HttpHost> expectedHosts = CountingHostsSniffer.buildHosts(runs);
                            assertEquals(expectedHosts, restClient.getHosts());
                            lastHosts = restClient.getHosts();
                        }
                    } catch(IOException e) {
                        if (hostsSniffer.failures.get() > failures) {
                            failures++;
                            assertEquals("communication breakdown", e.getMessage());
                        }
                    }
                }
                assertEquals(hostsSniffer.emptyList.get(), emptyList);
                assertEquals(hostsSniffer.failures.get(), failures);
                assertEquals(hostsSniffer.runs.get(), runs);
            }
        }
    }

    /**
     * Test multiple sniffing rounds by mocking the {@link Scheduler} as well as the {@link HostsSniffer}.
     * Simulates the ordinary behaviour of {@link Sniffer} when sniffing on failure is not enabled.
     * The {@link CountingHostsSniffer} doesn't make any network connection but may throw exception or return no hosts, which makes
     * it possible to verify that errors are properly handled and don't affect subsequent runs and their scheduling.
     * The {@link Scheduler} implementation submits rather than scheduling runs, meaning that it doesn't respect the requested sniff
     * delays while allowing to assert that the requested delays for each requested run and the following one are the expected values.
     */
    public void testOrdinarySniffRounds() throws Exception {
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        final int iters = randomIntBetween(30, 100);
        final List<Future<?>> futures = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger(iters);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<Future> lastFuture = new AtomicReference<>();
        Scheduler scheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                assertEquals(sniffInterval, task.nextTaskDelay);
                int numberOfRuns = runs.getAndDecrement();
                if (numberOfRuns == 0) {
                    latch.countDown();
                    return null;
                }
                if (numberOfRuns == iters) {
                    //the first call is to schedule the first sniff round from the Sniffer constructor, with delay O
                    assertEquals(0L, delayMillis);
                    assertEquals(sniffInterval, task.nextTaskDelay);
                } else {
                    //all of the subsequent times "schedule" is called with delay set to the configured sniff interval
                    assertEquals(sniffInterval, delayMillis);
                    assertEquals(sniffInterval, task.nextTaskDelay);
                }
                //we submit rather than scheduling to make the test quick and not depend on time
                Future<?> future = executor.submit(task);
                futures.add(future);
                if (numberOfRuns == 1) {
                    lastFuture.compareAndSet(null, future);
                }
                return future;
            }

            @Override
            public void shutdown() {
                //the executor is closed externally, shutdown is tested separately
            }
        };
        try {
            //all we need to do is initialize the sniffer, sniffing will start automatically in the background
            new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
            assertEquals(iters, futures.size());
            //the last future is the only one that may not be completed yet, as the count down happens
            //while scheduling the next round which is still part of the execution of the runnable itself.
            //we don't take the last item from the list as futures may be ouf of order in there
            lastFuture.get().get();
            for (Future<?> future : futures) {
                assertTrue(future.isDone());
                future.get();
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(1000, TimeUnit.MILLISECONDS));
        }
        int totalRuns = hostsSniffer.runs.get();
        assertEquals(iters, totalRuns);
        int setHostsRuns = totalRuns - hostsSniffer.failures.get() - hostsSniffer.emptyList.get();
        verify(restClient, times(setHostsRuns)).setHosts(Matchers.<HttpHost>anyVararg());
        verifyNoMoreInteractions(restClient);
    }

    /**
     * Test that {@link Sniffer#close()} shuts down the underlying {@link Scheduler}, and that such calls are idempotent.
     * Also verifies that the next scheduled round gets cancelled.
     */
    public void testClose() {
        final Future future = mock(Future.class);
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        final AtomicInteger shutdown = new AtomicInteger(0);
        Scheduler scheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                return future;
            }

            @Override
            public void shutdown() {
                shutdown.incrementAndGet();
            }
        };

        Sniffer sniffer = new Sniffer(restClient, new MockHostsSniffer(), scheduler, sniffInterval, sniffAfterFailureDelay);
        assertEquals(0, shutdown.get());
        int iters = randomIntBetween(3, 10);
        for (int i = 1; i <= iters; i++) {
            sniffer.close();
            verify(future, times(i)).cancel(false);
            assertEquals(i, shutdown.get());
        }
    }

    /**
     * Test calling {@link Sniffer#sniffOnFailure()} once. The next scheduled sniffing round is cancelled,
     * a new round is scheduled with delay 0, a following one is scheduled with sniffAfterFailure delay, after
     * which the ordinary sniffing rounds get scheduled with sniffInterval delay
     */
    public void testOnFailureSingleRound() throws Exception {
        final Future mockedFuture = mock(Future.class);
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        final long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        final AtomicBoolean initializing = new AtomicBoolean(true);
        final AtomicBoolean ongoingOnFailure = new AtomicBoolean(true);
        final AtomicBoolean afterFailureExpected = new AtomicBoolean(false);
        final CountDownLatch initializingLatch = new CountDownLatch(1);
        final CountDownLatch cancelLatch = new CountDownLatch(1);
        when(mockedFuture.cancel(false)).thenAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                cancelLatch.countDown();
                return null;
            }
        });
        final CountDownLatch doneLatch = new CountDownLatch(1);
        final List<Future> futures = new CopyOnWriteArrayList<>();
        final AtomicReference<Future> lastFuture = new AtomicReference<>();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Scheduler scheduler = new Scheduler() {
                @Override
                public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                    if (initializing.compareAndSet(true, false)) {
                        assertEquals(0L, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                        initializingLatch.countDown();
                        return mockedFuture;
                    }
                    if (ongoingOnFailure.compareAndSet(true, false)) {
                        assertEquals(0L, delayMillis);
                        assertEquals(sniffAfterFailureDelay, task.nextTaskDelay);
                        afterFailureExpected.set(true);
                        Future<?> future = executor.submit(task);
                        futures.add(future);
                        return future;
                    } else if (afterFailureExpected.compareAndSet(true, false)) {
                        //onFailure is called in another thread (not from the single threaded executor), hence we need to
                        //wait for it to be completed or the rest of the rounds (after failure etc.) may be executed
                        //before the onFailure scheduling is completed. This is a problem only when testing as we submit
                        //tasks instead of scheduling them with some delay.
                        try {
                            cancelLatch.await(500, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            fail(e.getMessage());
                        }
                        assertEquals(sniffAfterFailureDelay, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                        Future<?> future = executor.submit(task);
                        futures.add(future);
                        lastFuture.compareAndSet(null, future);
                        return future;
                    } else {
                        assertEquals(sniffInterval, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                        doneLatch.countDown();
                        return null;
                    }
                }

                @Override
                public void shutdown() {

                }
            };
            Sniffer sniffer = new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            assertTrue(initializingLatch.await(1000, TimeUnit.MILLISECONDS));

            sniffer.sniffOnFailure();

            assertTrue(doneLatch.await(1000, TimeUnit.MILLISECONDS));
            assertEquals(2, futures.size());
            lastFuture.get().get();
            for (Future future : futures) {
                assertTrue(future.isDone());
                future.get();
            }
            int totalRuns = hostsSniffer.runs.get();
            assertEquals(2, totalRuns);
            int setHostsRuns = totalRuns - hostsSniffer.failures.get() - hostsSniffer.emptyList.get();
            verify(restClient, times(setHostsRuns)).setHosts(Matchers.<HttpHost>anyVararg());
            verifyNoMoreInteractions(restClient);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(1000, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Test that when invoking {@link Sniffer#sniffOnFailure()} concurrently, the next scheduled sniffing round is always cancelled.
     */
    public void testSniffOnFailureCancelsNextRound() throws Exception {
        RestClient restClient = mock(RestClient.class);
        HostsSniffer hostsSniffer = mock(HostsSniffer.class);
        int onFailureRounds = randomIntBetween(30, 50);
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        final long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        final AtomicBoolean initialized = new AtomicBoolean(false);
        final List<Future> futures = new CopyOnWriteArrayList<>();
        Scheduler scheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                assertEquals(0L, delayMillis);
                if (initialized.compareAndSet(false, true)) {
                    assertEquals(sniffInterval, task.nextTaskDelay);
                } else {
                    assertEquals(sniffAfterFailureDelay, task.nextTaskDelay);
                }
                Future mockedFuture = mock(Future.class);
                futures.add(mockedFuture);
                return mockedFuture;
            }

            @Override
            public void shutdown() {

            }
        };

        final Sniffer sniffer = new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
        ExecutorService onFailureExecutor = Executors.newFixedThreadPool(randomIntBetween(2, 10));
        try {
            Future[] onFailureFutures = new Future[onFailureRounds];
            for (int i = 0; i < onFailureFutures.length; i++) {
                onFailureFutures[i] = onFailureExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        sniffer.sniffOnFailure();
                    }
                });
            }
            for (Future onFailureFuture : onFailureFutures) {
                onFailureFuture.get();
            }
        } finally {
            onFailureExecutor.shutdown();
            onFailureExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }

        //we have as many futures as the onFailure rounds, plus the initial one obtained at Sniffer construction
        assertEquals(onFailureRounds + 1, futures.size());
        boolean notInvokedFound = false;
        //all of these futures but one must be cancelled, one single time. The one that's not is the last scheduled but not
        //necessarily the last one in our list
        for (Future future : futures) {
            try {
                verify(future).cancel(false);
            } catch(WantedButNotInvoked e) {
                if (notInvokedFound) {
                    throw new AssertionError("future was cancelled more than once", e);
                }
                notInvokedFound = true;
            }
        }
    }

    /**
     * Test that no matter when an onFailure sniffing round is triggered, it will run and it will always
     * schedule a subsequent afterFailure round. See https://github.com/elastic/elasticsearch/issues/27697 .
     * The difference in this test compared to the other ones above is that we keep on scheduling ordinary sniffing
     * rounds to show that they don't interfere with the onFailure round that is scheduled at some point.
     */
    public void testSniffOnFailureIsAlwaysExecuted() throws Exception {
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        final long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        final CountDownLatch latch = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Scheduler scheduler = new Scheduler() {
                @Override
                public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                    //if we are scheduling a next round with delay sniffAfterFailureDelay it means that we are executing the onFailure task
                    if (delayMillis == sniffAfterFailureDelay) {
                        latch.countDown();
                        return null;
                    }
                    return executor.submit(task);
                }

                @Override
                public void shutdown() {

                }
            };
            Sniffer sniffer = new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            sniffer.sniffOnFailure();
            assertTrue(latch.await(1000L, TimeUnit.MILLISECONDS));
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000L, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Mock {@link HostsSniffer} implementation used for testing, which most of the times return a fixed host.
     * It rarely throws exception or return an empty list of hosts, to make sure that such situations are properly handled.
     * It also asserts that it never gets called concurrently, based on the assumption that only one sniff run can be run
     * at a given point in time.
     */
    private static class CountingHostsSniffer implements HostsSniffer {
        private final AtomicInteger runs = new AtomicInteger(0);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final AtomicInteger emptyList = new AtomicInteger(0);

        @Override
        public List<HttpHost> sniffHosts() throws IOException {
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
            return buildHosts(run);
        }

        private static List<HttpHost> buildHosts(int run) {
            int size = run % 5 + 1;
            assert size > 0;
            List<HttpHost> hosts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                hosts.add(new HttpHost("sniffed-" + run, 9200 + i));
            }
            return hosts;
        }
    }

    @SuppressWarnings("unchecked")
    public void testDefaultSchedulerSchedule() {
        RestClient restClient = mock(RestClient.class);
        HostsSniffer hostsSniffer = mock(HostsSniffer.class);
        Scheduler noOpScheduler = new Scheduler() {
            @Override
            public Future<?> schedule(Sniffer.Task task, long delayMillis) {
                return mock(Future.class);
            }

            @Override
            public void shutdown() {

            }
        };
        Sniffer sniffer = new Sniffer(restClient, hostsSniffer, noOpScheduler, 0L, 0L);
        Sniffer.Task task = sniffer.new Task(randomLongBetween(1, Long.MAX_VALUE));

        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        ScheduledFuture mockedFuture = mock(ScheduledFuture.class);
        when(scheduledExecutorService.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class))).thenReturn(mockedFuture);
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
