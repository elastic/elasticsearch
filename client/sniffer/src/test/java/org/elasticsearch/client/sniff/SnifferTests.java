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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            Scheduler noOpScheduler = new Scheduler() {
                @Override
                public Sniffer.ScheduledTask schedule(Sniffer.Task task, long delayMillis) {
                    return new Sniffer.ScheduledTask(task, null);
                }

                @Override
                public void shutdown() {

                }
            };
            HostsSniffer emptyHostsSniffer = new HostsSniffer() {
                @Override
                public List<HttpHost> sniffHosts() {
                    return Collections.emptyList();
                }
            };
            Sniffer sniffer = new Sniffer(restClient, emptyHostsSniffer, noOpScheduler, 0, 0);
            {
                assertEquals(1, restClient.getHosts().size());
                HttpHost httpHost = restClient.getHosts().get(0);
                assertEquals("localhost", httpHost.getHostName());
                assertEquals(9200, httpHost.getPort());
            }
            sniffer.sniff();
            {
                assertEquals(1, restClient.getHosts().size());
                HttpHost httpHost = restClient.getHosts().get(0);
                assertEquals("localhost", httpHost.getHostName());
                assertEquals(9200, httpHost.getPort());
            }

            HostsSniffer hostsSniffer = new HostsSniffer() {
                @Override
                public List<HttpHost> sniffHosts() {
                    return Collections.singletonList(new HttpHost("sniffed", 9210));
                }
            };
            new Sniffer(restClient, hostsSniffer, noOpScheduler, 0, 0).sniff();
            {
                assertEquals(1, restClient.getHosts().size());
                HttpHost httpHost = restClient.getHosts().get(0);
                assertEquals("sniffed", httpHost.getHostName());
                assertEquals(9210, httpHost.getPort());
            }

            HostsSniffer throwingHostsSniffer = new HostsSniffer() {
                @Override
                public List<HttpHost> sniffHosts() throws IOException {
                    throw new IOException("communication breakdown");
                }
            };
            Sniffer throwingSniffer = new Sniffer(restClient, throwingHostsSniffer, noOpScheduler, 0, 0);
            try {
                throwingSniffer.sniff();
                fail("should have failed");
            } catch(IOException e) {
                assertEquals("communication breakdown", e.getMessage());
            }
            {
                assertEquals(1, restClient.getHosts().size());
                HttpHost httpHost = restClient.getHosts().get(0);
                assertEquals("sniffed", httpHost.getHostName());
                assertEquals(9210, httpHost.getPort());
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
            public Sniffer.ScheduledTask schedule(Sniffer.Task task, long delayMillis) {
                assertEquals(sniffInterval, task.nextTaskDelay);
                int numberOfRuns = runs.getAndDecrement();
                if (numberOfRuns == 0) {
                    latch.countDown();
                    return new Sniffer.ScheduledTask(task, null);
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
                return new Sniffer.ScheduledTask(task, future);
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
        verify(restClient, times(setHostsRuns)).setHosts(any(HttpHost.class));
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
            public Sniffer.ScheduledTask schedule(Sniffer.Task task, long delayMillis) {
                return new Sniffer.ScheduledTask(task, future);
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

    //TODO test what happens when on failure is called while another round is already running https://github.com/elastic/elasticsearch/issues/27697

    //TODO test more on failure rounds?

    //TODO do we need ScheduledTask or is the future enough?

    //TODO test that sniffOnFailure doesn't do any IO blocking. https://github.com/elastic/elasticsearch/issues/25701

    public void test() throws Exception {
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
                public Sniffer.ScheduledTask schedule(Sniffer.Task task, long delayMillis) {
                    if (initializing.compareAndSet(true, false)) {
                        assertEquals(0L, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                        initializingLatch.countDown();
                        return new Sniffer.ScheduledTask(task, mockedFuture);
                    }
                    if (ongoingOnFailure.compareAndSet(true, false)) {
                        assertEquals(0L, delayMillis);
                        assertEquals(sniffAfterFailureDelay, task.nextTaskDelay);
                        afterFailureExpected.set(true);
                        Future<?> future = executor.submit(task);
                        futures.add(future);
                        return new Sniffer.ScheduledTask(task, future);
                    } else if (afterFailureExpected.compareAndSet(true, false)) {
                        //onFailure is called in another thread (not from the single threaded executor), hence we need to wait for
                        //it to be completed or the rest of the rounds (after failure etc.) may be executed before the onFailure
                        //scheduling is completed. This is a problem only when testing as we submit tasks instead of scheduling.
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
                        return new Sniffer.ScheduledTask(task, future);
                    } else {
                        assertEquals(sniffInterval, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                        doneLatch.countDown();
                        return new Sniffer.ScheduledTask(task, null);
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
            verify(restClient, times(setHostsRuns)).setHosts(any(HttpHost.class));
            verifyNoMoreInteractions(restClient);

        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(1000, TimeUnit.MILLISECONDS));
        }
    }

    public void testSniffOnFailure() throws Exception {
        RestClient restClient = mock(RestClient.class);

        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        int ordinaryRuns = randomIntBetween(20, 50);
        int onFailureRuns = randomIntBetween(5, 10);

        final AtomicBoolean initializing = new AtomicBoolean(true);
        final AtomicInteger runs = new AtomicInteger(ordinaryRuns);
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        final long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        final List<Future<?>> ordinaryFutures = new CopyOnWriteArrayList<>();
        final List<Future<?>> onFailureFutures = new CopyOnWriteArrayList<>();
        final List<Future<?>> afterFailureFutures = new CopyOnWriteArrayList<>();
        final AtomicInteger ongoingOnFailures = new AtomicInteger(0);

        final AtomicBoolean sniffAfterFailure = new AtomicBoolean(false);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        Scheduler scheduler = new Scheduler() {
            @Override
            public Sniffer.ScheduledTask schedule(Sniffer.Task task, long delayMillis) {
                int numberOfRuns = runs.getAndDecrement();
                if (numberOfRuns == 0) {
                    return new Sniffer.ScheduledTask(task, null);
                }

                Future<?> future = executor.submit(task);
                if (ongoingOnFailures.get() > 0) {
                    onFailureFutures.add(future);
                    assertEquals(0L, delayMillis);
                    assertEquals(sniffAfterFailureDelay, task.nextTaskDelay);
                    sniffAfterFailure.set(true);
                } else if (sniffAfterFailure.compareAndSet(true, false)) {
                    afterFailureFutures.add(future);
                    assertEquals(sniffAfterFailureDelay, delayMillis);
                    assertEquals(sniffInterval, task.nextTaskDelay);
                } else {
                    ordinaryFutures.add(future);
                    if (initializing.compareAndSet(true, false)) {
                        assertEquals(0L, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                    } else {
                        assertEquals(sniffInterval, delayMillis);
                        assertEquals(sniffInterval, task.nextTaskDelay);
                    }
                }
                return new Sniffer.ScheduledTask(task, future);
            }

            @Override
            public void shutdown() {

            }
        };


        ExecutorService onFailureExecutor = Executors.newFixedThreadPool(randomIntBetween(1, onFailureRuns));

        try {
            final Sniffer sniffer = new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            Future[] onFailureExecutorFutures = new Future[onFailureRuns];
            for (int i = 0; i < onFailureExecutorFutures.length; i++) {
                onFailureExecutorFutures[i] = onFailureExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        ongoingOnFailures.incrementAndGet();
                        try {
                            sniffer.sniffOnFailure();
                        } finally {
                            ongoingOnFailures.decrementAndGet();
                        }
                    }
                });
            }
            for (Future onFailureFuture : onFailureExecutorFutures) {
                onFailureFuture.get();
            }
            assertEquals(0, ongoingOnFailures.get());
            assertFalse(sniffAfterFailure.get());
            assertEquals(ordinaryRuns, ordinaryFutures.size());
            for (Future<?> future : ordinaryFutures) {
                assertTrue("Future is cancelled: " + future.isCancelled(), future.isDone());
                try {
                    future.get();
                } catch(CancellationException e) {
                    //all good

                    //TODO shall we count these?
                }
            }

            //TODO check other futures
        } finally {
            onFailureExecutor.shutdown();
            assertTrue(onFailureExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS));
            executor.shutdown();
            assertTrue(executor.awaitTermination(1000, TimeUnit.MILLISECONDS));

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
            runs.incrementAndGet();
            if (rarely()) {
                failures.incrementAndGet();
                //check that if communication breaks, sniffer keeps on working
                throw new IOException("communication breakdown");
            }
            if (rarely()) {
                emptyList.incrementAndGet();
                return Collections.emptyList();
            }
            return Collections.singletonList(new HttpHost("localhost", 9200));
        }
    }

    //TODO adapt this test
    @SuppressWarnings("unchecked")
/*
    public void testDefaultSchedulerSchedule() {
        ScheduledThreadPoolExecutor executor = mock(ScheduledThreadPoolExecutor.class);
        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(executor.isShutdown()).thenReturn(false);
        when(executor.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class))).thenReturn(scheduledFuture);

        DefaultScheduler defaultScheduler = new DefaultScheduler(executor);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };
        int iters = randomIntBetween(3, 10);
        {
            for (int i = 1; i <= iters; i++) {
                long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
                defaultScheduler.schedule(runnable, delayMillis);
                verify(executor, times(i)).schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
                verifyNoMoreInteractions(executor);
                verify(scheduledFuture, times(i)).cancel(false);
                verifyNoMoreInteractions(scheduledFuture);
            }
        }
        {
            when(executor.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
                    .thenThrow(new IllegalArgumentException());
            long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
            defaultScheduler.schedule(runnable, delayMillis);
            verify(executor).schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
            verifyNoMoreInteractions(executor);
            verify(scheduledFuture, times(2)).cancel(false);
            verifyNoMoreInteractions(scheduledFuture);
        }
        {
            when(executor.isShutdown()).thenReturn(true);
            long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
            defaultScheduler.schedule(runnable, delayMillis);
            verifyNoMoreInteractions(executor);
            verifyNoMoreInteractions(scheduledFuture);
        }
    }
*/

    public void testDefaultSchedulerThreadFactory() {
        DefaultScheduler defaultScheduler = new DefaultScheduler();
        try {
            ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = defaultScheduler.executor;
            assertThat(scheduledThreadPoolExecutor.getThreadFactory(), instanceOf(Sniffer.SnifferThreadFactory.class));
            int iters = randomIntBetween(3, 10);
            for (int i = 1; i <= iters; i++) {
                Thread thread = scheduledThreadPoolExecutor.getThreadFactory().newThread(new Runnable() {
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

    //TODO adapt this, it's not a good test though
/*    @SuppressWarnings("unchecked")
    public void testDefaultSchedulerShutdown() throws Exception {
        ScheduledThreadPoolExecutor executor = mock(ScheduledThreadPoolExecutor.class);
        when(executor.isShutdown()).thenReturn(false);
        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(executor.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class))).thenReturn(scheduledFuture);
        DefaultScheduler defaultScheduler = new DefaultScheduler(executor);

        defaultScheduler.schedule(new Runnable() {
            @Override
            public void run() {

            }
        }, 0L);
        verify(executor.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)));

        defaultScheduler.shutdown();
        verify(scheduledFuture).cancel(false);
        verify(executor).shutdown();
        verify(executor).awaitTermination(1000, TimeUnit.MILLISECONDS);
        verify(executor).shutdownNow();
        verifyNoMoreInteractions(executor, scheduledFuture);

        when(executor.awaitTermination(1000, TimeUnit.MILLISECONDS)).thenReturn(true);
        defaultScheduler.shutdown();
        verify(scheduledFuture, times(2)).cancel(false);
        verify(executor, times(2)).shutdown();
        verify(executor, times(2)).awaitTermination(1000, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(executor, scheduledFuture);
    }*/

    //TODO test on failure intervals

    //TODO add a sniffer test against proper http server, or maybe just throw exception and trigger on failure

}
