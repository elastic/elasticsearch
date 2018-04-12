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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SnifferTests extends RestClientTestCase {

    /**
     * Test multiple sniffing rounds by mocking the {@link Scheduler} as well as
     * the {@link HostsSniffer}.
     * The {@link CountingHostsSniffer} doesn't make any connection but may throw exception or return no hosts.
     * The {@link Scheduler} implementation doesn't respect requested sniff delays but rather immediately runs them while
     * allowing to assert that the requested delay for each schedule run is the expected one.
     */
    public void testOrdinarySniffRounds() throws Exception {
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        final int iters = randomIntBetween(30, 100);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean submitCalled = new AtomicBoolean(false);
        final AtomicInteger runs = new AtomicInteger(iters);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        Scheduler scheduler = new Scheduler() {
            @Override
            public void submit(Runnable runnable) {
                //the first call is to "submit" the first sniff round from the Sniffer constructor
                assertTrue(submitCalled.compareAndSet(false, true));
                assertEquals(iters, runs.getAndDecrement());
                executor.execute(runnable);
            }

            @Override
            public void schedule(Runnable runnable, long delayMillis) {
                int numberOfRuns = runs.getAndDecrement();
                if (numberOfRuns == 0) {
                    latch.countDown();
                    return;
                }
                assertThat(numberOfRuns, lessThan(iters));
                //all of the subsequent times "schedule" is called with delay set to the configured sniff interval
                assertEquals(sniffInterval, delayMillis);
                //we immediately run rather than scheduling
                executor.execute(runnable);
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
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
        int totalRuns = hostsSniffer.runs.get();
        assertEquals(iters, totalRuns);
        int setHostsRuns = totalRuns - hostsSniffer.failures.get() - hostsSniffer.emptyList.get();
        verify(restClient, times(setHostsRuns)).setHosts(any(HttpHost.class));
        verifyNoMoreInteractions(restClient);
    }

    /**
     * Test that {@link Sniffer#close()} shuts down the underlying {@link Scheduler}, and that such calls are idempotent
     */
    public void testClose() {
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        final AtomicInteger shutdown = new AtomicInteger(0);
        Scheduler scheduler = new Scheduler() {
            @Override
            public void submit(Runnable runnable) {

            }

            @Override
            public void schedule(Runnable runnable, long delayMillis) {
            }

            @Override
            public void shutdown() {
                shutdown.incrementAndGet();
            }
        };

        Sniffer sniffer = new Sniffer(restClient, new MockHostsSniffer(), scheduler, sniffInterval, sniffAfterFailureDelay);
        assertEquals(0, shutdown.get());
        int iters = randomIntBetween(3, 10);
        for (int i = 0; i < iters; i++) {
            sniffer.close();
            assertEquals(i + 1, shutdown.get());
        }
    }

    /**
     * Test concurrent calls to the sniffing code. They should not happen as we are using a single threaded executor.
     * This test makes sure that concurrent calls are not supported and such assumption is enforced.
     */
    public void testConcurrentSniffRounds() throws Exception {
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();

        final int numThreads = randomIntBetween(10, 30);
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final Future[] futures = new Future[numThreads];
        try {
            Scheduler scheduler = new Scheduler() {
                @Override
                public void submit(Runnable runnable) {
                    for (int i = 0; i < numThreads; i++) {
                        //this will only submit the same runnable n times, to simulate concurrent calls to the sniffing code
                        futures[i] = executor.submit(runnable);
                    }
                }

                @Override
                public void schedule(Runnable runnable, long delayMillis) {
                    //do nothing
                }

                @Override
                public void shutdown() {
                }
            };
            new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }

        int failures = 0;
        int successfulRuns = 0;
        for (Future future : futures) {
            try {
                future.get();
                successfulRuns++;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnsupportedOperationException) {
                    failures++;
                } else {
                    throw e;
                }
            }
        }

        int totalRuns = hostsSniffer.runs.get();
        //out of n concurrent threads trying to run a sniff round, two will never be run in parallel.
        //That's why the total number of runs is less or equal to the number of threads.
        //The important part is also that the assertion on no concurrent runs in the below CountingHostsSniffer never trips
        assertThat(totalRuns, lessThanOrEqualTo(numThreads));
        assertThat(successfulRuns, greaterThan(0));
        //the successful runs that we counted match with the hosts sniffer calls
        assertThat(totalRuns, equalTo(successfulRuns));
        //and the failures that we counted due to the another thread already sniffing are the rest of the threads
        assertThat(numThreads - totalRuns, equalTo(failures));
        //also check out how many times setHosts is called on the underlying client
        int setHostsRuns = totalRuns - hostsSniffer.failures.get() - hostsSniffer.emptyList.get();
        verify(restClient, times(setHostsRuns)).setHosts(any(HttpHost.class));
        verifyNoMoreInteractions(restClient);
    }

    public void testSniffOnFailure() throws Exception {
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicInteger submitRuns = new AtomicInteger(0);
        final AtomicBoolean sniffingOnFailure = new AtomicBoolean(false);
        Scheduler scheduler = new Scheduler() {
            @Override
            public void submit(Runnable runnable) {
                submitRuns.incrementAndGet();
            }

            @Override
            public void schedule(Runnable runnable, long delayMillis) {

            }

            @Override
            public void shutdown() {

            }
        };
        int numThreads = randomIntBetween(10, 20);
        final ExecutorService onFailureExecutor = Executors.newFixedThreadPool(numThreads);
        try {
            final Sniffer sniffer = new Sniffer(restClient, hostsSniffer, scheduler, sniffInterval, sniffAfterFailureDelay);
            for (int i = 0; i < numThreads; i++) {
                onFailureExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        sniffer.sniffOnFailure();
                    }
                });
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Mock {@link HostsSniffer} implementation used for testing, which most of the times return a fixed host.
     * It rarely throws exception or return an empty list of hosts, to make sure that such situations are properly handled.
     * It also asserts that it never gets called concurrently, based on the assumption that only one sniff run can be run
     * at a given point in time.
     */
    private static class CountingHostsSniffer implements HostsSniffer {
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicInteger runs = new AtomicInteger(0);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final AtomicInteger emptyList = new AtomicInteger(0);

        @Override
        public List<HttpHost> sniffHosts() throws IOException {
            //check that this method is never called concurrently
            assertTrue(running.compareAndSet(false, true));
            try {
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
            } finally {
                assertTrue(running.compareAndSet(true, false));
            }
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
