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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SnifferTests extends RestClientTestCase {

    /**
     * Test multiple sniffing rounds by mocking the {@link Scheduler} as well as
     * the {@link HostsSniffer}.
     * The {@link CountingHostsSniffer} doesn't make any connection but may throw exception or return no hosts.
     * The {@link Scheduler} implementation doesn't respect requested sniff delays but rather immediately runs them while
     * allowing to assert that the requested delay for each schedule run is the expected one.
     */
    public void testOrdinarySniffingRounds() throws Exception {
        final long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();
        final int iters = randomIntBetween(30, 100);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger(iters);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Scheduler scheduler = new Scheduler() {
                @Override
                public void schedule(Runnable runnable, long delayMillis) {
                    int numberOfRuns = runs.getAndDecrement();
                    if (numberOfRuns == 0) {
                        latch.countDown();
                        return;
                    }
                    if (numberOfRuns == iters) {
                        //the first time "schedule" gets called from the Sniffer constructor with delay set to 0
                        assertEquals(0L, delayMillis);
                    } else {
                        //all of the subsequent times "schedule" is called with delay set to the configured sniff interval
                        assertEquals(sniffInterval, delayMillis);
                    }
                    //we immediately run rather than scheduling
                    executor.execute(runnable);
                }

                @Override
                public void shutdown() {
                    //the executor is closed externally, shutdown is tested separately
                }
            };

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
     * Test concurrent calls to Sniffer#sniff. They may happen if you set a super low sniffInterval
     * and/or the {@link HostsSniffer} implementation is very slow.
     */
    public void testConcurrentSniffRounds() throws Exception {
        long sniffInterval = randomLongBetween(1, Long.MAX_VALUE);
        long sniffAfterFailureDelay = randomLongBetween(1, Long.MAX_VALUE);
        RestClient restClient = mock(RestClient.class);
        CountingHostsSniffer hostsSniffer = new CountingHostsSniffer();

        final int numThreads = randomIntBetween(10, 30);
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final AtomicBoolean firstRun = new AtomicBoolean(true);

        try {
            Scheduler scheduler = new Scheduler() {
                @Override
                public void schedule(Runnable runnable, long delayMillis) {
                    if (firstRun.compareAndSet(true, false)) {
                        for (int i = 0; i < numThreads; i++) {
                            //this will only run the same runnable n times, to simulate concurrent calls to the sniff method
                            executor.submit(runnable);
                        }
                    }
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

        int totalRuns = hostsSniffer.runs.get();
        //out of n concurrent threads trying to run Sniffer#sniff, only the first one will go through.
        //That's why the total number of runs is less or equal to the number of threads.
        //The important part is also that the assertion on no concurrent runs in the below CountingHostsSniffer never trips
        assertThat(totalRuns, lessThanOrEqualTo(numThreads));
        int setHostsRuns = totalRuns - hostsSniffer.failures.get() - hostsSniffer.emptyList.get();
        verify(restClient, times(setHostsRuns)).setHosts(any(HttpHost.class));
        verifyNoMoreInteractions(restClient);
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

    @SuppressWarnings("unchecked")
    public void testDefaultSchedulerSchedule() {
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(scheduledExecutorService.isShutdown()).thenReturn(false);
        when(scheduledExecutorService.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class))).thenReturn(scheduledFuture);

        DefaultScheduler defaultScheduler = new DefaultScheduler(scheduledExecutorService);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };
        {
            long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
            defaultScheduler.schedule(runnable, delayMillis);
            verify(scheduledExecutorService).isShutdown();
            verify(scheduledExecutorService).schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
            verifyNoMoreInteractions(scheduledExecutorService);
            verifyNoMoreInteractions(scheduledFuture);
        }
        {
            long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
            defaultScheduler.schedule(runnable, delayMillis);
            verify(scheduledExecutorService, times(2)).isShutdown();
            verify(scheduledExecutorService).schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
            verifyNoMoreInteractions(scheduledExecutorService);
            verify(scheduledFuture).cancel(false);
            verifyNoMoreInteractions(scheduledFuture);
        }
        {
            when(scheduledExecutorService.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
                    .thenThrow(new IllegalArgumentException());
            long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
            defaultScheduler.schedule(runnable, delayMillis);
            verify(scheduledExecutorService, times(3)).isShutdown();
            verify(scheduledExecutorService).schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
            verifyNoMoreInteractions(scheduledExecutorService);
            verify(scheduledFuture, times(2)).cancel(false);
            verifyNoMoreInteractions(scheduledFuture);
        }
        {
            when(scheduledExecutorService.isShutdown()).thenReturn(true);
            long delayMillis = randomLongBetween(0L, Long.MAX_VALUE);
            defaultScheduler.schedule(runnable, delayMillis);
            verify(scheduledExecutorService, times(4)).isShutdown();
            verifyNoMoreInteractions(scheduledExecutorService);
            verifyNoMoreInteractions(scheduledFuture);
        }
    }

    public void testDefaultSchedulerThreadFactory() {
        DefaultScheduler defaultScheduler = new DefaultScheduler();
        assertThat(defaultScheduler.scheduledExecutorService, instanceOf(ScheduledThreadPoolExecutor.class));
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) defaultScheduler.scheduledExecutorService;
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
    }

    public void testDefaultSchedulerShutdown() throws Exception {
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        DefaultScheduler defaultScheduler = new DefaultScheduler(scheduledExecutorService);

        defaultScheduler.shutdown();
        verify(scheduledExecutorService).shutdown();
        verify(scheduledExecutorService).awaitTermination(1000, TimeUnit.MILLISECONDS);
        verify(scheduledExecutorService).shutdownNow();
        verifyNoMoreInteractions(scheduledExecutorService);

        when(scheduledExecutorService.awaitTermination(1000, TimeUnit.MILLISECONDS)).thenReturn(true);
        defaultScheduler.shutdown();
        verify(scheduledExecutorService, times(2)).shutdown();
        verify(scheduledExecutorService, times(2)).awaitTermination(1000, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(scheduledExecutorService);
    }

    //TODO test on failure intervals

    //TODO add a sniffer test against proper http server, or maybe just throw exception and trigger on failure

}
