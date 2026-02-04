/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.AllocationStatsService;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStatsTests;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetAllocationStatsActionTests extends ESTestCase {

    private long startTimeMillis;
    private TimeValue allocationStatsCacheTTL;
    private ControlledRelativeTimeThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private AllocationStatsService allocationStatsService;

    private TransportGetAllocationStatsAction action;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTimeMillis = 0L;
        allocationStatsCacheTTL = TimeValue.timeValueMinutes(1);
        threadPool = new ControlledRelativeTimeThreadPool(TransportClusterAllocationExplainActionTests.class.getName(), startTimeMillis);
        clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            new ClusterSettings(
                Settings.builder().put(TransportGetAllocationStatsAction.CACHE_TTL_SETTING.getKey(), allocationStatsCacheTTL).build(),
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
            )
        );
        transportService = new CapturingTransport().createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> clusterService.localNode(),
            clusterService.getClusterSettings(),
            Set.of()
        );
        allocationStatsService = mock(AllocationStatsService.class);
        action = new TransportGetAllocationStatsAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Set.of()),
            allocationStatsService
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        clusterService.close();
        transportService.close();
    }

    private void disableAllocationStatsCache() {
        setAllocationStatsCacheTTL(TimeValue.ZERO);
    }

    private void setAllocationStatsCacheTTL(TimeValue ttl) {
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(TransportGetAllocationStatsAction.CACHE_TTL_SETTING.getKey(), ttl).build());
    };

    public void testReturnsOnlyRequestedStats() throws Exception {
        disableAllocationStatsCache();
        int expectedNumberOfStatsServiceCalls = 0;

        for (final var metrics : List.of(
            EnumSet.of(Metric.ALLOCATIONS, Metric.FS),
            EnumSet.of(Metric.ALLOCATIONS),
            EnumSet.of(Metric.FS),
            EnumSet.noneOf(Metric.class),
            EnumSet.allOf(Metric.class),
            EnumSet.copyOf(randomSubsetOf(between(1, Metric.values().length), EnumSet.allOf(Metric.class)))
        )) {
            var request = new TransportGetAllocationStatsAction.Request(TimeValue.ONE_MINUTE, TaskId.EMPTY_TASK_ID, metrics);

            when(allocationStatsService.stats(any())).thenReturn(
                Map.of(randomIdentifier(), NodeAllocationStatsTests.randomNodeAllocationStats())
            );

            var future = new PlainActionFuture<TransportGetAllocationStatsAction.Response>();
            action.masterOperation(getTask(), request, ClusterState.EMPTY_STATE, future);
            var response = future.get();

            if (metrics.contains(Metric.ALLOCATIONS)) {
                assertThat(response.getNodeAllocationStats(), not(anEmptyMap()));
                verifyAllocationStatsServiceNumCallsEqualTo(++expectedNumberOfStatsServiceCalls);
            } else {
                assertThat(response.getNodeAllocationStats(), anEmptyMap());
                verifyAllocationStatsServiceNumCallsEqualTo(expectedNumberOfStatsServiceCalls);
            }

            if (metrics.contains(Metric.FS)) {
                assertNotNull(response.getDiskThresholdSettings());
            } else {
                assertNull(response.getDiskThresholdSettings());
            }
        }
    }

    public void testDeduplicatesStatsComputations() throws InterruptedException {
        disableAllocationStatsCache();
        final var requestCounter = new AtomicInteger();
        final var isExecuting = new AtomicBoolean();
        when(allocationStatsService.stats(any())).thenAnswer(invocation -> {
            try {
                assertTrue(isExecuting.compareAndSet(false, true));
                assertThat(Thread.currentThread().getName(), containsString("[management]"));
                return Map.of(Integer.toString(requestCounter.incrementAndGet()), NodeAllocationStatsTests.randomNodeAllocationStats());
            } finally {
                Thread.yield();
                assertTrue(isExecuting.compareAndSet(true, false));
            }
        });

        final var threads = new Thread[between(1, 5)];
        final var startBarrier = new CyclicBarrier(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                safeAwait(startBarrier);

                final var minRequestIndex = requestCounter.get();

                final TransportGetAllocationStatsAction.Response response = safeAwait(
                    l -> action.masterOperation(getTask(), getRequest(), ClusterState.EMPTY_STATE, l)
                );

                final var requestIndex = Integer.valueOf(response.getNodeAllocationStats().keySet().iterator().next());
                assertThat(requestIndex, greaterThanOrEqualTo(minRequestIndex)); // did not get a stale result
            }, "thread-" + i);
            threads[i].start();
        }

        for (final var thread : threads) {
            thread.join();
        }
    }

    public void testAllTasksCancelledCacheEnabled() throws InterruptedException {
        runTestWithCancelledTasks(between(2, 10), false, true);
    }

    public void testAllTasksCancelledCacheDisabled() throws InterruptedException {
        runTestWithCancelledTasks(between(2, 10), true, true);
    }

    public void testSomeTasksCancelledCacheEnabled() throws InterruptedException {
        runTestWithCancelledTasks(between(2, 10), false, false);
    }

    public void testSomeTasksCancelledCacheDisabled() throws InterruptedException {
        runTestWithCancelledTasks(between(2, 10), true, false);
    }

    private void runTestWithCancelledTasks(final int numThreads, final boolean cacheDisabled, final boolean cancelAllTasks)
        throws InterruptedException {
        if (cacheDisabled) {
            disableAllocationStatsCache();
        }
        final var isExecuting = new AtomicBoolean();
        final var ensureNotCancelledCaptor = ArgumentCaptor.forClass(Runnable.class);
        final var tasks = new CancellableTask[numThreads];
        final var cancellations = new boolean[numThreads];
        final var stats = Map.of(randomIdentifier(), NodeAllocationStatsTests.randomNodeAllocationStats());

        when(allocationStatsService.stats(ensureNotCancelledCaptor.capture())).thenAnswer(invocation -> {
            try {
                assertTrue(isExecuting.compareAndSet(false, true));
                for (int i = 0; i < numThreads; ++i) {
                    if (cancellations[i]) {
                        TaskCancelHelper.cancel(tasks[i], "cancelled");
                    }
                }
                ensureNotCancelledCaptor.getValue().run();
                return stats;
            } finally {
                Thread.yield();
                assertTrue(isExecuting.compareAndSet(true, false));
            }
        });

        ESTestCase.startInParallel(numThreads, threadNumber -> {
            tasks[threadNumber] = getTask();
            cancellations[threadNumber] = cancelAllTasks || randomBoolean();
            final ActionListener<TransportGetAllocationStatsAction.Response> listener = ActionListener.wrap(response -> {
                assertSame(stats, response.getNodeAllocationStats());
            }, e -> {
                if (e instanceof TaskCancelledException) {
                    assertTrue("got an unexpected cancellation exception for thread " + threadNumber, cancellations[threadNumber]);
                } else {
                    fail(e);
                }
            });
            ActionListener.run(listener, l -> action.masterOperation(tasks[threadNumber], getRequest(), ClusterState.EMPTY_STATE, l));
        });
    }

    public void testGetStatsWithCachingEnabled() throws Exception {

        final AtomicReference<Map<String, NodeAllocationStats>> allocationStats = new AtomicReference<>();
        int numExpectedAllocationStatsServiceCalls = 0;

        final Runnable resetExpectedAllocationStats = () -> {
            final var stats = Map.of(randomIdentifier(), NodeAllocationStatsTests.randomNodeAllocationStats());
            allocationStats.set(stats);
            when(allocationStatsService.stats(any())).thenReturn(stats);
        };

        final CheckedConsumer<ActionListener<Void>, Exception> threadTask = l -> {
            action.masterOperation(getTask(), getRequest(), ClusterState.EMPTY_STATE, l.map(response -> {
                assertSame("Expected the cached allocation stats to be returned", response.getNodeAllocationStats(), allocationStats.get());
                return null;
            }));
        };

        // Initial cache miss, all threads should get the same value.
        resetExpectedAllocationStats.run();
        ESTestCase.startInParallel(between(1, 5), threadNumber -> safeAwait(threadTask));
        verifyAllocationStatsServiceNumCallsEqualTo(++numExpectedAllocationStatsServiceCalls);

        // Advance the clock to a time less than or equal to the TTL and verify we still get the cached stats.
        threadPool.setCurrentTimeInMillis(startTimeMillis + between(0, (int) allocationStatsCacheTTL.millis()));
        ESTestCase.startInParallel(between(1, 5), threadNumber -> safeAwait(threadTask));
        verifyAllocationStatsServiceNumCallsEqualTo(numExpectedAllocationStatsServiceCalls);

        // Force the cached stats to expire.
        threadPool.setCurrentTimeInMillis(startTimeMillis + allocationStatsCacheTTL.getMillis() + 1);

        // Expect a single call to the stats service on the cache miss.
        resetExpectedAllocationStats.run();
        ESTestCase.startInParallel(between(1, 5), threadNumber -> safeAwait(threadTask));
        verifyAllocationStatsServiceNumCallsEqualTo(++numExpectedAllocationStatsServiceCalls);

        // Update the TTL setting to disable the cache, we expect a service call each time.
        setAllocationStatsCacheTTL(TimeValue.ZERO);
        safeAwait(threadTask);
        safeAwait(threadTask);
        numExpectedAllocationStatsServiceCalls += 2;
        verifyAllocationStatsServiceNumCallsEqualTo(numExpectedAllocationStatsServiceCalls);

        // Re-enable the cache, only one thread should call the stats service.
        final var newTTL = TimeValue.timeValueMinutes(5);
        setAllocationStatsCacheTTL(newTTL);
        threadPool.setCurrentTimeInMillis(threadPool.relativeTimeInMillis() + newTTL.getMillis() + 1);
        resetExpectedAllocationStats.run();
        ESTestCase.startInParallel(between(1, 5), threadNumber -> safeAwait(threadTask));
        verifyAllocationStatsServiceNumCallsEqualTo(++numExpectedAllocationStatsServiceCalls);
    }

    private void verifyAllocationStatsServiceNumCallsEqualTo(int numCalls) {
        verify(allocationStatsService, times(numCalls)).stats(any());
    }

    private static TransportGetAllocationStatsAction.Request getRequest() {
        return new TransportGetAllocationStatsAction.Request(TEST_REQUEST_TIMEOUT, TaskId.EMPTY_TASK_ID, EnumSet.of(Metric.ALLOCATIONS));
    }

    private static CancellableTask getTask() {
        return new CancellableTask(randomLong(), "type", "action", "desc", null, Map.of());
    }

    private static class ControlledRelativeTimeThreadPool extends ThreadPool {

        private long currentTimeInMillis;

        ControlledRelativeTimeThreadPool(String name, long startTimeMillis) {
            super(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders()
            );
            this.currentTimeInMillis = startTimeMillis;
            stopCachedTimeThread();
        }

        @Override
        public long relativeTimeInMillis() {
            return currentTimeInMillis;
        }

        void setCurrentTimeInMillis(long currentTimeInMillis) {
            this.currentTimeInMillis = currentTimeInMillis;
        }
    }
}
