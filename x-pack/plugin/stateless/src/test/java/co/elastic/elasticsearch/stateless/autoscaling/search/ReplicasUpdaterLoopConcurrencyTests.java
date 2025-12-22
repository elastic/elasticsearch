/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService.State.IDLE;
import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService.State.RUNNING;
import static co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService.State.RUNNING_WITH_PENDING;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ReplicasUpdaterLoopConcurrencyTests extends ESTestCase {

    private SearchMetricsService searchMetricsService;
    private ReplicasUpdaterService replicasUpdaterService;
    private TestThreadPool testThreadPool;
    private TestExecutionTrackingListener listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AtomicLong currentRelativeTimeInNanos = new AtomicLong(1L);
        MemoryMetricsService memoryMetricsService = mock(MemoryMetricsService.class);
        when(memoryMetricsService.getSearchTierMemoryMetrics()).thenReturn(new MemoryMetrics(4096, 8192, MetricQuality.EXACT));
        ClusterSettings clusterSettings = createClusterSettings();
        searchMetricsService = spy(
            new SearchMetricsService(
                clusterSettings,
                currentRelativeTimeInNanos::get,
                memoryMetricsService,
                TelemetryProvider.NOOP.getMeterRegistry()
            )
        );
        this.testThreadPool = new TestThreadPool(getTestName());
        listener = new TestExecutionTrackingListener();
        replicasUpdaterService = new ReplicasUpdaterService(
            testThreadPool,
            createClusterService(testThreadPool, createClusterSettings()),
            new NoOpNodeClient(testThreadPool),
            searchMetricsService,
            listener
        );
        // very high interval to control calls manually
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(600));
    }

    @After
    public void afterTest() {
        this.testThreadPool.shutdown();
    }

    public void testSequentialExecutions() {
        replicasUpdaterService.performReplicaUpdates();
        replicasUpdaterService.performReplicaUpdates();
        replicasUpdaterService.performReplicaUpdates();

        assertThat(listener.getRunCount(), is(3));
        assertThat(listener.getExecutionLog(), is(List.of("run-1", "run-2", "run-3")));
        assertThat(replicasUpdaterService.state(), is(IDLE));
    }

    public void testConcurrentCallsExecuteAtLeastOnePendingRun() throws Exception {
        CountDownLatch runStartLatch = new CountDownLatch(1);
        CountDownLatch runBlockLatch = new CountDownLatch(1);
        listener.setRunStartLatch(runStartLatch);
        listener.setRunBlockLatch(runBlockLatch);

        try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
            CountDownLatch allThreadsReady = new CountDownLatch(10);
            CountDownLatch startGun = new CountDownLatch(1);

            List<Future<?>> futures = new ArrayList<>(10);
            // Launch 10 threads simultaneously
            for (int i = 0; i < 10; i++) {
                futures.add(executor.submit(() -> {
                    allThreadsReady.countDown();
                    try {
                        startGun.await();
                        replicasUpdaterService.performReplicaUpdates();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }

            // Wait for all threads to be ready, then start them
            allThreadsReady.await();
            startGun.countDown();

            // Wait for the run loop to start (one thread is executing the replicas updater loop)
            runStartLatch.await(5, TimeUnit.SECONDS);

            // The loop is blocked on this latch, to simulate it taking some time. release it to allow completion.
            runBlockLatch.countDown();

            // wait for all threads to complete
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }

            assertThat(listener.getRunCount(), is(greaterThanOrEqualTo(2)));
        }
    }

    public void testTriggerDuringExecutionRunsThePendingExecution() throws Exception {
        CountDownLatch runStartLatch = new CountDownLatch(1);
        CountDownLatch runBlockLatch = new CountDownLatch(1);
        listener.setRunStartLatch(runStartLatch);
        listener.setRunBlockLatch(runBlockLatch);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            // Thread 1: Start execution and block
            Future<?> future1 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());

            // Wait for thread 1 to start running
            runStartLatch.await(5, TimeUnit.SECONDS);
            assertThat(replicasUpdaterService.state(), is(RUNNING));

            // Thread 2: Trigger while thread 1 is running
            Future<?> future2 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());

            assertBusy(() -> assertThat(replicasUpdaterService.state(), is(RUNNING_WITH_PENDING)));

            // allow thread1 to complete the loop
            runBlockLatch.countDown();

            future1.get(5, TimeUnit.SECONDS);
            future2.get(5, TimeUnit.SECONDS);

            // Should have run twice: once for initial, once for pending
            assertThat(listener.getRunCount(), is(2));
            assertThat(replicasUpdaterService.state(), is(IDLE));
            assertThat(listener.getSkipCount(), is(1));
            assertThat(listener.getMaxChainedRuns(), is(1));

            // Verify the sequence: run, skip, pending run, run (the second run is the pending one)
            List<String> log = listener.getExecutionLog();
            assertThat(log, is(List.of("run-1", "skip", "pending-run-call", "run-2")));
        }
    }

    public void testTriggerDuringStateTransitionToIdle() throws Exception {
        // This test FORCES a race condition on the state transition to IDLE followed by a new thread triggering the execution.
        // We must have 2 runs in total: T1's initial run, and T2's run that claims execution

        CountDownLatch runStartLatch = new CountDownLatch(1);
        CountDownLatch runBlockLatch = new CountDownLatch(1);
        CountDownLatch onIdleLatch = new CountDownLatch(1);
        CountDownLatch onIdleBlock = new CountDownLatch(1);
        listener.setRunStartLatch(runStartLatch);
        listener.setRunBlockLatch(runBlockLatch);
        listener.setOnTransitionToIdleLatch(onIdleLatch);
        listener.setOnTransitionToIdleBlock(onIdleBlock);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            // Thread 1: Start execution
            Future<?> future1 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());

            // Wait for T1 to start running
            runStartLatch.await(5, TimeUnit.SECONDS);
            assertThat(replicasUpdaterService.state(), is(RUNNING));

            // Release T1 to complete its run
            runBlockLatch.countDown();

            // Wait for T1 to release state to IDLE (but T1 is now blocked in onIdleLatch)
            onIdleLatch.await(5, TimeUnit.SECONDS);

            // At this exact moment:
            // - State is IDLE (T1 just set it)
            // - T1 hasn't checked if there's a pending run yet (blocked in listener)
            // - This is the RACE WINDOW, so now we trigger T2

            assertThat(replicasUpdaterService.state(), is(IDLE));

            // Thread 2 triggers a run now that we're in the race window
            Future<?> future2 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());

            // Now release T1 to continue
            onIdleBlock.countDown();

            // Wait for both to complete
            future1.get(5, TimeUnit.SECONDS);
            future2.get(5, TimeUnit.SECONDS);

            // Verify final state, should be back to IDLE after completing the execution
            assertThat(replicasUpdaterService.state(), is(IDLE));

            // T1 ran once (stateAfterRun was RUNNING, so no pending run)
            // T2 ran once (claimed execution after T1 moved state to IDLE)
            assertThat(listener.getRunCount(), is(2));

            // No skips - T2 successfully claimed execution
            assertThat(listener.getSkipCount(), is(0));

        }
    }

    public void testTriggerDuringStateTransitionToIdleWhenPendingRunExists() throws Exception {
        // Similar to testTriggerDuringStateTransitionToIdle but T2 sets the state to RUNNING_WITH_PENDING before T1 changes the state to
        // IDLE.
        // This verifies that pending flags are not lost and we will get at least 2 runs.

        CountDownLatch runStartLatch = new CountDownLatch(1);
        CountDownLatch runBlockLatch = new CountDownLatch(1);
        CountDownLatch onIdleLatch = new CountDownLatch(1);
        CountDownLatch onIdleBlock = new CountDownLatch(1);
        listener.setRunStartLatch(runStartLatch);
        listener.setRunBlockLatch(runBlockLatch);
        listener.setOnTransitionToIdleLatch(onIdleLatch);
        listener.setOnTransitionToIdleBlock(onIdleBlock);

        try (ExecutorService executor = Executors.newFixedThreadPool(3)) {
            // Thread 1: Start execution
            Future<?> future1 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());

            // Wait for T1 to start running
            runStartLatch.await(5, TimeUnit.SECONDS);
            assertThat(replicasUpdaterService.state(), is(RUNNING));

            // Thread 2: Set pending flag while T1 is running
            Future<?> future2 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());
            assertBusy(() -> assertThat(replicasUpdaterService.state(), is(RUNNING_WITH_PENDING)));

            // Release T1 to complete
            runBlockLatch.countDown();

            // T1 changes the state to IDLE and remains there before checking what the previous state was
            onIdleLatch.await(5, TimeUnit.SECONDS);
            assertThat(replicasUpdaterService.state(), is(IDLE));

            // Thread 3: Try to trigger and race with T1 checking if it needs to re-enter the loop
            Future<?> future3 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates());

            // T1 checks the previous state and will see RUNNING_WITH_PENDING so it will re-enter the loop
            onIdleBlock.countDown();

            future1.get(5, TimeUnit.SECONDS);
            future2.get(5, TimeUnit.SECONDS);
            future3.get(5, TimeUnit.SECONDS);

            // T1: initial run + recursive run (saw RWP)
            // T3: either it ran as was cheduled after T1 finished re-entering the loop and completing it
            // (i.e. it saw the state as IDLE and ran another loop) or otherwise it raced with T1 on changing the state from IDLE to
            // RUNNING, or from RUNNING (T1 was first) to RUNNING_WITH_PENDING
            assertThat(listener.getRunCount(), is(greaterThanOrEqualTo(2)));
            assertThat(listener.getRunCount(), is(lessThanOrEqualTo(3)));
            assertThat(replicasUpdaterService.state(), is(IDLE));
        }
    }

    public void testImmediateScaleDownFlagPreserved() throws Exception {
        CountDownLatch runStartLatch = new CountDownLatch(1);
        CountDownLatch runBlockLatch = new CountDownLatch(1);
        listener.setRunStartLatch(runStartLatch);
        listener.setRunBlockLatch(runBlockLatch);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            // Thread 1 starts with immediateScaleDown=false
            Future<?> future1 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates(false));

            runStartLatch.await(5, TimeUnit.SECONDS);

            // Thread 2 triggers with immediateScaleDown=true while T1 is running
            Future<?> future2 = executor.submit(() -> replicasUpdaterService.performReplicaUpdates(true));
            // let thread 2 complete (it won't run the loop as T1 is running)
            future2.get(5, TimeUnit.SECONDS);
            // allow T1 to complete its run (and the pending run that T2 created)
            runBlockLatch.countDown();

            future1.get(5, TimeUnit.SECONDS);

            // Should have run twice
            assertThat(listener.getRunCount(), is(2));

            // first run should be false (original call)
            // second run should be true (from pending request)
            assertThat(listener.scaleDownValues, is(List.of(false, true)));
        }
    }

    public void testRandomizedConcurrentCalls() throws Exception {
        int numThreads = randomIntBetween(5, 20);
        int numCallsPerThread = randomIntBetween(3, 10);

        CountDownLatch allThreadsReady = new CountDownLatch(numThreads);
        CountDownLatch startGun = new CountDownLatch(1);

        // track concurrent executions to verify no parallel runs (only one run at a time)
        AtomicInteger concurrentExecutions = new AtomicInteger(0);
        AtomicInteger maxConcurrentExecutions = new AtomicInteger(0);

        AtomicInteger immediateScaleDownCallsMade = new AtomicInteger(0);

        TestExecutionTrackingListener concurrencyCheckingListener = new TestExecutionTrackingListener() {
            @Override
            public void onRunStart(boolean immediateScaleDown) {
                int concurrent = concurrentExecutions.incrementAndGet();
                maxConcurrentExecutions.updateAndGet(max -> Math.max(max, concurrent));

                // sometimes simulate the run method taking a bit longer
                if (randomBoolean()) {
                    try {
                        Thread.sleep(randomIntBetween(0, 20));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.onRunStart(immediateScaleDown);
            }

            @Override
            public void onRunComplete() {
                concurrentExecutions.decrementAndGet();
                super.onRunComplete();
            }
        };

        replicasUpdaterService = new ReplicasUpdaterService(
            testThreadPool,
            createClusterService(testThreadPool, createClusterSettings()),
            new NoOpNodeClient(testThreadPool),
            searchMetricsService,
            concurrencyCheckingListener
        );
        replicasUpdaterService.setInterval(TimeValue.timeValueMinutes(600));

        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < numThreads; i++) {
                futures.add(executor.submit(() -> {
                    allThreadsReady.countDown();
                    try {
                        startGun.await();
                        for (int j = 0; j < numCallsPerThread; j++) {
                            boolean useImmediateScaleDown = randomBoolean();
                            if (useImmediateScaleDown) {
                                immediateScaleDownCallsMade.incrementAndGet();
                            }
                            replicasUpdaterService.performReplicaUpdates(useImmediateScaleDown);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }

            // start all threads simultaneously
            allThreadsReady.await();
            startGun.countDown();

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
            assertThat(replicasUpdaterService.state(), is(IDLE));

            assertThat("run method executed in parallel", maxConcurrentExecutions.get(), is(1));

            // I If any immediateScaleDown=true call was made, at least one run should have seen it
            if (immediateScaleDownCallsMade.get() > 0) {
                boolean hasImmediateScaleDown = concurrencyCheckingListener.scaleDownValues.contains(true);
                assertThat("immediateScaleDown=true was called but never executed", hasImmediateScaleDown, is(true));
            }
            assertThat(replicasUpdaterService.state(), is(IDLE));
        }
    }

    /**
     * Test listener that manages concurrency constructs to trigger and observe race conditions. It also tracks the execution so we can
     * assert on expected outcomes.
     */
    static class TestExecutionTrackingListener implements ReplicasUpdaterExecutionListener {
        // signals that the run loop has started execution
        private CountDownLatch runStartLatch;
        // blocks the thread that is executing the run loop in RUNNING mode
        private CountDownLatch runBlockLatch;
        // similar to above, signals that one thread has transitioned to IDLE
        private CountDownLatch onTransitionToIdleLatch;
        // blocks that thread that just transitioned to IDLE and keeps it there until released
        private CountDownLatch onTransitionToIdleBlock;
        private final AtomicInteger runCount = new AtomicInteger(0);
        private final AtomicInteger skipCount = new AtomicInteger(0);
        private final AtomicInteger chainedPendingRuns = new AtomicInteger(0);
        private final AtomicInteger maxChainedRuns = new AtomicInteger(0);
        private final List<String> executionLog = new CopyOnWriteArrayList<>();
        private final List<Boolean> scaleDownValues = new CopyOnWriteArrayList<>();

        @Override
        public void onRunStart(boolean immediateScaleDown) {
            int chainedRuns = chainedPendingRuns.incrementAndGet();
            maxChainedRuns.updateAndGet(max -> Math.max(max, chainedRuns));
            executionLog.add("run-" + runCount.incrementAndGet());
            scaleDownValues.add(immediateScaleDown);

            if (runStartLatch != null) {
                runStartLatch.countDown();
            }

            if (runBlockLatch != null) {
                try {
                    runBlockLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onRunComplete() {
            chainedPendingRuns.decrementAndGet();
        }

        @Override
        public void onSkipped() {
            skipCount.incrementAndGet();
            executionLog.add("skip");
        }

        @Override
        public void onPendingRunExecution() {
            executionLog.add("pending-run-call");
        }

        @Override
        public void onStateTransitionToIdle() {
            if (onTransitionToIdleLatch != null) {
                onTransitionToIdleLatch.countDown();
            }

            if (onTransitionToIdleBlock != null) {
                try {
                    onTransitionToIdleBlock.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        int getRunCount() {
            return runCount.get();
        }

        int getSkipCount() {
            return skipCount.get();
        }

        int getMaxChainedRuns() {
            return maxChainedRuns.get();
        }

        List<String> getExecutionLog() {
            return new ArrayList<>(executionLog);
        }

        public void setRunStartLatch(CountDownLatch runStartLatch) {
            this.runStartLatch = runStartLatch;
        }

        public void setRunBlockLatch(CountDownLatch runBlockLatch) {
            this.runBlockLatch = runBlockLatch;
        }

        public void setOnTransitionToIdleLatch(CountDownLatch onTransitionToIdleLatch) {
            this.onTransitionToIdleLatch = onTransitionToIdleLatch;
        }

        public void setOnTransitionToIdleBlock(CountDownLatch onTransitionToIdleBlock) {
            this.onTransitionToIdleBlock = onTransitionToIdleBlock;
        }
    }

    private static ClusterSettings createClusterSettings() {
        Set<Setting<?>> defaultClusterSettings = Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL,
            ReplicasUpdaterService.REPLICA_UPDATER_SCALEDOWN_REPETITIONS,
            ReplicasUpdaterService.AUTO_EXPAND_REPLICA_INDICES,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
            ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER,
            ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING,
            ReplicasLoadBalancingScaler.MAX_REPLICA_RELATIVE_SEARCH_LOAD
        );
        return new ClusterSettings(
            Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true).build(),
            defaultClusterSettings
        );
    }
}
