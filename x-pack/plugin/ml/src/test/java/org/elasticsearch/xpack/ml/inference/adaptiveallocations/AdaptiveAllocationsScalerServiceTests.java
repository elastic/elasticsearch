/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adaptiveallocations;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AdaptiveAllocationsScalerServiceTests extends ESTestCase {

    private static final long ONE_SECOND = 1L;
    private static final AtomicLong ATOMIC_SECOND = new AtomicLong(1);
    private static final AtomicLong SIXTY_SECONDS = new AtomicLong(60);
    private static final AtomicLong TWO_THOUSAND_MILLISECONDS = new AtomicLong(2_000);
    private static final AtomicLong SIXTY_THOUSAND_MILLISECONDS = new AtomicLong(60_000);

    /**
     * Runs the stats cycles of the scaler service one at a time, so that a test can verify each cycle
     * without racing the scheduler. The stub of the stats request calls {@link #answer}. The test calls
     * {@link #run} for each cycle, and {@link #continueScheduler} when it is done with that cycle. The
     * teardown calls {@link #close}.
     */
    private static class StatsCycles {
        private final BlockingQueue<Supplier<GetDeploymentStatsAction.Response>> responses = new LinkedBlockingQueue<>();
        private final Semaphore cycleDone = new Semaphore(0);
        private final Semaphore schedulerContinued = new Semaphore(0);

        /**
         * Answers the stats request with the response of the current cycle, which is created now, so that its timestamps
         * are current. Afterwards, it doesn't return until {@link #continueScheduler}. This pauses the scheduler, because
         * it schedules the next cycle only after this one returns. After {@link #close}, it returns without answering.
         */
        void answer(ActionListener<GetDeploymentStatsAction.Response> listener) throws InterruptedException {
            GetDeploymentStatsAction.Response response = responses.take().get(); // waits for run() or close()
            if (response == null) {
                // Closed. Not answering keeps the service busy, so it doesn't start new cycles.
                return;
            }
            listener.onResponse(response);
            cycleDone.release(); // lets run() return
            schedulerContinued.acquire(); // waits for continueScheduler() or close()
        }

        /**
         * Runs the next cycle with the given stats response, and waits until it is done.
         */
        void run(Supplier<GetDeploymentStatsAction.Response> response) {
            responses.add(response); // lets answer() respond
            safeAcquire(cycleDone); // waits for answer() to finish the cycle
        }

        /**
         * Lets the scheduler continue after a cycle, so that it schedules the next one.
         */
        void continueScheduler() {
            schedulerContinued.release(); // lets answer() return
        }

        /**
         * Lets a waiting cycle return, also when a test failed halfway, and stops answering stats requests.
         * Without an answer the service doesn't start new cycles, because it's still busy with the last one.
         */
        void close() {
            responses.add(() -> null); // lets answer() return when it waits for a response
            schedulerContinued.release(); // lets answer() return when it waits for continueScheduler()
        }
    }

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private Client client;
    private InferenceAuditor inferenceAuditor;
    private MeterRegistry meterRegistry;
    // The node in the deployment stats. It must be the same in all stats of a test, because the
    // recent stats are computed per node from the difference with the previous stats.
    private DiscoveryNode statsNode;
    private StatsCycles statsCycles;
    // The clock of a service that runs with stats cycles, so that the time between cycles doesn't depend on how fast the
    // test runs. The scaler computes the request rate from that time.
    private final AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());

    @Before
    public void setup() throws Exception {
        threadPool = createThreadPool(
            new ScalingExecutorBuilder(MachineLearning.UTILITY_THREAD_POOL_NAME, 0, 1, TimeValue.timeValueMinutes(10), false)
        );
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, Set.of()));
        client = mock(Client.class);
        inferenceAuditor = mock(InferenceAuditor.class);
        meterRegistry = mock(MeterRegistry.class);
        statsNode = randomBoolean() ? DiscoveryNodeUtils.create("node_1") : null;

        // By default, the stats requests are answered by the stats cycles, and updates succeed.
        statsCycles = new StatsCycles();
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            statsCycles.answer(listener);
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());
    }

    @After
    public void closeThreadPool() throws Exception {
        statsCycles.close();
        this.threadPool.close();
    }

    private ClusterState getClusterState(int numAllocations, AssignmentState assignmentState) {
        return getClusterState(numAllocations, assignmentState, new AdaptiveAllocationsSettings(true, null, null));
    }

    private ClusterState getClusterState(
        int numAllocations,
        AssignmentState assignmentState,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getSingleProjectCustom("trained_model_assignment")).thenReturn(
            new TrainedModelAssignmentMetadata(
                Map.of(
                    "test-deployment",
                    TrainedModelAssignment.Builder.empty(
                        new StartTrainedModelDeploymentAction.TaskParams(
                            "model-id",
                            "test-deployment",
                            100_000_000,
                            numAllocations,
                            1,
                            1024,
                            ByteSizeValue.ZERO,
                            Priority.NORMAL,
                            100_000_000,
                            100_000_000
                        ),
                        adaptiveAllocationsSettings
                    ).setAssignmentState(assignmentState).build()
                )
            )
        );
        return clusterState;
    }

    private GetDeploymentStatsAction.Response getDeploymentStatsResponse(
        int numAllocations,
        int inferenceCount,
        double latency,
        boolean recentStartup,
        AssignmentState assignmentState
    ) {
        Instant now = Instant.ofEpochMilli(currentTimeMillis.get());
        return getDeploymentStatsResponse(
            numAllocations,
            inferenceCount,
            latency,
            now,
            recentStartup ? now : now.minus(1, ChronoUnit.HOURS),
            assignmentState
        );
    }

    private GetDeploymentStatsAction.Response getDeploymentStatsResponse(
        int numAllocations,
        int inferenceCount,
        double latency,
        Instant lastAccess,
        Instant startTime,
        AssignmentState assignmentState
    ) {
        return new GetDeploymentStatsAction.Response(
            List.of(),
            List.of(),
            List.of(
                new AssignmentStats(
                    "test-deployment",
                    "model-id",
                    1,
                    numAllocations,
                    new AdaptiveAllocationsSettings(true, null, null),
                    1024,
                    ByteSizeValue.ZERO,
                    Instant.now().minus(1, ChronoUnit.DAYS),
                    // a deployment without allocations has no node stats
                    numAllocations == 0
                        ? List.of()
                        : List.of(
                            AssignmentStats.NodeStats.forStartedState(
                                statsNode,
                                inferenceCount,
                                latency,
                                latency,
                                0,
                                0,
                                0,
                                0,
                                0,
                                lastAccess,
                                startTime,
                                1,
                                numAllocations,
                                inferenceCount,
                                inferenceCount,
                                latency,
                                0,
                                0L,
                                null
                            )
                        ),
                    Priority.NORMAL
                ).setState(assignmentState)
            ),
            0
        );
    }

    public void test_scaleUp() {
        // Initialize the cluster with a deployment with 1 allocation.
        ClusterState clusterState = getClusterState(1, AssignmentState.STARTED);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            SIXTY_SECONDS,
            SIXTY_THOUSAND_MILLISECONDS,
            currentTimeMillis::get
        );

        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        // First cycle: 1 inference request, so no need for scaling.
        statsCycles.run(() -> getDeploymentStatsResponse(1, 1, 11.0, false, AssignmentState.STARTED));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        clearInvocations(client, clusterService);
        statsCycles.continueScheduler();

        // Second cycle: 150 inference request with a latency of 10ms, so scale up to 2 allocations.
        currentTimeMillis.addAndGet(1_000);
        statsCycles.run(() -> getDeploymentStatsResponse(1, 150, 10.0, false, AssignmentState.STARTED));

        verify(client, times(2)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(2);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
        verifyNoMoreInteractions(client, clusterService);
        clearInvocations(client, clusterService);

        clusterState = getClusterState(2, AssignmentState.STARTED);
        ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        service.clusterChanged(clusterChangedEvent);
        statsCycles.continueScheduler();

        // Third cycle: 0 inference requests, but keep 2 allocations, because of cooldown.
        currentTimeMillis.addAndGet(1_000);
        statsCycles.run(() -> getDeploymentStatsResponse(2, 0, 9.0, false, AssignmentState.STARTED));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);

        service.stop();
    }

    public void test_scaleDownToZero_whenNoRequests() {
        // Initialize the cluster with a deployment with 1 allocation.
        ClusterState clusterState = getClusterState(1, AssignmentState.STARTED);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            2 * ONE_SECOND,
            ATOMIC_SECOND,
            TWO_THOUSAND_MILLISECONDS,
            currentTimeMillis::get
        );

        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        // First cycle: 1 inference request, so no need for scaling.
        statsCycles.run(() -> getDeploymentStatsResponse(1, 1, 11.0, false, AssignmentState.STARTED));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        clearInvocations(client, clusterService);
        statsCycles.continueScheduler();

        // Second cycle: 0 inference requests for 1 second, so scale down to 0 allocations.
        currentTimeMillis.addAndGet(2_000);
        statsCycles.run(() -> getDeploymentStatsResponse(1, 0, 10.0, false, AssignmentState.STARTED));

        verify(client, times(2)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(0);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
        verifyNoMoreInteractions(client, clusterService);

        service.stop();
    }

    public void test_dontScale_whenNotStarted() {
        // Initialize the cluster with a deployment with 1 allocation.
        ClusterState clusterState = getClusterState(1, AssignmentState.STARTING);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            ATOMIC_SECOND,
            TWO_THOUSAND_MILLISECONDS,
            currentTimeMillis::get
        );

        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        // First cycle: many inference requests
        statsCycles.run(() -> getDeploymentStatsResponse(1, 10000, 10.0, false, AssignmentState.STARTING));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        clearInvocations(client, clusterService);
        statsCycles.continueScheduler();

        // Second cycle: again many inference requests
        currentTimeMillis.addAndGet(1_000);
        statsCycles.run(() -> getDeploymentStatsResponse(1, 20000, 10.0, false, AssignmentState.STARTING));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        service.stop();
    }

    public void test_noScaleDownToZero_whenRecentlyScaledUpByOtherNode() {
        // Initialize the cluster with a deployment with 1 allocation.
        ClusterState clusterState = getClusterState(1, AssignmentState.STARTED);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            ATOMIC_SECOND,
            TWO_THOUSAND_MILLISECONDS,
            currentTimeMillis::get
        );

        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        // First cycle: 1 inference request, so no need for scaling.
        statsCycles.run(() -> getDeploymentStatsResponse(1, 1, 11.0, true, AssignmentState.STARTED));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        clearInvocations(client, clusterService);
        statsCycles.continueScheduler();

        // Second cycle: 0 inference requests for 1 second, but a recent scale up by another node.
        currentTimeMillis.addAndGet(1_000);
        statsCycles.run(() -> getDeploymentStatsResponse(1, 0, 10.0, true, AssignmentState.STARTED));

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        clearInvocations(client, clusterService);
        statsCycles.continueScheduler();

        // Third cycle: 0 inference requests for 1 second and no recent scale up, so scale down to 0 allocations.
        currentTimeMillis.addAndGet(1_000);
        statsCycles.run(() -> getDeploymentStatsResponse(1, 0, 10.0, false, AssignmentState.STARTED));

        verify(client, times(2)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(0);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
        verifyNoMoreInteractions(client, clusterService);

        service.stop();
    }

    public void test_scaleDownToZero_afterMasterFailover_whenAlreadyIdle() throws Exception {
        // A newly elected master starts with a deployment with 1 allocation,
        // which was started and last used a day ago, so it has been idle for longer than the scale-to-zero time.
        ClusterState clusterState = getClusterState(1, AssignmentState.STARTED);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            SIXTY_SECONDS,
            SIXTY_THOUSAND_MILLISECONDS
        );

        Instant oneDayAgo = Instant.now().minus(1, ChronoUnit.DAYS);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 10, 10.0, oneDayAgo, oneDayAgo, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        service.start();

        // Scale down to zero on the first evaluation, without waiting for the scale-to-zero time again.
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(0);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        assertBusy(() -> verify(client, atLeastOnce()).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any()));

        service.stop();
    }

    public void test_noEarlyScaleDownToZero_afterReelectedAsMaster() throws Exception {
        // The deployment was just started and scales to zero after 6 seconds without requests.
        // The scale-up cooldown is short, so it doesn't prevent scaling down.
        ClusterState clusterState = getClusterState(1, AssignmentState.STARTED);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            new AtomicLong(6),
            TWO_THOUSAND_MILLISECONDS
        );

        Instant lastActivity = Instant.now();
        AtomicInteger statsRequestCount = new AtomicInteger();
        AtomicLong idleMillisAtFirstScaleDown = new AtomicLong(-1);
        doAnswer(invocationOnMock -> {
            statsRequestCount.incrementAndGet();
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 10, 10.0, lastActivity, lastActivity, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            idleMillisAtFirstScaleDown.compareAndSet(-1, System.currentTimeMillis() - lastActivity.toEpochMilli());
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        // This node is master and collects stats once, then stops being master.
        service.start();
        assertBusy(() -> assertThat(statsRequestCount.get(), greaterThanOrEqualTo(1)));
        service.stop();

        // Simulate some time not being the master node.
        safeSleep(2_000);

        // Only count a scale down after re-election. On a slow machine, the first term could have scaled down already.
        clearInvocations(client);
        idleMillisAtFirstScaleDown.set(-1);

        // After re-election, it scales down to zero once the deployment has been idle for more than 6 seconds.
        service.start();
        // Wait for the idle time to be recorded, because Mockito records the invocation before running the answer.
        assertBusy(() -> assertThat(idleMillisAtFirstScaleDown.get(), greaterThanOrEqualTo(0L)));
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(0);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, atLeastOnce()).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
        assertThat(idleMillisAtFirstScaleDown.get(), greaterThanOrEqualTo(6_000L));
        service.stop();
    }

    public void test_scaleUpToMinimum_whenZeroAllocationsAndMinimumIsAboveZero() throws Exception {
        var settings = new AdaptiveAllocationsSettings(true, 2, null);
        ClusterState clusterState = getClusterState(0, AssignmentState.STARTED, settings);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            SIXTY_SECONDS,
            SIXTY_THOUSAND_MILLISECONDS
        );
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(0, 0, 10.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        service.start();

        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(2);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        assertBusy(() -> verify(client, atLeastOnce()).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any()));

        service.stop();
    }

    public void testMaybeStartAllocation() {
        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            SIXTY_SECONDS,
            TWO_THOUSAND_MILLISECONDS
        );

        // will not start when adaptive allocations are not enabled
        assertFalse(service.maybeStartAllocation(TrainedModelAssignment.Builder.empty(taskParams(1), null).build()));
        assertFalse(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(1), new AdaptiveAllocationsSettings(Boolean.FALSE, 1, 2)).build()
            )
        );
        verify(client, never()).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams("d1", 0), new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 2)).build()
            )
        );
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("d1");
        updateRequest.setNumberOfAllocations(1);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());

        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams("d2", 0), new AdaptiveAllocationsSettings(Boolean.TRUE, 2, 4)).build()
            )
        );
        updateRequest = new UpdateTrainedModelDeploymentAction.Request("d2");
        updateRequest.setNumberOfAllocations(2);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
    }

    public void testMaybeStartAllocation_waitsWithoutScaling_whenAllocationsAreAlreadyRequested() {
        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            SIXTY_SECONDS,
            TWO_THOUSAND_MILLISECONDS
        );

        // Allocations are requested, but not started yet. The request should wait for them,
        // also when the minimum has changed in the meantime, and the allocations should not be changed.
        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(2), new AdaptiveAllocationsSettings(Boolean.TRUE, 1, 4)).build()
            )
        );
        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(3), new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 4)).build()
            )
        );
        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(1), new AdaptiveAllocationsSettings(Boolean.TRUE, 2, 4)).build()
            )
        );

        verify(client, never()).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());
    }

    public void testMaybeStartAllocation_BlocksMultipleRequests() throws Exception {
        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            ONE_SECOND,
            SIXTY_SECONDS,
            SIXTY_THOUSAND_MILLISECONDS
        );

        var latch = new CountDownLatch(1);
        var scalingUpRequestSent = new AtomicBoolean();

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            scalingUpRequestSent.set(true);
            latch.await();
            listener.onResponse(mock(CreateTrainedModelAssignmentAction.Response.class));
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            var starting = service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(0), new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 2)).build()
            );
            assertTrue(starting);
        });

        // wait for the request to be sent
        assertBusy(() -> assertTrue(scalingUpRequestSent.get()));

        // Due to the inflight request this will not trigger an update request
        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(0), new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 2)).build()
            )
        );
        // release the inflight request
        latch.countDown();

        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());
    }

    private StartTrainedModelDeploymentAction.TaskParams taskParams(int numAllocations) {
        return taskParams("foo", numAllocations);
    }

    private StartTrainedModelDeploymentAction.TaskParams taskParams(String deploymentId, int numAllocations) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            deploymentId,
            deploymentId,
            1000L,
            numAllocations,
            1,
            100,
            null,
            Priority.NORMAL,
            100L,
            100L
        );
    }
}
