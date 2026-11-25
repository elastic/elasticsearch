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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
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
    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private Client client;
    private InferenceAuditor inferenceAuditor;
    private MeterRegistry meterRegistry;

    @Before
    public void initAdaptiveAllocationsTestDeps() throws Exception {
        threadPool = createThreadPool(
            new ScalingExecutorBuilder(MachineLearning.UTILITY_THREAD_POOL_NAME, 0, 1, TimeValue.timeValueMinutes(10), false)
        );
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, Set.of()));
        client = mock(Client.class);
        inferenceAuditor = mock(InferenceAuditor.class);
        meterRegistry = mock(MeterRegistry.class);
    }

    @After
    public void closeThreadPool() throws Exception {
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
        return getDeploymentStatsResponse(
            numAllocations,
            inferenceCount,
            latency,
            Instant.now(),
            recentStartup ? Instant.now() : Instant.now().minus(1, ChronoUnit.HOURS),
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
                                randomBoolean() ? DiscoveryNodeUtils.create("node_1") : null,
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
            SIXTY_THOUSAND_MILLISECONDS
        );
        // First cycle: 1 inference request, so no need for scaling.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 1, 11.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        safeSleep(1500);

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // Second cycle: 150 inference request with a latency of 10ms, so scale up to 2 allocations.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 150, 10.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        safeSleep(1000);

        verify(client, times(2)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(2);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        clusterState = getClusterState(2, AssignmentState.STARTED);
        ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        service.clusterChanged(clusterChangedEvent);

        // Third cycle: 0 inference requests, but keep 2 allocations, because of cooldown.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(2, 0, 9.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        safeSleep(1000);

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
            TWO_THOUSAND_MILLISECONDS
        );
        // First cycle: 1 inference request, so no need for scaling.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 1, 11.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        safeSleep(2500);

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // Second cycle: 0 inference requests for 1 second, so scale down to 0 allocations.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 0, 10.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        safeSleep(2000);

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
            TWO_THOUSAND_MILLISECONDS
        );
        // First cycle: many inference requests
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 10000, 10.0, false, AssignmentState.STARTING));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        safeSleep(1200);

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // Second cycle: again many inference requests
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 20000, 10.0, false, AssignmentState.STARTING));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());

        safeSleep(1200);

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
            TWO_THOUSAND_MILLISECONDS
        );
        // First cycle: 1 inference request, so no need for scaling.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 1, 11.0, true, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);

        safeSleep(1500);

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // Second cycle: 0 inference requests for 1 second, but a recent scale up by another node.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 0, 10.0, true, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        safeSleep(1000);

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // Third cycle: 0 inference requests for 1 second and no recent scale up, so scale down to 0 allocations.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 0, 10.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        safeSleep(1000);

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
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 10, 10.0, oneDayAgo, oneDayAgo, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());
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
        when(client.threadPool()).thenReturn(threadPool);
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
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        // This node is master and collects stats once, then stops being master.
        service.start();
        assertBusy(() -> assertThat(statsRequestCount.get(), greaterThanOrEqualTo(1)));
        service.stop();

        // Simulate some time not being the master node.
        safeSleep(2_000);

        // After re-election, the deployment has been idle for about 4 seconds at the first stats collection and
        // about 5 seconds at the second one, so it should not scale down to zero yet.
        int statsRequestCountBeforeReelection = statsRequestCount.get();
        service.start();
        assertBusy(() -> assertThat(statsRequestCount.get(), greaterThanOrEqualTo(statsRequestCountBeforeReelection + 2)));
        verify(client, never()).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());

        // Once the deployment has been idle for more than 6 seconds, it scales down to zero.
        var updateRequest = new UpdateTrainedModelDeploymentAction.Request("test-deployment");
        updateRequest.setNumberOfAllocations(0);
        updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.ADAPTIVE_ALLOCATIONS);
        assertBusy(() -> verify(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any()));
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
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(0, 0, 10.0, false, AssignmentState.STARTED));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CreateTrainedModelAssignmentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(client).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), any(), any());
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

        when(client.threadPool()).thenReturn(threadPool);

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

        when(client.threadPool()).thenReturn(threadPool);

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

        when(client.threadPool()).thenReturn(threadPool);
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
