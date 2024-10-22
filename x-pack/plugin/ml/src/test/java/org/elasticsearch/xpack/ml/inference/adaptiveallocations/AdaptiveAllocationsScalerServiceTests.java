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
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AdaptiveAllocationsScalerServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private Client client;
    private InferenceAuditor inferenceAuditor;
    private MeterRegistry meterRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
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

    @Override
    @After
    public void tearDown() throws Exception {
        this.threadPool.close();
        super.tearDown();
    }

    private ClusterState getClusterState(int numAllocations) {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.custom("trained_model_assignment")).thenReturn(
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
                        new AdaptiveAllocationsSettings(true, null, null)
                    ).build()
                )
            )
        );
        return clusterState;
    }

    private GetDeploymentStatsAction.Response getDeploymentStatsResponse(int numAllocations, int inferenceCount, double latency) {
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
                    Instant.now(),
                    List.of(
                        AssignmentStats.NodeStats.forStartedState(
                            DiscoveryNodeUtils.create("node_1"),
                            inferenceCount,
                            latency,
                            latency,
                            0,
                            0,
                            0,
                            0,
                            0,
                            Instant.now(),
                            Instant.now(),
                            1,
                            numAllocations,
                            inferenceCount,
                            inferenceCount,
                            latency,
                            0
                        )
                    ),
                    Priority.NORMAL
                )
            ),
            0
        );
    }

    public void test() throws IOException {
        // Initialize the cluster with a deployment with 1 allocation.
        ClusterState clusterState = getClusterState(1);
        when(clusterService.state()).thenReturn(clusterState);

        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            1
        );
        service.start();

        verify(clusterService).state();
        verify(clusterService).addListener(same(service));
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // First cycle: 1 inference request, so no need for scaling.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 1, 11.0));
            return Void.TYPE;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), eq(new GetDeploymentStatsAction.Request("test-deployment")), any());

        safeSleep(1200);

        verify(client, times(1)).threadPool();
        verify(client, times(1)).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        // Second cycle: 150 inference request with a latency of 10ms, so scale up to 2 allocations.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(1, 150, 10.0));
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
        updateRequest.setIsInternal(true);
        verify(client, times(1)).execute(eq(UpdateTrainedModelDeploymentAction.INSTANCE), eq(updateRequest), any());
        verifyNoMoreInteractions(client, clusterService);
        reset(client, clusterService);

        clusterState = getClusterState(2);
        ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        service.clusterChanged(clusterChangedEvent);

        // Third cycle: 0 inference requests, but keep 2 allocations, because of cooldown.
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(getDeploymentStatsResponse(2, 0, 9.0));
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

    public void testMaybeStartAllocation() {
        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            1
        );

        when(client.threadPool()).thenReturn(threadPool);

        // will not start when adaptive allocations are not enabled
        assertFalse(service.maybeStartAllocation(TrainedModelAssignment.Builder.empty(taskParams(1), null).build()));
        assertFalse(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(1), new AdaptiveAllocationsSettings(Boolean.FALSE, 1, 2)).build()
            )
        );
        // min allocations > 0
        assertFalse(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(0), new AdaptiveAllocationsSettings(Boolean.TRUE, 1, 2)).build()
            )
        );
        assertTrue(
            service.maybeStartAllocation(
                TrainedModelAssignment.Builder.empty(taskParams(0), new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 2)).build()
            )
        );
    }

    public void testMaybeStartAllocation_BlocksMultipleRequests() throws Exception {
        AdaptiveAllocationsScalerService service = new AdaptiveAllocationsScalerService(
            threadPool,
            clusterService,
            client,
            inferenceAuditor,
            meterRegistry,
            true,
            1
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
        return new StartTrainedModelDeploymentAction.TaskParams(
            "foo",
            "foo",
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
