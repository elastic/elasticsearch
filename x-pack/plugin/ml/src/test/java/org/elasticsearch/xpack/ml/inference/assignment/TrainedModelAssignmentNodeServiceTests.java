/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentStateAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TrainedModelAssignmentNodeServiceTests extends ESTestCase {

    private static final String NODE_ID = "test-node";

    private ClusterService clusterService;
    private DeploymentManager deploymentManager;
    private ThreadPool threadPool;
    private TrainedModelAssignmentService trainedModelAssignmentService;
    private TaskManager taskManager;

    @Before
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setupObjects() {
        trainedModelAssignmentService = mock(TrainedModelAssignmentService.class);
        clusterService = mock(ClusterService.class);
        threadPool = new TestThreadPool(
            "TrainedModelAssignmentNodeServiceTests",
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.ml.utility_thread_pool"
            )
        );
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        deploymentManager = mock(DeploymentManager.class);
        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(invocationOnMock.getArguments()[0]);
            return null;
        }).when(deploymentManager).startDeployment(any(), any());
        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(trainedModelAssignmentService).updateModelAssignmentState(any(), any());
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testLoadQueuedModels() {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        // When there are no queued models
        trainedModelAssignmentNodeService.loadQueuedModels();
        verify(deploymentManager, never()).startDeployment(any(), any());

        String modelToLoad = "loading-model";
        String anotherModel = "loading-model-again";

        // Should only load each model once
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(anotherModel));

        trainedModelAssignmentNodeService.loadQueuedModels();

        ArgumentCaptor<TrainedModelDeploymentTask> taskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentStateAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(taskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(2)).updateModelAssignmentState(requestCapture.capture(), any());

        assertThat(taskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        assertThat(taskCapture.getAllValues().get(1).getModelId(), equalTo(anotherModel));
        assertThat(requestCapture.getAllValues().get(1).getModelId(), equalTo(anotherModel));
        assertThat(requestCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(1).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        // Since models are loaded, there shouldn't be any more loadings to occur
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(anotherModel));
        trainedModelAssignmentNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenFailureIsRetried() {
        String modelToLoad = "loading-model";
        String failedModelToLoad = "failed-search-loading-model";
        withSearchingLoadFailure(failedModelToLoad);
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(failedModelToLoad));

        trainedModelAssignmentNodeService.loadQueuedModels();

        trainedModelAssignmentNodeService.loadQueuedModels();

        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentStateAction.Request.class
        );
        verify(deploymentManager, times(3)).startDeployment(startTaskCapture.capture(), any());
        // Only the successful one is notifying, the failed one keeps retrying but not notifying as it is never successful
        verify(trainedModelAssignmentService, times(1)).updateModelAssignmentState(requestCapture.capture(), any());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        assertThat(startTaskCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(startTaskCapture.getAllValues().get(2).getModelId(), equalTo(failedModelToLoad));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenStopped() {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        // When there are no queued models
        String modelToLoad = "loading-model";

        // Should only load each model once
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAssignmentNodeService.stop();

        trainedModelAssignmentNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenTaskIsStopped() throws Exception {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        // When there are no queued models
        String modelToLoad = "loading-model";
        String stoppedModelToLoad = "stopped-loading-model";

        // Only one model should be loaded, the other should be stopped
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(stoppedModelToLoad));
        trainedModelAssignmentNodeService.getTask(stoppedModelToLoad).stop("testing", ActionListener.noop());
        trainedModelAssignmentNodeService.loadQueuedModels();

        assertBusy(() -> {
            ArgumentCaptor<TrainedModelDeploymentTask> stoppedTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
            verify(deploymentManager, times(1)).stopDeployment(stoppedTaskCapture.capture());
            assertThat(stoppedTaskCapture.getValue().getModelId(), equalTo(stoppedModelToLoad));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentStateAction.Request.class
        );
        verify(deploymentManager, times(1)).startDeployment(startTaskCapture.capture(), any());
        assertBusy(() -> verify(trainedModelAssignmentService, times(3)).updateModelAssignmentState(requestCapture.capture(), any()));

        boolean seenStopping = false;
        for (int i = 0; i < 3; i++) {
            UpdateTrainedModelAssignmentStateAction.Request request = requestCapture.getAllValues().get(i);
            assertThat(request.getNodeId(), equalTo(NODE_ID));
            if (request.getModelId().equals(stoppedModelToLoad)) {
                if (seenStopping) {
                    assertThat(request.getRoutingState().getState(), equalTo(RoutingState.STOPPED));
                } else {
                    assertThat(request.getRoutingState().getState(), equalTo(RoutingState.STOPPING));
                    seenStopping = true;
                }
            } else {
                assertThat(request.getModelId(), equalTo(modelToLoad));
                assertThat(request.getRoutingState().getState(), equalTo(RoutingState.STARTED));
            }
        }
        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenOneFails() throws InterruptedException {
        String modelToLoad = "loading-model";
        String failedModelToLoad = "failed-loading-model";
        withLoadFailure(failedModelToLoad);
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(failedModelToLoad));

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            latch.countDown();
            return null;
        }).when(deploymentManager).stopDeployment(any());

        trainedModelAssignmentNodeService.loadQueuedModels();

        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentStateAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(2)).updateModelAssignmentState(requestCapture.capture(), any());

        ArgumentCaptor<TrainedModelDeploymentTask> stopTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        verify(deploymentManager).stopDeployment(stopTaskCapture.capture());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        assertThat(startTaskCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(requestCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(requestCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(1).getRoutingState().getState(), equalTo(RoutingState.FAILED));

        assertThat(stopTaskCapture.getValue().getModelId(), equalTo(failedModelToLoad));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testClusterChangedWithResetMode() {
        final TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .localNodeId(NODE_ID)
            .add(
                new DiscoveryNode(
                    NODE_ID,
                    NODE_ID,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles(),
                    Version.CURRENT
                )
            )
            .build();
        String modelOne = "model-1";
        String modelTwo = "model-2";
        String notUsedModel = "model-3";
        ClusterChangedEvent event = new ClusterChangedEvent(
            "testClusterChanged",
            ClusterState.builder(new ClusterName("testClusterChanged"))
                .nodes(nodes)
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            TrainedModelAssignmentMetadata.NAME,
                            TrainedModelAssignmentMetadata.Builder.empty()
                                .addNewAssignment(
                                    modelOne,
                                    TrainedModelAssignment.Builder.empty(newParams(modelOne)).addNewRoutingEntry(NODE_ID)
                                )
                                .addNewAssignment(
                                    modelTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(modelTwo)).addNewRoutingEntry(NODE_ID)
                                )
                                .addNewAssignment(
                                    notUsedModel,
                                    TrainedModelAssignment.Builder.empty(newParams(notUsedModel)).addNewRoutingEntry("some-other-node")
                                )
                                .build()
                        )
                        .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(true).build())
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );

        trainedModelAssignmentNodeService.clusterChanged(event);
        trainedModelAssignmentNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testClusterChanged() throws Exception {
        final TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .localNodeId(NODE_ID)
            .add(
                new DiscoveryNode(
                    NODE_ID,
                    NODE_ID,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles(),
                    Version.CURRENT
                )
            )
            .build();
        String modelOne = "model-1";
        String modelTwo = "model-2";
        String notUsedModel = "model-3";
        String previouslyUsedModel = "model-4";
        ClusterChangedEvent event = new ClusterChangedEvent(
            "testClusterChanged",
            ClusterState.builder(new ClusterName("testClusterChanged"))
                .nodes(nodes)
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            TrainedModelAssignmentMetadata.NAME,
                            TrainedModelAssignmentMetadata.Builder.empty()
                                .addNewAssignment(
                                    modelOne,
                                    TrainedModelAssignment.Builder.empty(newParams(modelOne)).addNewRoutingEntry(NODE_ID)
                                )
                                .addNewAssignment(
                                    modelTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(modelTwo))
                                        .addNewRoutingEntry(NODE_ID)
                                        .updateExistingRoutingEntry(
                                            NODE_ID,
                                            new RoutingStateAndReason(
                                                randomFrom(RoutingState.STARTED, RoutingState.STARTING),
                                                randomAlphaOfLength(10)
                                            )
                                        )
                                )
                                .addNewAssignment(
                                    previouslyUsedModel,
                                    TrainedModelAssignment.Builder.empty(newParams(modelTwo))
                                        .addNewRoutingEntry(NODE_ID)
                                        .updateExistingRoutingEntry(
                                            NODE_ID,
                                            new RoutingStateAndReason(
                                                randomFrom(RoutingState.STOPPED, RoutingState.FAILED, RoutingState.STOPPING),
                                                randomAlphaOfLength(10)
                                            )
                                        )
                                )
                                .addNewAssignment(
                                    notUsedModel,
                                    TrainedModelAssignment.Builder.empty(newParams(notUsedModel)).addNewRoutingEntry("some-other-node")
                                )
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );

        trainedModelAssignmentNodeService.clusterChanged(event);

        event = new ClusterChangedEvent(
            "testClusterChanged",
            ClusterState.builder(new ClusterName("testClusterChanged"))
                .nodes(nodes)
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            TrainedModelAssignmentMetadata.NAME,
                            TrainedModelAssignmentMetadata.Builder.empty()
                                .addNewAssignment(
                                    modelOne,
                                    TrainedModelAssignment.Builder.empty(newParams(modelOne)).addNewRoutingEntry(NODE_ID)
                                )
                                .addNewAssignment(
                                    modelTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(modelTwo)).addNewRoutingEntry("some-other-node")
                                )
                                .addNewAssignment(
                                    notUsedModel,
                                    TrainedModelAssignment.Builder.empty(newParams(notUsedModel)).addNewRoutingEntry("some-other-node")
                                )
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );
        trainedModelAssignmentNodeService.clusterChanged(event);

        trainedModelAssignmentNodeService.loadQueuedModels();

        assertBusy(() -> {
            ArgumentCaptor<TrainedModelDeploymentTask> stoppedTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
            verify(deploymentManager, times(1)).stopDeployment(stoppedTaskCapture.capture());
            assertThat(stoppedTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelTwo));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentStateAction.Request.class
        );
        verify(deploymentManager, times(1)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(1)).updateModelAssignmentState(requestCapture.capture(), any());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelOne));
        assertThat(requestCapture.getAllValues().get(0).getModelId(), equalTo(modelOne));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        event = new ClusterChangedEvent(
            "testClusterChanged",
            ClusterState.builder(new ClusterName("testClusterChanged"))
                .nodes(nodes)
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            TrainedModelAssignmentMetadata.NAME,
                            TrainedModelAssignmentMetadata.Builder.empty()
                                .addNewAssignment(
                                    modelOne,
                                    TrainedModelAssignment.Builder.empty(newParams(modelOne)).addNewRoutingEntry(NODE_ID)
                                )
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );
        trainedModelAssignmentNodeService.clusterChanged(event);

        trainedModelAssignmentNodeService.loadQueuedModels();

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void withLoadFailure(String modelId) {
        doAnswer(invocationOnMock -> {
            TrainedModelDeploymentTask task = (TrainedModelDeploymentTask) invocationOnMock.getArguments()[0];
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            if (task.getModelId().equals(modelId)) {
                listener.onFailure(new ResourceNotFoundException("model node found"));
            } else {
                listener.onResponse(task);
            }
            return null;
        }).when(deploymentManager).startDeployment(any(), any());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void withSearchingLoadFailure(String modelId) {
        doAnswer(invocationOnMock -> {
            TrainedModelDeploymentTask task = (TrainedModelDeploymentTask) invocationOnMock.getArguments()[0];
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            if (task.getModelId().equals(modelId)) {
                listener.onFailure(new SearchPhaseExecutionException("all shards failed", "foo", ShardSearchFailure.EMPTY_ARRAY));
            } else {
                listener.onResponse(task);
            }
            return null;
        }).when(deploymentManager).startDeployment(any(), any());
    }

    private static StartTrainedModelDeploymentAction.TaskParams newParams(String modelId) {
        return new StartTrainedModelDeploymentAction.TaskParams(modelId, randomNonNegativeLong(), 1, 1, 1024);
    }

    private TrainedModelAssignmentNodeService createService() {
        return new TrainedModelAssignmentNodeService(
            trainedModelAssignmentService,
            clusterService,
            deploymentManager,
            TestIndexNameExpressionResolver.newInstance(),
            taskManager,
            threadPool,
            NODE_ID,
            mock(XPackLicenseState.class)
        );
    }

}
