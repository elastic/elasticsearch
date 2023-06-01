/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
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
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentRoutingInfoAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
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
import static org.mockito.Mockito.when;

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

    public void testLoadQueuedModels_GivenNoQueuedModels() {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        // When there are no queued models
        trainedModelAssignmentNodeService.loadQueuedModels();
        verify(deploymentManager, never()).startDeployment(any(), any());
    }

    public void testLoadQueuedModels() {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        String modelToLoad = "loading-model";
        String anotherModel = "loading-model-again";
        String deploymentId = "foo";
        String anotherDeployment = "bar";

        givenAssignmentsInClusterStateForModels(List.of(deploymentId, anotherDeployment), List.of(modelToLoad, anotherModel));

        // Should only load each model once
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(deploymentId, modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(anotherDeployment, anotherModel));

        trainedModelAssignmentNodeService.loadQueuedModels();

        ArgumentCaptor<TrainedModelDeploymentTask> taskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentRoutingInfoAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentRoutingInfoAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(taskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(2)).updateModelAssignmentState(requestCapture.capture(), any());

        assertThat(taskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getDeploymentId(), equalTo(deploymentId));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));

        assertThat(taskCapture.getAllValues().get(1).getModelId(), equalTo(anotherModel));
        assertThat(requestCapture.getAllValues().get(1).getDeploymentId(), equalTo(anotherDeployment));
        assertThat(requestCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(1).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));

        // Since models are loaded, there shouldn't be any more loadings to occur
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(anotherDeployment, anotherModel));
        trainedModelAssignmentNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenFailureIsRetried() {
        String modelToLoad = "loading-model";
        String failedModelToLoad = "failed-search-loading-model";
        String deploymentId = "foo";
        String failedDeploymentId = "failed-foo";

        givenAssignmentsInClusterStateForModels(List.of(deploymentId, failedDeploymentId), List.of(modelToLoad, failedModelToLoad));
        withSearchingLoadFailure(failedModelToLoad);
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(deploymentId, modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(failedDeploymentId, failedModelToLoad));

        trainedModelAssignmentNodeService.loadQueuedModels();

        trainedModelAssignmentNodeService.loadQueuedModels();

        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentRoutingInfoAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentRoutingInfoAction.Request.class
        );
        verify(deploymentManager, times(3)).startDeployment(startTaskCapture.capture(), any());
        // Only the successful one is notifying, the failed one keeps retrying but not notifying as it is never successful
        verify(trainedModelAssignmentService, times(1)).updateModelAssignmentState(requestCapture.capture(), any());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getDeploymentId(), equalTo(deploymentId));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));

        assertThat(startTaskCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(startTaskCapture.getAllValues().get(1).getDeploymentId(), equalTo(failedDeploymentId));
        assertThat(startTaskCapture.getAllValues().get(2).getModelId(), equalTo(failedModelToLoad));
        assertThat(startTaskCapture.getAllValues().get(2).getDeploymentId(), equalTo(failedDeploymentId));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenStopped() {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        // When there are no queued models
        String modelToLoad = "loading-model";

        // Should only load each model once
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(modelToLoad, modelToLoad));
        trainedModelAssignmentNodeService.stop();

        trainedModelAssignmentNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenTaskIsStopped() throws Exception {
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        // When there are no queued models
        String modelToLoad = "loading-model";
        String stoppedModelToLoad = "stopped-loading-model";
        String loadingDeploymentId = "loading-foo";
        String stoppedLoadingDeploymentId = "stopped-loading-foo";

        givenAssignmentsInClusterStateForModels(
            List.of(loadingDeploymentId, stoppedLoadingDeploymentId),
            List.of(modelToLoad, stoppedModelToLoad)
        );

        // Only one model should be loaded, the other should be stopped
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(loadingDeploymentId, modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(stoppedLoadingDeploymentId, stoppedModelToLoad));
        trainedModelAssignmentNodeService.getTask(stoppedLoadingDeploymentId).stop("testing", ActionListener.noop());
        trainedModelAssignmentNodeService.loadQueuedModels();

        assertBusy(() -> {
            ArgumentCaptor<TrainedModelDeploymentTask> stoppedTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
            verify(deploymentManager, times(1)).stopDeployment(stoppedTaskCapture.capture());
            assertThat(stoppedTaskCapture.getValue().getModelId(), equalTo(stoppedModelToLoad));
            assertThat(stoppedTaskCapture.getValue().getDeploymentId(), equalTo(stoppedLoadingDeploymentId));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentRoutingInfoAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentRoutingInfoAction.Request.class
        );
        verify(deploymentManager, times(1)).startDeployment(startTaskCapture.capture(), any());
        assertBusy(() -> verify(trainedModelAssignmentService, times(3)).updateModelAssignmentState(requestCapture.capture(), any()));

        boolean seenStopping = false;
        for (int i = 0; i < 3; i++) {
            UpdateTrainedModelAssignmentRoutingInfoAction.Request request = requestCapture.getAllValues().get(i);
            assertThat(request.getNodeId(), equalTo(NODE_ID));
            if (request.getDeploymentId().equals(stoppedLoadingDeploymentId)) {
                if (seenStopping) {
                    assertThat(request.getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STOPPED));
                } else {
                    assertThat(request.getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STOPPING));
                    seenStopping = true;
                }
            } else {
                assertThat(request.getDeploymentId(), equalTo(loadingDeploymentId));
                assertThat(request.getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));
            }
        }
        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testLoadQueuedModelsWhenOneFails() throws InterruptedException {
        String modelToLoad = "loading-model";
        String failedModelToLoad = "failed-loading-model";
        String loadingDeploymentId = "loading-foo";
        String failedLoadingDeploymentId = "failed-loading-foo";

        givenAssignmentsInClusterStateForModels(
            List.of(loadingDeploymentId, failedLoadingDeploymentId),
            List.of(modelToLoad, failedModelToLoad)
        );
        withLoadFailure(failedModelToLoad);
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();

        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(loadingDeploymentId, modelToLoad));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(failedLoadingDeploymentId, failedModelToLoad));

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            latch.countDown();
            return null;
        }).when(deploymentManager).stopDeployment(any());

        trainedModelAssignmentNodeService.loadQueuedModels();

        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentRoutingInfoAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentRoutingInfoAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(2)).updateModelAssignmentState(requestCapture.capture(), any());

        ArgumentCaptor<TrainedModelDeploymentTask> stopTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        verify(deploymentManager).stopDeployment(stopTaskCapture.capture());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getDeploymentId(), equalTo(loadingDeploymentId));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));

        assertThat(startTaskCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(requestCapture.getAllValues().get(1).getDeploymentId(), equalTo(failedLoadingDeploymentId));
        assertThat(requestCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(1).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.FAILED));

        assertThat(stopTaskCapture.getValue().getModelId(), equalTo(failedModelToLoad));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    public void testClusterChangedWithResetMode() {
        final TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();
        final DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId(NODE_ID).add(DiscoveryNodeUtils.create(NODE_ID, NODE_ID)).build();
        String modelOne = "model-1";
        String modelTwo = "model-2";
        String notUsedModel = "model-3";
        String deploymentOne = "deployment-1";
        String deploymentTwo = "deployment-2";
        String notUsedDeployment = "deployment-3";

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
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentOne, modelOne))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                )
                                .addNewAssignment(
                                    modelTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentTwo, modelTwo))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                )
                                .addNewAssignment(
                                    notUsedModel,
                                    TrainedModelAssignment.Builder.empty(newParams(notUsedDeployment, notUsedModel))
                                        .addRoutingEntry("some-other-node", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
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
        final DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId(NODE_ID).add(DiscoveryNodeUtils.create(NODE_ID, NODE_ID)).build();
        String modelOne = "model-1";
        String modelTwo = "model-2";
        String notUsedModel = "model-3";
        String previouslyUsedModel = "model-4";
        String deploymentOne = "deployment-1";
        String deploymentTwo = "deployment-2";
        String notUsedDeployment = "deployment-3";
        String previouslyUsedDeployment = "deployment-4";

        givenAssignmentsInClusterStateForModels(
            List.of(deploymentOne, deploymentTwo, previouslyUsedDeployment),
            List.of(modelOne, modelTwo, previouslyUsedModel)
        );
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
                                    deploymentOne,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentOne, modelOne))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                )
                                .addNewAssignment(
                                    deploymentTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentTwo, modelTwo))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        .updateExistingRoutingEntry(
                                            NODE_ID,
                                            new RoutingInfo(
                                                1,
                                                1,
                                                randomFrom(RoutingState.STARTED, RoutingState.STARTING),
                                                randomAlphaOfLength(10)
                                            )
                                        )
                                )
                                .addNewAssignment(
                                    previouslyUsedDeployment,
                                    TrainedModelAssignment.Builder.empty(newParams(previouslyUsedDeployment, previouslyUsedModel))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        .updateExistingRoutingEntry(
                                            NODE_ID,
                                            new RoutingInfo(
                                                1,
                                                1,
                                                randomFrom(RoutingState.STOPPED, RoutingState.FAILED, RoutingState.STOPPING),
                                                randomAlphaOfLength(10)
                                            )
                                        )
                                )
                                .addNewAssignment(
                                    notUsedDeployment,
                                    TrainedModelAssignment.Builder.empty(newParams(notUsedDeployment, notUsedModel))
                                        .addRoutingEntry("some-other-node", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
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
                                    deploymentOne,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentOne, modelOne))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                )
                                .addNewAssignment(
                                    deploymentTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentTwo, modelTwo))
                                        .addRoutingEntry("some-other-node", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                )
                                .addNewAssignment(
                                    notUsedDeployment,
                                    TrainedModelAssignment.Builder.empty(newParams(notUsedDeployment, notUsedModel))
                                        .addRoutingEntry("some-other-node", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
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
            assertThat(stoppedTaskCapture.getAllValues().get(0).getDeploymentId(), equalTo(deploymentTwo));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentRoutingInfoAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentRoutingInfoAction.Request.class
        );
        verify(deploymentManager, times(1)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(1)).updateModelAssignmentState(requestCapture.capture(), any());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelOne));
        assertThat(requestCapture.getAllValues().get(0).getDeploymentId(), equalTo(deploymentOne));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));

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
                                    deploymentOne,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentOne, modelOne))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
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

    public void testClusterChanged_GivenAllStartedAssignments_AndNonMatchingTargetAllocations() throws Exception {
        final TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = createService();
        final DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId(NODE_ID).add(DiscoveryNodeUtils.create(NODE_ID, NODE_ID)).build();
        String modelOne = "model-1";
        String modelTwo = "model-2";
        String deploymentOne = "deployment-1";
        String deploymentTwo = "deployment-2";
        givenAssignmentsInClusterStateForModels(List.of(deploymentOne, deploymentTwo), List.of(modelOne, modelTwo));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(deploymentOne, modelOne));
        trainedModelAssignmentNodeService.prepareModelToLoad(newParams(deploymentTwo, modelTwo));
        trainedModelAssignmentNodeService.loadQueuedModels();

        ClusterChangedEvent event = new ClusterChangedEvent(
            "shouldUpdateAllocations",
            ClusterState.builder(new ClusterName("shouldUpdateAllocations"))
                .nodes(nodes)
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            TrainedModelAssignmentMetadata.NAME,
                            TrainedModelAssignmentMetadata.Builder.empty()
                                .addNewAssignment(
                                    deploymentOne,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentOne, modelOne))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(1, 3, RoutingState.STARTED, ""))
                                )
                                .addNewAssignment(
                                    deploymentTwo,
                                    TrainedModelAssignment.Builder.empty(newParams(deploymentTwo, modelTwo))
                                        .addRoutingEntry(NODE_ID, new RoutingInfo(2, 1, RoutingState.STARTED, ""))
                                )
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );

        trainedModelAssignmentNodeService.clusterChanged(event);

        assertBusy(() -> {
            ArgumentCaptor<TrainedModelDeploymentTask> updatedTasks = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
            ArgumentCaptor<Integer> updatedAllocations = ArgumentCaptor.forClass(Integer.class);
            verify(deploymentManager, times(2)).updateNumAllocations(updatedTasks.capture(), updatedAllocations.capture(), any(), any());
            assertThat(updatedTasks.getAllValues().get(0).getModelId(), equalTo(modelOne));
            assertThat(updatedTasks.getAllValues().get(0).getDeploymentId(), equalTo(deploymentOne));
            assertThat(updatedTasks.getAllValues().get(1).getModelId(), equalTo(modelTwo));
            assertThat(updatedTasks.getAllValues().get(1).getDeploymentId(), equalTo(deploymentTwo));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAssignmentRoutingInfoAction.Request> updateCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAssignmentRoutingInfoAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAssignmentService, times(2)).updateModelAssignmentState(updateCapture.capture(), any());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelOne));
        assertThat(startTaskCapture.getAllValues().get(1).getModelId(), equalTo(modelTwo));
        assertThat(updateCapture.getAllValues().get(0).getDeploymentId(), equalTo(deploymentOne));
        assertThat(updateCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(updateCapture.getAllValues().get(0).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));
        assertThat(updateCapture.getAllValues().get(1).getDeploymentId(), equalTo(deploymentTwo));
        assertThat(updateCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(updateCapture.getAllValues().get(1).getUpdate().getStateAndReason().get().getState(), equalTo(RoutingState.STARTED));

        verifyNoMoreInteractions(deploymentManager, trainedModelAssignmentService);
    }

    private void givenAssignmentsInClusterStateForModels(List<String> deploymentIds, List<String> modelIds) {
        assertEquals(deploymentIds.size(), modelIds.size());
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.Builder.empty();
        for (int i = 0; i < modelIds.size(); i++) {
            builder.addNewAssignment(
                deploymentIds.get(i),
                TrainedModelAssignment.Builder.empty(newParams(deploymentIds.get(i), modelIds.get(i)))
                    .addRoutingEntry("test-node", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
            );
        }

        ClusterState currentState = ClusterState.builder(new ClusterName("testLoadQueuedModels"))
            .metadata(Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, builder.build()).build())
            .build();

        when(clusterService.state()).thenReturn(currentState);
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

    private static StartTrainedModelDeploymentAction.TaskParams newParams(String deploymentId, String modelId) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            deploymentId,
            randomNonNegativeLong(),
            1,
            1,
            1024,
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomNonNegativeLong()),
            randomFrom(Priority.values())
        );
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
