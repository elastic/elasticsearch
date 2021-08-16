/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TrainedModelAllocationNodeServiceTests extends ESTestCase {

    private static final String NODE_ID = "test-node";

    private ClusterService clusterService;
    private DeploymentManager deploymentManager;
    private ThreadPool threadPool;
    private TrainedModelAllocationService trainedModelAllocationService;
    private TaskManager taskManager;

    @Before
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setupObjects() {
        trainedModelAllocationService = mock(TrainedModelAllocationService.class);
        clusterService = mock(ClusterService.class);
        threadPool = new TestThreadPool(
            "TrainedModelAllocationNodeServiceTests",
            new ScalingExecutorBuilder(UTILITY_THREAD_POOL_NAME, 1, 4, TimeValue.timeValueMinutes(10), "xpack.ml.utility_thread_pool")
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
        }).when(trainedModelAllocationService).updateModelAllocationState(any(), any());
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testLoadQueuedModels() {
        TrainedModelAllocationNodeService trainedModelAllocationNodeService = createService();

        // When there are no queued models
        trainedModelAllocationNodeService.loadQueuedModels();
        verify(deploymentManager, never()).startDeployment(any(), any());

        String modelToLoad = "loading-model";
        String anotherModel = "loading-model-again";

        // Should only load each model once
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(anotherModel));

        trainedModelAllocationNodeService.loadQueuedModels();

        ArgumentCaptor<TrainedModelDeploymentTask> taskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAllocationStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAllocationStateAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(taskCapture.capture(), any());
        verify(trainedModelAllocationService, times(2)).updateModelAllocationState(requestCapture.capture(), any());

        assertThat(taskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        assertThat(taskCapture.getAllValues().get(1).getModelId(), equalTo(anotherModel));
        assertThat(requestCapture.getAllValues().get(1).getModelId(), equalTo(anotherModel));
        assertThat(requestCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(1).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        // Since models are loaded, there shouldn't be any more loadings to occur
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(anotherModel));
        trainedModelAllocationNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAllocationService);
    }

    public void testLoadQueuedModelsWhenStopped() {
        TrainedModelAllocationNodeService trainedModelAllocationNodeService = createService();

        // When there are no queued models
        String modelToLoad = "loading-model";

        // Should only load each model once
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAllocationNodeService.stop();

        trainedModelAllocationNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAllocationService);
    }

    public void testLoadQueuedModelsWhenTaskIsStopped() throws Exception {
        TrainedModelAllocationNodeService trainedModelAllocationNodeService = createService();

        // When there are no queued models
        String modelToLoad = "loading-model";
        String stoppedModelToLoad = "stopped-loading-model";

        // Only one model should be loaded, the other should be stopped
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(stoppedModelToLoad));
        trainedModelAllocationNodeService.getTask(stoppedModelToLoad).stop("testing");
        trainedModelAllocationNodeService.loadQueuedModels();

        assertBusy(() -> {
            ArgumentCaptor<TrainedModelDeploymentTask> stoppedTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
            verify(deploymentManager, times(1)).stopDeployment(stoppedTaskCapture.capture());
            assertThat(stoppedTaskCapture.getValue().getModelId(), equalTo(stoppedModelToLoad));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAllocationStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAllocationStateAction.Request.class
        );
        verify(deploymentManager, times(1)).startDeployment(startTaskCapture.capture(), any());
        assertBusy(() -> verify(trainedModelAllocationService, times(3)).updateModelAllocationState(requestCapture.capture(), any()));

        boolean seenStopping = false;
        for (int i = 0; i < 3; i++) {
            UpdateTrainedModelAllocationStateAction.Request request = requestCapture.getAllValues().get(i);
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

        verifyNoMoreInteractions(deploymentManager, trainedModelAllocationService);
    }

    public void testLoadQueuedModelsWhenOneFails() {
        String modelToLoad = "loading-model";
        String failedModelToLoad = "failed-loading-model";
        withLoadFailure(failedModelToLoad);
        TrainedModelAllocationNodeService trainedModelAllocationNodeService = createService();

        trainedModelAllocationNodeService.prepareModelToLoad(newParams(modelToLoad));
        trainedModelAllocationNodeService.prepareModelToLoad(newParams(failedModelToLoad));

        trainedModelAllocationNodeService.loadQueuedModels();

        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAllocationStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAllocationStateAction.Request.class
        );
        verify(deploymentManager, times(2)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAllocationService, times(2)).updateModelAllocationState(requestCapture.capture(), any());

        assertThat(startTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getModelId(), equalTo(modelToLoad));
        assertThat(requestCapture.getAllValues().get(0).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(0).getRoutingState().getState(), equalTo(RoutingState.STARTED));

        assertThat(startTaskCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(requestCapture.getAllValues().get(1).getModelId(), equalTo(failedModelToLoad));
        assertThat(requestCapture.getAllValues().get(1).getNodeId(), equalTo(NODE_ID));
        assertThat(requestCapture.getAllValues().get(1).getRoutingState().getState(), equalTo(RoutingState.FAILED));

        verifyNoMoreInteractions(deploymentManager, trainedModelAllocationService);
    }

    public void testClusterChangedWithResetMode() {
        final TrainedModelAllocationNodeService trainedModelAllocationNodeService = createService();
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
                            TrainedModelAllocationMetadata.NAME,
                            TrainedModelAllocationMetadata.Builder.empty()
                                .addNewAllocation(newParams(modelOne))
                                .addNode(modelOne, NODE_ID)
                                .addNewAllocation(newParams(modelTwo))
                                .addNode(modelTwo, NODE_ID)
                                .addNewAllocation(newParams(notUsedModel))
                                .addNode(notUsedModel, "some-other-node")
                                .build()
                        )
                        .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(true).build())
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );

        trainedModelAllocationNodeService.clusterChanged(event);
        trainedModelAllocationNodeService.loadQueuedModels();
        verifyNoMoreInteractions(deploymentManager, trainedModelAllocationService);
    }

    public void testClusterChanged() throws Exception {
        final TrainedModelAllocationNodeService trainedModelAllocationNodeService = createService();
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
                            TrainedModelAllocationMetadata.NAME,
                            TrainedModelAllocationMetadata.Builder.empty()
                                .addNewAllocation(newParams(modelOne))
                                .addNode(modelOne, NODE_ID)
                                .addNewAllocation(newParams(modelTwo))
                                .addNode(modelTwo, NODE_ID)
                                .addNewAllocation(newParams(notUsedModel))
                                .addNode(notUsedModel, "some-other-node")
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );

        trainedModelAllocationNodeService.clusterChanged(event);

        event = new ClusterChangedEvent(
            "testClusterChanged",
            ClusterState.builder(new ClusterName("testClusterChanged"))
                .nodes(nodes)
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            TrainedModelAllocationMetadata.NAME,
                            TrainedModelAllocationMetadata.Builder.empty()
                                .addNewAllocation(newParams(modelOne))
                                .addNode(modelOne, NODE_ID)
                                .addNewAllocation(newParams(modelTwo))
                                .addNode(modelTwo, "some-other-node")
                                .addNewAllocation(newParams(notUsedModel))
                                .addNode(notUsedModel, "some-other-node")
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );
        trainedModelAllocationNodeService.clusterChanged(event);

        trainedModelAllocationNodeService.loadQueuedModels();

        assertBusy(() -> {
            ArgumentCaptor<TrainedModelDeploymentTask> stoppedTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
            verify(deploymentManager, times(1)).stopDeployment(stoppedTaskCapture.capture());
            assertThat(stoppedTaskCapture.getAllValues().get(0).getModelId(), equalTo(modelTwo));
        });
        ArgumentCaptor<TrainedModelDeploymentTask> startTaskCapture = ArgumentCaptor.forClass(TrainedModelDeploymentTask.class);
        ArgumentCaptor<UpdateTrainedModelAllocationStateAction.Request> requestCapture = ArgumentCaptor.forClass(
            UpdateTrainedModelAllocationStateAction.Request.class
        );
        verify(deploymentManager, times(1)).startDeployment(startTaskCapture.capture(), any());
        verify(trainedModelAllocationService, times(1)).updateModelAllocationState(requestCapture.capture(), any());

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
                            TrainedModelAllocationMetadata.NAME,
                            TrainedModelAllocationMetadata.Builder.empty()
                                .addNewAllocation(newParams(modelOne))
                                .addNode(modelOne, NODE_ID)
                                .build()
                        )
                        .build()
                )
                .build(),
            ClusterState.EMPTY_STATE
        );
        trainedModelAllocationNodeService.clusterChanged(event);

        trainedModelAllocationNodeService.loadQueuedModels();

        verifyNoMoreInteractions(deploymentManager, trainedModelAllocationService);
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

    private static StartTrainedModelDeploymentAction.TaskParams newParams(String modelId) {
        return new StartTrainedModelDeploymentAction.TaskParams(modelId, randomNonNegativeLong());
    }

    private TrainedModelAllocationNodeService createService() {
        return new TrainedModelAllocationNodeService(
            trainedModelAllocationService,
            clusterService,
            deploymentManager,
            taskManager,
            threadPool,
            NODE_ID
        );
    }

}
