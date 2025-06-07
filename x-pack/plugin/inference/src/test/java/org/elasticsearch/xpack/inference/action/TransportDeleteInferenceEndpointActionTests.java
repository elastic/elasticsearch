/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportDeleteInferenceEndpointActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private TransportDeleteInferenceEndpointAction action;
    private ThreadPool threadPool;
    private ModelRegistry mockModelRegistry;
    private InferenceServiceRegistry mockInferenceServiceRegistry;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityPool());
        mockModelRegistry = mock(ModelRegistry.class);
        mockInferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        action = new TransportDeleteInferenceEndpointAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            mockModelRegistry,
            mockInferenceServiceRegistry
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testFailsToDelete_ADefaultEndpoint_WithoutPassingForceQueryParameter() {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel("model_id", TaskType.COMPLETION, "service", Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(anyString(), any());
        when(mockModelRegistry.containsDefaultConfigId(anyString())).thenReturn(true);

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request("model-id", TaskType.COMPLETION, false, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("[model-id] is a reserved inference endpoint. Use the force=true query parameter to delete the inference endpoint.")
        );
    }

    public void testDeletesDefaultEndpoint_WhenForceIsTrue() {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel("model_id", TaskType.COMPLETION, "service", Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(anyString(), any());
        when(mockModelRegistry.containsDefaultConfigId(anyString())).thenReturn(true);
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(1);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModel(anyString(), any());

        var mockService = mock(InferenceService.class);
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(1);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(mockService).stop(any(), any());

        when(mockInferenceServiceRegistry.getService(anyString())).thenReturn(Optional.of(mockService));

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request("model-id", TaskType.COMPLETION, true, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var response = listener.actionGet(TIMEOUT);

        assertTrue(response.isAcknowledged());
    }

    public void testFailsToDeleteUnparsableEndpoint_WhenForceIsFalse() {
        var inferenceEndpointId = randomAlphaOfLengthBetween(5, 10);
        var serviceName = randomAlphanumericOfLength(10);
        var taskType = randomFrom(TaskType.values());
        var mockService = mock(InferenceService.class);
        mockUnparsableModel(inferenceEndpointId, serviceName, taskType, mockService);
        when(mockModelRegistry.containsDefaultConfigId(inferenceEndpointId)).thenReturn(false);

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();
        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request(inferenceEndpointId, taskType, false, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), containsString("Failed to parse model configuration for inference endpoint"));

        verify(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        verify(mockInferenceServiceRegistry).getService(eq(serviceName));
        verify(mockModelRegistry).containsDefaultConfigId(eq(inferenceEndpointId));
        verify(mockService).parsePersistedConfig(eq(inferenceEndpointId), eq(taskType), any());
        verifyNoMoreInteractions(mockModelRegistry, mockInferenceServiceRegistry, mockService);
    }

    public void testDeletesUnparsableEndpoint_WhenForceIsTrue() {
        var inferenceEndpointId = randomAlphaOfLengthBetween(5, 10);
        var serviceName = randomAlphanumericOfLength(10);
        var taskType = randomFrom(TaskType.values());
        var mockService = mock(InferenceService.class);
        mockUnparsableModel(inferenceEndpointId, serviceName, taskType, mockService);
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(1);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModel(eq(inferenceEndpointId), any());

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request(inferenceEndpointId, taskType, true, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var response = listener.actionGet(TIMEOUT);
        assertTrue(response.isAcknowledged());

        verify(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        verify(mockInferenceServiceRegistry).getService(eq(serviceName));
        verify(mockService).parsePersistedConfig(eq(inferenceEndpointId), eq(taskType), any());
        verify(mockModelRegistry).deleteModel(eq(inferenceEndpointId), any());
        verifyNoMoreInteractions(mockModelRegistry, mockInferenceServiceRegistry, mockService);
    }

    private void mockUnparsableModel(String inferenceEndpointId, String serviceName, TaskType taskType, InferenceService mockService) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel(inferenceEndpointId, taskType, serviceName, Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        doThrow(new ElasticsearchStatusException(randomAlphanumericOfLength(10), RestStatus.INTERNAL_SERVER_ERROR)).when(mockService)
            .parsePersistedConfig(eq(inferenceEndpointId), eq(taskType), any());
        when(mockInferenceServiceRegistry.getService(serviceName)).thenReturn(Optional.of(mockService));
    }

    public void testDeletesEndpointWithNoService_WhenForceIsTrue() {
        var inferenceEndpointId = randomAlphaOfLengthBetween(5, 10);
        var serviceName = randomAlphanumericOfLength(10);
        var taskType = randomFrom(TaskType.values());
        mockNoService(inferenceEndpointId, serviceName, taskType);
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(1);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModel(anyString(), any());

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request(inferenceEndpointId, taskType, true, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var response = listener.actionGet(TIMEOUT);
        assertTrue(response.isAcknowledged());
        verify(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        verify(mockInferenceServiceRegistry).getService(eq(serviceName));
        verify(mockModelRegistry).deleteModel(eq(inferenceEndpointId), any());
        verifyNoMoreInteractions(mockModelRegistry, mockInferenceServiceRegistry);
    }

    public void testFailsToDeleteEndpointWithNoService_WhenForceIsFalse() {
        var inferenceEndpointId = randomAlphaOfLengthBetween(5, 10);
        var serviceName = randomAlphanumericOfLength(10);
        var taskType = randomFrom(TaskType.values());
        mockNoService(inferenceEndpointId, serviceName, taskType);
        when(mockModelRegistry.containsDefaultConfigId(inferenceEndpointId)).thenReturn(false);

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request(inferenceEndpointId, taskType, false, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), containsString("No service found for this inference endpoint"));
        verify(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        verify(mockInferenceServiceRegistry).getService(eq(serviceName));
        verify(mockModelRegistry).containsDefaultConfigId(eq(inferenceEndpointId));
        verifyNoMoreInteractions(mockModelRegistry, mockInferenceServiceRegistry);
    }

    private void mockNoService(String inferenceEndpointId, String serviceName, TaskType taskType) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel(inferenceEndpointId, taskType, serviceName, Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        when(mockInferenceServiceRegistry.getService(serviceName)).thenReturn(Optional.empty());
    }

    public void testFailsToDeleteEndpointIfModelDeploymentStopFails_WhenForceIsFalse() {
        var inferenceEndpointId = randomAlphaOfLengthBetween(5, 10);
        var serviceName = randomAlphanumericOfLength(10);
        var taskType = randomFrom(TaskType.values());
        var mockService = mock(InferenceService.class);
        var mockModel = mock(Model.class);
        mockStopDeploymentFails(inferenceEndpointId, serviceName, taskType, mockService, mockModel);
        when(mockModelRegistry.containsDefaultConfigId(inferenceEndpointId)).thenReturn(false);

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();
        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request(inferenceEndpointId, taskType, false, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), containsString("Failed to stop model deployment"));
        verify(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        verify(mockInferenceServiceRegistry).getService(eq(serviceName));
        verify(mockModelRegistry).containsDefaultConfigId(eq(inferenceEndpointId));
        verify(mockService).parsePersistedConfig(eq(inferenceEndpointId), eq(taskType), any());
        verify(mockService).stop(eq(mockModel), any());
        verifyNoMoreInteractions(mockModelRegistry, mockInferenceServiceRegistry, mockService, mockModel);
    }

    public void testDeletesEndpointIfModelDeploymentStopFails_WhenForceIsTrue() {
        var inferenceEndpointId = randomAlphaOfLengthBetween(5, 10);
        var serviceName = randomAlphanumericOfLength(10);
        var taskType = randomFrom(TaskType.values());
        var mockService = mock(InferenceService.class);
        var mockModel = mock(Model.class);
        mockStopDeploymentFails(inferenceEndpointId, serviceName, taskType, mockService, mockModel);
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(1);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModel(eq(inferenceEndpointId), any());

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();
        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request(inferenceEndpointId, taskType, true, false),
            ClusterState.EMPTY_STATE,
            listener
        );

        var response = listener.actionGet(TIMEOUT);
        assertTrue(response.isAcknowledged());
        verify(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        verify(mockInferenceServiceRegistry).getService(eq(serviceName));
        verify(mockService).parsePersistedConfig(eq(inferenceEndpointId), eq(taskType), any());
        verify(mockService).stop(eq(mockModel), any());
        verify(mockModelRegistry).deleteModel(eq(inferenceEndpointId), any());
        verifyNoMoreInteractions(mockModelRegistry, mockInferenceServiceRegistry, mockService, mockModel);
    }

    private void mockStopDeploymentFails(
        String inferenceEndpointId,
        String serviceName,
        TaskType taskType,
        InferenceService mockService,
        Model mockModel
    ) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel(inferenceEndpointId, taskType, serviceName, Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(eq(inferenceEndpointId), any());
        when(mockInferenceServiceRegistry.getService(serviceName)).thenReturn(Optional.of(mockService));
        doReturn(mockModel).when(mockService).parsePersistedConfig(eq(inferenceEndpointId), eq(taskType), any());
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(1);
            listener.onFailure(new ElasticsearchStatusException("Failed to stop model deployment", RestStatus.INTERNAL_SERVER_ERROR));
            return Void.TYPE;
        }).when(mockService).stop(eq(mockModel), any());
    }

}
