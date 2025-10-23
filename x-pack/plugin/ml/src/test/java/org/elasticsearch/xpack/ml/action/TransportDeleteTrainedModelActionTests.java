/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAction.cancelDownloadTask;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.getTaskInfoListOfOne;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.mockClientWithTasksResponse;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.mockListTasksClient;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeleteTrainedModelActionTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testCancelDownloadTaskCallsListenerWithNullWhenNoTasksExist() {
        var client = mockClientWithTasksResponse(Collections.emptyList(), threadPool);
        var listener = new PlainActionFuture<ListTasksResponse>();

        cancelDownloadTask(client, "inferenceEntityId", listener, TIMEOUT);

        assertThat(listener.actionGet(TIMEOUT), nullValue());
    }

    public void testCancelDownloadTaskCallsOnFailureWithErrorWhenCancellingFailsWithAnError() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne(), threadPool);
        mockCancelTask(client);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> listener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            listener.onFailure(new Exception("cancel error"));

            return Void.TYPE;
        }).when(client).execute(same(TransportCancelTasksAction.TYPE), any(), any());

        var listener = new PlainActionFuture<ListTasksResponse>();

        cancelDownloadTask(client, "inferenceEntityId", listener, TIMEOUT);

        var exception = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to cancel task for model id [inferenceEntityId]"));
    }

    public void testCancelDownloadTaskCallsOnResponseNullWhenTheTaskNoLongerExistsWhenCancelling() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne(), threadPool);
        mockCancelTask(client);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> listener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            listener.onFailure(new ResourceNotFoundException("task no longer there"));

            return Void.TYPE;
        }).when(client).execute(same(TransportCancelTasksAction.TYPE), any(), any());

        var listener = new PlainActionFuture<ListTasksResponse>();

        cancelDownloadTask(client, "inferenceEntityId", listener, TIMEOUT);

        assertThat(listener.actionGet(TIMEOUT), nullValue());
    }

    public void testCancelDownloadTasksCallsGetsUnableToRetrieveTaskInfoError() {
        var client = mockListTasksClient(threadPool);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onFailure(new Exception("error"));

            return Void.TYPE;
        }).when(client).execute(same(TransportListTasksAction.TYPE), any(), any());

        var listener = new PlainActionFuture<ListTasksResponse>();

        cancelDownloadTask(client, "inferenceEntityId", listener, TIMEOUT);

        var exception = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to retrieve existing task information for model id [inferenceEntityId]"));
    }

    public void testCancelDownloadTaskCallsOnResponseWithTheCancelResponseWhenATaskExists() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne(), threadPool);

        var cancelResponse = mock(ListTasksResponse.class);
        mockCancelTasksResponse(client, cancelResponse);

        var listener = new PlainActionFuture<ListTasksResponse>();

        cancelDownloadTask(client, "inferenceEntityId", listener, TIMEOUT);

        assertThat(listener.actionGet(TIMEOUT), is(cancelResponse));
    }

    public void testModelExistsIsTrueWhenModelIsFound() throws Exception {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);
        TrainedModelConfig expectedConfig = buildTrainedModelConfig("modelId");

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onResponse(expectedConfig);
            return null;
        }).when(trainedModelProvider).getTrainedModel(any(), any(), any(), any());

        TransportDeleteTrainedModelAction action = createTransportDeleteTrainedModelAction(trainedModelProvider);

        // Use a future to capture the async listener result
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        action.modelExists("modelId", null, future);

        assertTrue(future.get());
    }

    public void testModelExistsIsFalseWhenModelIsNotFound() throws Exception {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onFailure(new ResourceNotFoundException("not found"));
            return null;
        }).when(trainedModelProvider).getTrainedModel(any(), any(), any(), any());

        TransportDeleteTrainedModelAction action = createTransportDeleteTrainedModelAction(trainedModelProvider);

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        action.modelExists("modelId", null, future);

        assertFalse(future.get());
    }

    public void testDeleteModelThrowsExceptionWhenModelIsNotFound() throws Exception {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onFailure(new ResourceNotFoundException("not found"));
            return null;
        }).when(trainedModelProvider).getTrainedModel(any(), any(), any(), any());

        TransportDeleteTrainedModelAction action = createTransportDeleteTrainedModelAction(trainedModelProvider);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();

        action.deleteModel(new DeleteTrainedModelAction.Request("modelId"), clusterState, null, future);

        ExecutionException executionException = expectThrows(ExecutionException.class, future::get);
        Throwable cause = executionException.getCause();

        assertThat(cause, instanceOf(ResourceNotFoundException.class));
        assertThat(cause.getMessage(), containsString("Could not find trained model"));
    }

    private static void mockCancelTask(Client client) {
        var cluster = client.admin().cluster();
        when(cluster.prepareCancelTasks()).thenReturn(new CancelTasksRequestBuilder(client));
    }

    private static void mockCancelTasksResponse(Client client, ListTasksResponse response) {
        mockCancelTask(client);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> listener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);

            return Void.TYPE;
        }).when(client).execute(same(TransportCancelTasksAction.TYPE), any(), any());
    }

    private TransportDeleteTrainedModelAction createTransportDeleteTrainedModelAction(TrainedModelProvider configProvider) {
        TransportService mockTransportService = mock(TransportService.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        ActionFilters mockFilters = mock(ActionFilters.class);
        Client mockClient = mock(Client.class);
        InferenceAuditor auditor = mock(InferenceAuditor.class);

        return new TransportDeleteTrainedModelAction(
            mockTransportService,
            mockClusterService,
            threadPool,
            mockClient,
            mockFilters,
            configProvider,  // TrainedModelProvider
            auditor,         // InferenceAuditor
            mock(org.elasticsearch.ingest.IngestService.class),
            mock(org.elasticsearch.cluster.project.ProjectResolver.class)
        );
    }

    private static TrainedModelConfig buildTrainedModelConfig(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType(TrainedModelType.TREE_ENSEMBLE)
            .setVersion(MlConfigVersion.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setModelSize(0)
            .setEstimatedOperations(0)
            .setInput(TrainedModelInputTests.createRandomInput())
            .build();
    }
}
