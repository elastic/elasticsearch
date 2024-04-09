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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    public void testModelExistsIsTrueWhenModelIsFound() {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);
        TrainedModelConfig expectedConfig = buildTrainedModelConfig("modelId");

        Mockito.doAnswer(invocation -> {
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onResponse(expectedConfig);
            return null;
        })
            .when(trainedModelProvider)
            .getTrainedModel(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.<ActionListener<TrainedModelConfig>>any());

        TransportDeleteTrainedModelAction transportDeleteTrainedModelAction = createTransportDeleteTrainedModelAction(trainedModelProvider);
        boolean modelExists = transportDeleteTrainedModelAction.modelExists("modelId");

        assertThat(modelExists, is(Boolean.TRUE));
    }

    public void testModelExistsIsFalseWhenModelIsNotFound() {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);
        Exception failureException = new Exception("Failed to retrieve model");

        Mockito.doAnswer(invocation -> {
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onFailure(failureException);
            return null;
        })
            .when(trainedModelProvider)
            .getTrainedModel(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.<ActionListener<TrainedModelConfig>>any());

        TransportDeleteTrainedModelAction transportDeleteTrainedModelAction = createTransportDeleteTrainedModelAction(trainedModelProvider);
        boolean modelExists = transportDeleteTrainedModelAction.modelExists("modelId");

        assertThat(modelExists, is(Boolean.FALSE));
    }

    public void testDeleteModelThrowsExceptionWhenModelIsNotFound() {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);
        Exception failureException = new Exception("Failed to retrieve model");
        ClusterState CLUSTER_STATE = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(".my-system").system(true).settings(indexSettings(IndexVersion.current(), 1, 0)).build(),
                        true
                    )
                    .build()
            )
            .build();
        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> mockedListener = mock(ActionListener.class);

        Mockito.doAnswer(invocation -> {
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onFailure(failureException);
            return null;
        })
            .when(trainedModelProvider)
            .getTrainedModel(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.<ActionListener<TrainedModelConfig>>any());

        TransportDeleteTrainedModelAction transportDeleteTrainedModelAction = createTransportDeleteTrainedModelAction(trainedModelProvider);
        transportDeleteTrainedModelAction.deleteModel(new DeleteTrainedModelAction.Request("modelId"), CLUSTER_STATE, mockedListener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockedListener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertThat(capturedException, is(instanceOf(ResourceNotFoundException.class)));
        assertThat(capturedException.getMessage(), containsString("Could not find trained model [modelId]"));
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
        doReturn(threadPool).when(mockTransportService).getThreadPool();
        ClusterService mockClusterService = mock(ClusterService.class);
        ActionFilters mockFilters = mock(ActionFilters.class);
        doReturn(null).when(mockFilters).filters();
        Client mockClient = mock(Client.class);
        doReturn(null).when(mockClient).settings();
        doReturn(threadPool).when(mockClient).threadPool();
        InferenceAuditor auditor = mock(InferenceAuditor.class);

        return new TransportDeleteTrainedModelAction(
            mockTransportService,
            mockClusterService,
            threadPool,
            null,
            mockFilters,
            null,
            configProvider,
            auditor,
            null
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
