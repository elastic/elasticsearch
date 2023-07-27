/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAction.cancelDownloadTask;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.getTaskInfoListOfOne;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.mockClientWithTasksResponse;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.mockListTasksClient;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeleteTrainedModelActionTests extends ESTestCase {
    public void testCancelDownloadTaskCallsListenerWithNullWhenNoTasksExist() {
        var client = mockClientWithTasksResponse(Collections.emptyList());
        var listener = new PlainActionFuture<CancelTasksResponse>();

        cancelDownloadTask(client, "modelId", listener);

        assertThat(listener.actionGet(), nullValue());
    }

    public void testCancelDownloadTaskCallsOnFailureWithErrorWhenCancellingFailsWithAnError() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne());
        mockCancelTask(client);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CancelTasksResponse> listener = (ActionListener<CancelTasksResponse>) invocationOnMock.getArguments()[2];
            listener.onFailure(new Exception("cancel error"));

            return Void.TYPE;
        }).when(client).execute(same(CancelTasksAction.INSTANCE), any(), any());

        var listener = new PlainActionFuture<CancelTasksResponse>();

        cancelDownloadTask(client, "modelId", listener);

        var exception = expectThrows(ElasticsearchException.class, listener::actionGet);
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to cancel task for model id [modelId]"));
    }

    public void testCancelDownloadTasksCallsGetsUnableToRetrieveTaskInfoError() {
        var client = mockListTasksClient();

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onFailure(new Exception("error"));

            return Void.TYPE;
        }).when(client).execute(same(ListTasksAction.INSTANCE), any(), any());

        var listener = new PlainActionFuture<CancelTasksResponse>();

        cancelDownloadTask(client, "modelId", listener);

        var exception = expectThrows(ElasticsearchException.class, listener::actionGet);
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to retrieve existing task information for model id [modelId]"));
    }

    public void testCancelDownloadTaskCallsOnResponseWithTheCancelResponseWhenATaskExists() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne());

        var cancelResponse = mock(CancelTasksResponse.class);
        mockCancelTasksResponse(client, cancelResponse);

        var listener = new PlainActionFuture<CancelTasksResponse>();

        cancelDownloadTask(client, "modelId", listener);

        assertThat(listener.actionGet(), is(cancelResponse));
    }

    private static void mockCancelTask(Client client) {
        var cluster = client.admin().cluster();
        when(cluster.prepareCancelTasks()).thenReturn(new CancelTasksRequestBuilder(client, CancelTasksAction.INSTANCE));
    }

    private static void mockCancelTasksResponse(Client client, CancelTasksResponse response) {
        mockCancelTask(client);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CancelTasksResponse> listener = (ActionListener<CancelTasksResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);

            return Void.TYPE;
        }).when(client).execute(same(CancelTasksAction.INSTANCE), any(), any());
    }
}
