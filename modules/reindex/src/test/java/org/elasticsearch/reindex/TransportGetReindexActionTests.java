/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetReindexActionTests extends ESTestCase {

    private TransportGetReindexAction action;
    private ClusterAdminClient clusterAdminClient;

    @Before
    public void setUp() {
        Client client = mock();
        AdminClient adminClient = mock();
        clusterAdminClient = mock();
        TransportService transportService = mock();
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        action = new TransportGetReindexAction(transportService, mock(), client);
    }

    public void testGetReindexTask() {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 123));

        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            null,
            100,
            50,
            10,
            0,
            5,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            1.0f,
            null,
            TimeValue.ZERO
        );

        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            ReindexAction.NAME,
            "reindex test",
            "test",
            status,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        TaskResult taskResult = new TaskResult(false, taskInfo);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);

        ActionListener<GetReindexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> l = invocation.getArgument(1);
            l.onResponse(getTaskResponse);
            return null;
        }).when(clusterAdminClient).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<GetTaskRequest> getTaskRequestCaptor = ArgumentCaptor.forClass(GetTaskRequest.class);
        verify(clusterAdminClient).getTask(getTaskRequestCaptor.capture(), any());

        GetTaskRequest capturedRequest = getTaskRequestCaptor.getValue();
        assertThat(capturedRequest.getTaskId(), equalTo(new TaskId("node1", 123)));

        ArgumentCaptor<GetReindexResponse> responseCaptor = ArgumentCaptor.forClass(GetReindexResponse.class);
        verify(listener).onResponse(responseCaptor.capture());

        GetReindexResponse response = responseCaptor.getValue();
        assertThat(response, notNullValue());
        TaskResult task = response.getTask();
        assertThat(task, notNullValue());
        assertThat(task.isCompleted(), equalTo(false));
    }

    public void testGetNonReindexTask() {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 123));

        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/update_by_query", // Not a reindex action
            "update by query test",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        TaskResult taskResult = new TaskResult(false, taskInfo);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);

        ActionListener<GetReindexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> l = invocation.getArgument(1);
            l.onResponse(getTaskResponse);
            return null;
        }).when(clusterAdminClient).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        Exception exception = exceptionCaptor.getValue();
        assertThat(exception, instanceOf(ResourceNotFoundException.class));
        assertThat(exception.getMessage(), containsString("Reindex"));
        assertThat(exception.getMessage(), containsString("not found"));
    }

    public void testGetNonExistentTask() {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 999));

        ActionListener<GetReindexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> l = invocation.getArgument(1);
            l.onFailure(new ResourceNotFoundException("task not found"));
            return null;
        }).when(clusterAdminClient).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        Exception exception = exceptionCaptor.getValue();
        assertThat(exception, instanceOf(ResourceNotFoundException.class));
        assertThat(exception.getMessage(), containsString("Reindex"));
        assertThat(exception.getMessage(), containsString("not found"));
    }

    public void testGetCompletedReindexTaskWithResponse() throws IOException {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 123));

        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            ReindexAction.NAME,
            "reindex test",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        BytesReference responseBytes = new BytesArray("{\"took\":100}");
        TaskResult taskResult = new TaskResult(taskInfo, responseBytes);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);

        ActionListener<GetReindexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> l = invocation.getArgument(1);
            l.onResponse(getTaskResponse);
            return null;
        }).when(clusterAdminClient).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<GetReindexResponse> responseCaptor = ArgumentCaptor.forClass(GetReindexResponse.class);
        verify(listener).onResponse(responseCaptor.capture());

        GetReindexResponse response = responseCaptor.getValue();
        assertThat(response, notNullValue());
        TaskResult task = response.getTask();
        assertThat(task, notNullValue());
        assertThat(task.isCompleted(), equalTo(true));
        assertThat(task.getResponse(), notNullValue());
        assertThat(task.getError(), nullValue());
    }

    public void testGetCompletedReindexTaskWithError() throws IOException {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 123));

        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            ReindexAction.NAME,
            "reindex test",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        Exception error = new RuntimeException("test error");
        TaskResult taskResult = new TaskResult(taskInfo, error);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);

        ActionListener<GetReindexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> l = invocation.getArgument(1);
            l.onResponse(getTaskResponse);
            return null;
        }).when(clusterAdminClient).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<GetReindexResponse> responseCaptor = ArgumentCaptor.forClass(GetReindexResponse.class);
        verify(listener).onResponse(responseCaptor.capture());

        GetReindexResponse response = responseCaptor.getValue();
        assertThat(response, notNullValue());
        TaskResult task = response.getTask();
        assertThat(task, notNullValue());
        assertThat(task.isCompleted(), equalTo(true));
        assertThat(task.getError(), notNullValue());
        assertThat(task.getResponse(), nullValue());
    }
}
