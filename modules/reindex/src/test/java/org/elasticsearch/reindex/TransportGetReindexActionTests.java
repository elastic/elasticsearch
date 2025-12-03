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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.elasticsearch.reindex.TransportGetReindexAction.notFoundException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetReindexActionTests extends ESTestCase {

    private TaskId taskId;
    private TimeValue timeout;
    private TransportGetReindexAction action;
    private ClusterAdminClient client;

    @Before
    public void setup() {
        timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 600));
        taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000));
        client = mock();
        action = new TransportGetReindexAction(mock(), mock(), setupMockClient(client));
    }

    public void testReindexFound() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        TaskResult taskResult = new TaskResult(true, reindexTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        GetReindexResponse expectedResponse = new GetReindexResponse(taskResult);
        verify(listener).onResponse(eq(expectedResponse));

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequest = requestCaptor.getValue();
        assertEquals(taskId, capturedRequest.getTaskId());
        assertEquals(timeout, capturedRequest.getTimeout());
        assertFalse(capturedRequest.getWaitForCompletion());
    }

    public void testTaskNotFound() {
        GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        ResourceNotFoundException notFoundException = new ResourceNotFoundException("task not found");
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onFailure(notFoundException);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        verify(listener).onFailure(
            argThat(e -> e instanceof ResourceNotFoundException && e.getMessage().equals(notFoundException(taskId).getMessage()))
        );
    }

    public void testTreatNonReindexTasksAsNotFound() {
        TaskInfo otherTask = createTaskInfo(taskId, "other_action");
        TaskResult taskResult = new TaskResult(true, otherTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        verify(listener).onFailure(
            argThat(e -> e instanceof ResourceNotFoundException && e.getMessage().equals(notFoundException(taskId).getMessage()))
        );
    }

    public void testHandleOtherExceptions() {
        GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        RuntimeException otherException = new RuntimeException("some other error");
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onFailure(otherException);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        verify(listener).onFailure(eq(otherException));
    }

    public void testWaitForReindexToComplete() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        // set up get task result
        TaskResult taskResultIncomplete = new TaskResult(false, reindexTask);
        GetTaskResponse getTaskResponseIncomplete = new GetTaskResponse(taskResultIncomplete);
        TaskResult taskResultCompleted = new TaskResult(true, reindexTask);
        GetTaskResponse getTaskResponseCompleted = new GetTaskResponse(taskResultCompleted);
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponseIncomplete);
            return null;
        }).doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponseCompleted);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        GetReindexResponse expectedResponse = new GetReindexResponse(taskResultCompleted);
        verify(listener).onResponse(eq(expectedResponse));

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client, times(2)).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequestIncomplete = requestCaptor.getAllValues().getFirst();
        assertEquals(taskId, capturedRequestIncomplete.getTaskId());
        assertEquals(timeout, capturedRequestIncomplete.getTimeout());
        assertFalse(capturedRequestIncomplete.getWaitForCompletion());

        GetTaskRequest capturedRequestCompleted = requestCaptor.getAllValues().getLast();
        assertEquals(taskId, capturedRequestCompleted.getTaskId());
        assertEquals(timeout, capturedRequestCompleted.getTimeout());
        assertTrue(capturedRequestCompleted.getWaitForCompletion());
    }

    public void testDoNotWaitForNonReindex() {
        TaskInfo otherTask = createTaskInfo(taskId, "other_action");
        TaskResult taskResult = new TaskResult(false, otherTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        verify(listener).onFailure(
            argThat(e -> e instanceof ResourceNotFoundException && e.getMessage().equals(notFoundException(taskId).getMessage()))
        );

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequest = requestCaptor.getValue();
        assertEquals(taskId, capturedRequest.getTaskId());
        assertEquals(timeout, capturedRequest.getTimeout());
        assertFalse(capturedRequest.getWaitForCompletion());
    }

    public void testDoNotWaitForCompletedTask() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        // set up get task result
        TaskResult taskResult = new TaskResult(true, reindexTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        GetReindexResponse expectedResponse = new GetReindexResponse(taskResult);
        verify(listener).onResponse(eq(expectedResponse));

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequest = requestCaptor.getValue();
        assertEquals(taskId, capturedRequest.getTaskId());
        assertEquals(timeout, capturedRequest.getTimeout());
        assertFalse(capturedRequest.getWaitForCompletion());
    }

    public void testHandleExceptionInWaitForTask() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        TaskResult taskResultIncomplete = new TaskResult(false, reindexTask);
        GetTaskResponse getTaskResponseIncomplete = new GetTaskResponse(taskResultIncomplete);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);
        ActionListener<GetReindexResponse> listener = mock();

        RuntimeException expectedException = new RuntimeException("random error");
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponseIncomplete);
            return null;
        }).doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onFailure(expectedException);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        verify(listener).onFailure(eq(expectedException));

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client, times(2)).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequestIncomplete = requestCaptor.getAllValues().getFirst();
        assertEquals(taskId, capturedRequestIncomplete.getTaskId());
        assertEquals(timeout, capturedRequestIncomplete.getTimeout());
        assertFalse(capturedRequestIncomplete.getWaitForCompletion());

        GetTaskRequest capturedRequestCompleted = requestCaptor.getAllValues().getLast();
        assertEquals(taskId, capturedRequestCompleted.getTaskId());
        assertEquals(timeout, capturedRequestCompleted.getTimeout());
        assertTrue(capturedRequestCompleted.getWaitForCompletion());
    }

    private TaskInfo createTaskInfo(TaskId taskId, String action) {
        return new TaskInfo(
            taskId,
            "test",
            taskId.getNodeId(),
            action,
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
    }

    private GetReindexRequest createGetReindexRequest(TaskId taskId, boolean waitForCompletion, TimeValue timeout) {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(taskId);
        request.setWaitForCompletion(waitForCompletion);
        request.setTimeout(timeout);
        return request;
    }

    private Client setupMockClient(ClusterAdminClient clusterAdminClient) {
        Client client = mock();
        AdminClient adminClient = mock();
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        return client;
    }

}
