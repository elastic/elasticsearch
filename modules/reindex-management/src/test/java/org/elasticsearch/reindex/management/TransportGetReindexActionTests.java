/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.reindex.TaskRelocatedException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.reindex.management.TransportGetReindexAction.notFoundException;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
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
    private AtomicReference<GetReindexResponse> responseRef;
    private AtomicReference<Exception> failureRef;
    private ActionListener<GetReindexResponse> listener;

    @Before
    public void setup() {
        timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 600));
        taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000));
        client = mock();
        action = new TransportGetReindexAction(mock(), mock(), setupMockClient(client));
        responseRef = new AtomicReference<>();
        failureRef = new AtomicReference<>();
        listener = ActionListener.wrap(responseRef::set, failureRef::set);
    }

    public void testReindexFound() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        TaskResult taskResult = new TaskResult(true, reindexTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        GetReindexResponse expectedResponse = new GetReindexResponse(taskResult);
        assertEquals(expectedResponse, responseRef.get());

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequest = requestCaptor.getValue();
        assertEquals(taskId, capturedRequest.getTaskId());
        assertFalse(capturedRequest.getWaitForCompletion());
    }

    public void testTaskNotFound() {
        GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);

        ResourceNotFoundException notFoundException = new ResourceNotFoundException("task not found");
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onFailure(notFoundException);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
    }

    public void testTreatNonReindexTasksAsNotFound() {
        TaskInfo otherTask = createTaskInfo(taskId, "other_action");
        TaskResult taskResult = new TaskResult(true, otherTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
    }

    public void testHandleOtherExceptions() {
        GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);

        RuntimeException otherException = new RuntimeException("some other error");
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onFailure(otherException);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertSame(otherException, failureRef.get());
    }

    public void testWaitForReindexToComplete() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);

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

        assertNull(failureRef.get());
        GetReindexResponse expectedResponse = new GetReindexResponse(taskResultCompleted);
        assertEquals(expectedResponse, responseRef.get());

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client, times(2)).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequestIncomplete = requestCaptor.getAllValues().getFirst();
        assertEquals(taskId, capturedRequestIncomplete.getTaskId());
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

        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequest = requestCaptor.getValue();
        assertEquals(taskId, capturedRequest.getTaskId());
        assertFalse(capturedRequest.getWaitForCompletion());
    }

    public void testDoNotWaitForCompletedTask() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);

        // set up get task result
        TaskResult taskResult = new TaskResult(true, reindexTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);
        doAnswer(invocation -> {
            ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(getTaskResponse);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        GetReindexResponse expectedResponse = new GetReindexResponse(taskResult);
        assertEquals(expectedResponse, responseRef.get());

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequest = requestCaptor.getValue();
        assertEquals(taskId, capturedRequest.getTaskId());
        assertFalse(capturedRequest.getWaitForCompletion());
    }

    public void testHandleExceptionInWaitForTask() {
        TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        TaskResult taskResultIncomplete = new TaskResult(false, reindexTask);
        GetTaskResponse getTaskResponseIncomplete = new GetTaskResponse(taskResultIncomplete);
        GetReindexRequest request = createGetReindexRequest(taskId, true, timeout);

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

        assertNull(responseRef.get());
        assertSame(expectedException, failureRef.get());

        ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client, times(2)).getTask(requestCaptor.capture(), any());

        GetTaskRequest capturedRequestIncomplete = requestCaptor.getAllValues().getFirst();
        assertEquals(taskId, capturedRequestIncomplete.getTaskId());
        assertFalse(capturedRequestIncomplete.getWaitForCompletion());

        GetTaskRequest capturedRequestCompleted = requestCaptor.getAllValues().getLast();
        assertEquals(taskId, capturedRequestCompleted.getTaskId());
        assertEquals(timeout, capturedRequestCompleted.getTimeout());
        assertTrue(capturedRequestCompleted.getWaitForCompletion());
    }

    public void testNoRelocationReturnsAsIs() {
        final TaskInfo reindexTask = createTaskInfo(taskId, ReindexAction.NAME);
        final TaskResult taskResult = new TaskResult(false, reindexTask);
        final GetReindexRequest request = createGetReindexRequest(taskId, false, timeout);

        doAnswer(invocation -> {
            final ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(new GetTaskResponse(taskResult));
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertNotNull(responseRef.get());
        final GetReindexResponse response = responseRef.get();
        assertEquals(taskResult, response.getOriginalTask());
        assertTrue(response.getRelocatedTask().isEmpty());
    }

    public void testFollowsSingleRelocation() throws IOException {
        final TaskId originalTaskId = taskId;
        final TaskId relocatedTaskId = randomValueOtherThan(taskId, () -> new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000)));

        final TaskRelocatedException relocatedException = new TaskRelocatedException();
        relocatedException.setOriginalAndRelocatedTaskIdMetadata(originalTaskId, relocatedTaskId);

        final TaskInfo originalInfo = createTaskInfo(originalTaskId, ReindexAction.NAME);
        final TaskResult originalResult = new TaskResult(originalInfo, (Exception) relocatedException);

        final TaskInfo relocatedInfo = createTaskInfo(relocatedTaskId, ReindexAction.NAME);
        final TaskResult relocatedResult = new TaskResult(false, relocatedInfo);

        final GetReindexRequest request = createGetReindexRequest(originalTaskId, false, timeout);

        doAnswer(invocation -> {
            final ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(new GetTaskResponse(originalResult));
            return null;
        }).doAnswer(invocation -> {
            final ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(new GetTaskResponse(relocatedResult));
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertNotNull(responseRef.get());
        final GetReindexResponse response = responseRef.get();
        assertEquals(originalResult, response.getOriginalTask());
        assertThat(relocatedResult, equalTo(response.getRelocatedTask().orElseThrow()));

        final ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client, times(2)).getTask(requestCaptor.capture(), any());
        assertEquals(originalTaskId, requestCaptor.getAllValues().get(0).getTaskId());
        assertEquals(relocatedTaskId, requestCaptor.getAllValues().get(1).getTaskId());
    }

    public void testFollowsTwoRelocations() throws IOException {
        final TaskId originalTaskId = taskId;
        final TaskId firstRelocatedTaskId = randomValueOtherThan(
            taskId,
            () -> new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000))
        );
        final TaskId secondRelocatedTaskId = randomValueOtherThan(
            firstRelocatedTaskId,
            () -> new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000))
        );

        final TaskRelocatedException firstRelocation = new TaskRelocatedException();
        firstRelocation.setOriginalAndRelocatedTaskIdMetadata(originalTaskId, firstRelocatedTaskId);

        final TaskRelocatedException secondRelocation = new TaskRelocatedException();
        secondRelocation.setOriginalAndRelocatedTaskIdMetadata(firstRelocatedTaskId, secondRelocatedTaskId);

        final TaskInfo originalInfo = createTaskInfo(originalTaskId, ReindexAction.NAME);
        final TaskResult originalResult = new TaskResult(originalInfo, (Exception) firstRelocation);

        final TaskInfo firstRelocatedInfo = createTaskInfo(firstRelocatedTaskId, ReindexAction.NAME);
        final TaskResult firstRelocatedResult = new TaskResult(firstRelocatedInfo, (Exception) secondRelocation);

        final TaskInfo secondRelocatedInfo = createTaskInfo(secondRelocatedTaskId, ReindexAction.NAME);
        final TaskResult secondRelocatedResult = new TaskResult(false, secondRelocatedInfo);

        final GetReindexRequest request = createGetReindexRequest(originalTaskId, false, timeout);

        doAnswer(invocation -> {
            final ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(new GetTaskResponse(originalResult));
            return null;
        }).doAnswer(invocation -> {
            final ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(new GetTaskResponse(firstRelocatedResult));
            return null;
        }).doAnswer(invocation -> {
            final ActionListener<GetTaskResponse> inner = invocation.getArgument(1);
            inner.onResponse(new GetTaskResponse(secondRelocatedResult));
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertNotNull(responseRef.get());
        final GetReindexResponse response = responseRef.get();
        assertEquals(originalResult, response.getOriginalTask());
        assertEquals(secondRelocatedResult, response.getRelocatedTask().orElseThrow());

        final ArgumentCaptor<GetTaskRequest> requestCaptor = ArgumentCaptor.captor();
        verify(client, times(3)).getTask(requestCaptor.capture(), any());
        assertEquals(originalTaskId, requestCaptor.getAllValues().get(0).getTaskId());
        assertEquals(firstRelocatedTaskId, requestCaptor.getAllValues().get(1).getTaskId());
        assertEquals(secondRelocatedTaskId, requestCaptor.getAllValues().get(2).getTaskId());
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
        return new GetReindexRequest(taskId, waitForCompletion, timeout);
    }

    private Client setupMockClient(ClusterAdminClient clusterAdminClient) {
        Client client = mock();
        AdminClient adminClient = mock();
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        return client;
    }

}
