/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.reindex.management.TransportCancelReindexAction.notFoundException;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCancelReindexActionTests extends ESTestCase {

    private TaskId taskId;
    private TransportCancelReindexAction action;
    private Client client;
    private AtomicReference<CancelReindexResponse> responseRef;
    private AtomicReference<Exception> failureRef;
    private ActionListener<CancelReindexResponse> listener;

    @Before
    public void setup() {
        taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000));
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        // The action constructor reads transportService.getThreadPool().executor(GENERIC); the executor is only used for incoming
        // request dispatch (not exercised in these direct doExecute tests), so an inline executor is sufficient.
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        action = new TransportCancelReindexAction(transportService, mock(), client);
        responseRef = new AtomicReference<>();
        failureRef = new AtomicReference<>();
        listener = ActionListener.wrap(responseRef::set, failureRef::set);
    }

    public void testCancelTaskRequestIsScopedToReindexParentTasks() {
        mockCancelTasks(new ListTasksResponse(List.of(taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID)), List.of(), List.of()));

        action.doExecute(mock(), new CancelReindexRequest(taskId, false), listener);

        ArgumentCaptor<CancelTasksRequest> captor = ArgumentCaptor.captor();
        verify(client).execute(eq(TransportCancelTasksAction.TYPE), captor.capture(), any());
        CancelTasksRequest sent = captor.getValue();
        assertThat(sent.getTargetTaskId(), is(taskId));
        assertThat(sent.getActions(), is(new String[] { ReindexAction.NAME }));
        assertThat("parent_task_only is what protects us from sub-task cancellation", sent.parentTaskOnly(), is(true));
        assertThat("waitForCompletion is handled separately via Get Reindex", sent.waitForCompletion(), is(false));
    }

    public void testCancelAsynchronouslyReturnsAcknowledged() {
        mockCancelTasks(new ListTasksResponse(List.of(taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID)), List.of(), List.of()));

        action.doExecute(mock(), new CancelReindexRequest(taskId, false), listener);

        assertNull(failureRef.get());
        assertNotNull(responseRef.get());
        assertThat(responseRef.get().getCompletedReindexResponse().isPresent(), is(false));
        verify(client, never()).execute(eq(TransportGetReindexAction.TYPE), any(), any());
    }

    public void testCancelSynchronouslyEmbedsGetReindexResponseWithCancelledTrue() {
        TaskInfo info = taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        mockCancelTasks(new ListTasksResponse(List.of(info), List.of(), List.of()));
        mockGetReindex(new GetReindexResponse(new TaskResult(true, info)));

        action.doExecute(mock(), new CancelReindexRequest(taskId, true), listener);

        assertNull(failureRef.get());
        assertNotNull(responseRef.get());
        TaskInfo embedded = responseRef.get().getCompletedReindexResponse().orElseThrow().getTaskResult().getTask();
        assertThat("cancelled is forced to true even if GET reflects pre-cancel state", embedded.cancelled(), is(true));
    }

    public void testCancelEmptyTasksTreatedAsNotFound() {
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of()));

        action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
    }

    public void testNodeFailureWithResourceNotFoundCauseTreatedAsNotFound() {
        FailedNodeException nodeFailure = new FailedNodeException(taskId.getNodeId(), "node failed", new ResourceNotFoundException("gone"));
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
    }

    public void testNodeFailureWithIllegalArgumentCauseTreatedAsNotFound() {
        // This is the path for a sub-task targeted with parent_task_only=true: cancel-tasks throws IAE("doesn't support this operation").
        FailedNodeException nodeFailure = new FailedNodeException(
            taskId.getNodeId(),
            "node failed",
            new IllegalArgumentException("task [" + taskId + "] doesn't support this operation")
        );
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
    }

    public void testNonValidationNodeFailurePropagated() {
        // A real transport-level failure (e.g. node disconnected) must not be silently mapped to 404.
        RuntimeException underlying = new RuntimeException("connection refused");
        FailedNodeException nodeFailure = new FailedNodeException(taskId.getNodeId(), "node failed", underlying);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), listener);

        assertNull(responseRef.get());
        assertNotNull(failureRef.get());
        assertThat(
            "real node failures must surface, not be swallowed as not-found",
            failureRef.get(),
            instanceOf(FailedNodeException.class)
        );
        assertSame(underlying, failureRef.get().getCause());
    }

    public void testTaskFailurePropagatedAsConflict() {
        // 409 from the relocation gate comes back as a TaskOperationFailure; it must be surfaced.
        ElasticsearchStatusException conflict = new ElasticsearchStatusException(
            "cannot cancel task [" + taskId.getId() + "] because it is being relocated",
            RestStatus.CONFLICT
        );
        TaskOperationFailure taskFailure = new TaskOperationFailure(taskId.getNodeId(), taskId.getId(), conflict);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(taskFailure), List.of()));

        action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), listener);

        assertNull(responseRef.get());
        assertNotNull(failureRef.get());
        assertThat(failureRef.get().getMessage(), containsString("cancel_reindex of"));
        assertThat(failureRef.get().getCause(), is(conflict));
    }

    public void testGetReindexFailurePropagated() {
        mockCancelTasks(new ListTasksResponse(List.of(taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID)), List.of(), List.of()));

        RuntimeException getFailure = new RuntimeException("boom");
        doAnswer((InvocationOnMock inv) -> {
            ActionListener<GetReindexResponse> l = inv.getArgument(2);
            l.onFailure(getFailure);
            return null;
        }).when(client).execute(eq(TransportGetReindexAction.TYPE), any(), any());

        action.doExecute(mock(), new CancelReindexRequest(taskId, true), listener);

        assertNull(responseRef.get());
        assertSame(getFailure, failureRef.get());
    }

    // --- helpers ---

    private TaskInfo taskInfo(TaskId taskId, String action, TaskId parent) {
        return new TaskInfo(taskId, "test", taskId.getNodeId(), action, "test", null, 0, 0, true, false, parent, Collections.emptyMap());
    }

    private void mockCancelTasks(ListTasksResponse response) {
        doAnswer(inv -> {
            ActionListener<ListTasksResponse> l = inv.getArgument(2);
            l.onResponse(response);
            return null;
        }).when(client).execute(eq(TransportCancelTasksAction.TYPE), any(CancelTasksRequest.class), any());
    }

    private void mockGetReindex(GetReindexResponse response) {
        doAnswer(inv -> {
            ActionListener<GetReindexResponse> l = inv.getArgument(2);
            l.onResponse(response);
            return null;
        }).when(client).execute(eq(TransportGetReindexAction.TYPE), any(), any());
    }
}
