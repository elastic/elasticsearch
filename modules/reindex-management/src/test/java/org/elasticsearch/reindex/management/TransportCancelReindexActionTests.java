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
import java.util.Map;

import static org.elasticsearch.reindex.management.TransportCancelReindexAction.notFoundException;
import static org.hamcrest.Matchers.arrayContaining;
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

    @Before
    public void setup() {
        taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000));
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        action = new TransportCancelReindexAction(transportService, mock(), client);
    }

    public void testCancelTaskRequestIsScopedToReindexParentTasks() {
        mockCancelTasks(new ListTasksResponse(List.of(taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID)), List.of(), List.of()));

        safeAwait((ActionListener<CancelReindexResponse> l) -> action.doExecute(mock(), new CancelReindexRequest(taskId, false), l));

        ArgumentCaptor<CancelTasksRequest> captor = ArgumentCaptor.captor();
        verify(client).execute(eq(TransportCancelTasksAction.TYPE), captor.capture(), any());
        CancelTasksRequest sent = captor.getValue();
        assertThat(sent.getTargetTaskId(), is(taskId));
        assertThat(sent.getActions(), is(new String[] { ReindexAction.NAME }));
        assertThat("exclude_child_tasks is what protects us from sub-task cancellation", sent.excludeChildTasks(), is(true));
        assertThat("waitForCompletion is handled separately via Get Reindex", sent.waitForCompletion(), is(false));
    }

    public void testCancelAsynchronouslyReturnsAcknowledged() {
        mockCancelTasks(new ListTasksResponse(List.of(taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID)), List.of(), List.of()));

        final CancelReindexResponse response = safeAwait(l -> action.doExecute(mock(), new CancelReindexRequest(taskId, false), l));

        assertThat(response.getCompletedReindexResponse().isPresent(), is(false));
        verify(client, never()).execute(eq(TransportGetReindexAction.TYPE), any(), any());
    }

    public void testCancelSynchronouslyPreservesOriginalTaskIdentityWhenRelocated() {
        TaskId originalTaskId = new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
        TaskId currentTaskId = randomValueOtherThan(originalTaskId, () -> new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()));
        long originalStart = between(0, 1_000);
        long currentStart = originalStart + randomLongBetween(1, 1_000);
        TaskInfo info = new TaskInfo(
            currentTaskId,
            randomAlphaOfLength(10),
            currentTaskId.getNodeId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null,
            currentStart,
            randomNonNegativeLong(),
            true,
            randomBoolean(),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            originalTaskId,
            originalStart
        );
        mockCancelTasks(new ListTasksResponse(List.of(info), List.of(), List.of()));
        mockGetReindex(new GetReindexResponse(new TaskResult(true, info)));

        final CancelReindexResponse response = safeAwait(l -> action.doExecute(mock(), new CancelReindexRequest(currentTaskId, true), l));

        TaskInfo embedded = response.getCompletedReindexResponse().orElseThrow().getTaskResult().getTask();
        assertTrue(embedded.cancelled());
        assertEquals(originalTaskId, embedded.originalTaskId());
        assertEquals(originalStart, embedded.originalStartTimeMillis());
        assertEquals(currentTaskId, embedded.taskId());
    }

    public void testCancelSynchronouslyEmbedsGetReindexResponseWithCancelledTrue() {
        TaskInfo info = taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        mockCancelTasks(new ListTasksResponse(List.of(info), List.of(), List.of()));
        mockGetReindex(new GetReindexResponse(new TaskResult(true, info)));

        final CancelReindexResponse response = safeAwait(l -> action.doExecute(mock(), new CancelReindexRequest(taskId, true), l));

        TaskInfo embedded = response.getCompletedReindexResponse().orElseThrow().getTaskResult().getTask();
        assertThat("cancelled is forced to true even if GET reflects pre-cancel state", embedded.cancelled(), is(true));
    }

    public void testCancelEmptyTasksTreatedAsNotFound() {
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of()));

        final ResourceNotFoundException failure = safeAwaitFailure(
            ResourceNotFoundException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertEquals(notFoundException(taskId).getMessage(), failure.getMessage());
    }

    public void testNodeFailureWithResourceNotFoundCauseTreatedAsNotFound() {
        FailedNodeException nodeFailure = new FailedNodeException(taskId.getNodeId(), "node failed", new ResourceNotFoundException("gone"));
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        final ResourceNotFoundException failure = safeAwaitFailure(
            ResourceNotFoundException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertEquals(notFoundException(taskId).getMessage(), failure.getMessage());
    }

    public void testNodeFailureWithFilterMismatchIaeTreatedAsNotFound() {
        FailedNodeException nodeFailure = new FailedNodeException(
            taskId.getNodeId(),
            "node failed",
            new IllegalArgumentException("task [" + taskId + "] doesn't support this operation")
        );
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        final ResourceNotFoundException failure = safeAwaitFailure(
            ResourceNotFoundException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertEquals(notFoundException(taskId).getMessage(), failure.getMessage());
    }

    public void testNodeFailureWithNonCancellableIaeTreatedAsNotFound() {
        // The other IAE message processTasks emits: target exists but isn't CancellableTask.
        FailedNodeException nodeFailure = new FailedNodeException(
            taskId.getNodeId(),
            "node failed",
            new IllegalArgumentException("task [" + taskId + "] doesn't support cancellation")
        );
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        final ResourceNotFoundException failure = safeAwaitFailure(
            ResourceNotFoundException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertEquals(notFoundException(taskId).getMessage(), failure.getMessage());
    }

    public void testNodeFailureWithUnrelatedIaePropagated() {
        // An IllegalArgumentException whose message isn't one of the two strings processTasks throws should not be silently mapped to 404.
        IllegalArgumentException iae = new IllegalArgumentException("something else entirely");
        FailedNodeException nodeFailure = new FailedNodeException(taskId.getNodeId(), "node failed", iae);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        final Exception failure = safeAwaitFailure(
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertSame("unrelated IAE must surface, not be masked as not-found", nodeFailure, failure);
    }

    public void testNonValidationNodeFailurePropagated() {
        // A real transport-level failure (e.g. node disconnected) must not be silently mapped to 404.
        RuntimeException underlying = new RuntimeException("connection refused");
        FailedNodeException nodeFailure = new FailedNodeException(taskId.getNodeId(), "node failed", underlying);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(), List.of(nodeFailure)));

        final FailedNodeException failure = safeAwaitFailure(
            FailedNodeException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertSame("real node failures must surface, not be swallowed as not-found", underlying, failure.getCause());
    }

    public void testTaskFailurePropagatedAsServiceUnavailable() {
        ElasticsearchStatusException relocating = new ElasticsearchStatusException(
            "cannot cancel task [" + taskId.getId() + "] because it is being relocated",
            RestStatus.SERVICE_UNAVAILABLE
        );
        TaskOperationFailure taskFailure = new TaskOperationFailure(taskId.getNodeId(), taskId.getId(), relocating);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(taskFailure), List.of()));

        final ElasticsearchStatusException failure = safeAwaitFailure(
            ElasticsearchStatusException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertSame("cause is rethrown directly", relocating, failure);
    }

    public void testValidationNodeFailureDroppedWhenMixedWithRealFailure() {
        FailedNodeException validation = new FailedNodeException(
            "some-other-node",
            "Failed node [some-other-node]",
            new ResourceNotFoundException("task [{}] is not found", taskId)
        );
        ElasticsearchStatusException relocating = new ElasticsearchStatusException("being relocated", RestStatus.SERVICE_UNAVAILABLE);
        TaskOperationFailure taskFailure = new TaskOperationFailure(taskId.getNodeId(), taskId.getId(), relocating);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(taskFailure), List.of(validation)));

        final ElasticsearchStatusException failure = safeAwaitFailure(
            ElasticsearchStatusException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertSame("real task failure surfaces, validation noise is dropped", relocating, failure);
        assertEquals("validation node failure is not attached as suppressed", 0, failure.getSuppressed().length);
    }

    public void testMultipleFailuresAttachedAsSuppressed() {
        // If cancel-tasks ever fans out and returns more than one failure, we want all of them visible — the head determines the surfaced
        // status (and message), the rest hang off via getSuppressed() so they don't get silently dropped.
        FailedNodeException nodeFailure = new FailedNodeException(taskId.getNodeId(), "node failed", new RuntimeException("transport"));
        ElasticsearchStatusException relocating = new ElasticsearchStatusException("being relocated", RestStatus.SERVICE_UNAVAILABLE);
        TaskOperationFailure taskFailure = new TaskOperationFailure(taskId.getNodeId(), taskId.getId(), relocating);
        mockCancelTasks(new ListTasksResponse(List.of(), List.of(taskFailure), List.of(nodeFailure)));

        // Node failures come first in the response model, so they're the head.
        final FailedNodeException failure = safeAwaitFailure(
            FailedNodeException.class,
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, randomBoolean()), l)
        );
        assertSame(nodeFailure, failure);
        assertThat(failure.getSuppressed(), arrayContaining(relocating));
    }

    public void testGetReindexFailurePropagated() {
        mockCancelTasks(new ListTasksResponse(List.of(taskInfo(taskId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID)), List.of(), List.of()));

        RuntimeException getFailure = new RuntimeException("boom");
        doAnswer((InvocationOnMock inv) -> {
            ActionListener<GetReindexResponse> l = inv.getArgument(2);
            l.onFailure(getFailure);
            return null;
        }).when(client).execute(eq(TransportGetReindexAction.TYPE), any(), any());

        final Exception failure = safeAwaitFailure(
            CancelReindexResponse.class,
            l -> action.doExecute(mock(), new CancelReindexRequest(taskId, true), l)
        );
        assertSame(getFailure, failure);
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
