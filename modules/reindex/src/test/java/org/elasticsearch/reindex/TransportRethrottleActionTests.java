/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.action.ActionListener.assertOnce;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.ActionListenerUtils.neverCalledListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.theInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportRethrottleActionTests extends ESTestCase {
    private int slices;
    private BulkByPaginatedSearchTask task;
    private Client mockClient;
    private TransportRethrottleAction rethrottleAction;

    @Before
    public void createTask() {
        slices = between(2, 50);
        task = new BulkByPaginatedSearchTask(
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            "test_type",
            "test_action",
            "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            false,
            randomBoolean()
                ? null
                : new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()), randomNonNegativeLong())
        );
        task.setWorkerCount(slices, Float.POSITIVE_INFINITY);
    }

    /**
     * Test rethrottling.
     * @param runningSlices the number of slices still running
     * @param simulator simulate a response from the sub-request to rethrottle the child requests
     * @param verifier verify the resulting response
     */
    private void rethrottleTestCase(
        int runningSlices,
        Consumer<ActionListener<ListTasksResponse>> simulator,
        Consumer<ActionListener<TaskInfo>> verifier
    ) {
        Client client = mock(Client.class);
        String localNodeId = randomAlphaOfLength(5);
        float newRequestsPerSecond = randomValueOtherThanMany(f -> f <= 0, () -> randomFloat());
        @SuppressWarnings("unchecked")
        ActionListener<TaskInfo> listener = mock(ActionListener.class);
        when(listener.delegateFailureAndWrap(any())).thenCallRealMethod();

        TransportRethrottleAction.rethrottle(logger, localNodeId, client, task, newRequestsPerSecond, listener);

        // Capture the sub request and the listener so we can verify they are sane
        ArgumentCaptor<RethrottleRequest> subRequest = ArgumentCaptor.forClass(RethrottleRequest.class);
        @SuppressWarnings({ "unchecked", "rawtypes" }) // Magical generics incantation.....
        ArgumentCaptor<ActionListener<ListTasksResponse>> subListener = ArgumentCaptor.forClass((Class) ActionListener.class);
        if (runningSlices > 0) {
            verify(client).execute(eq(ReindexPlugin.RETHROTTLE_ACTION), subRequest.capture(), subListener.capture());

            assertEquals(new TaskId(localNodeId, task.getId()), subRequest.getValue().getTargetParentTaskId());
            assertEquals(newRequestsPerSecond / runningSlices, subRequest.getValue().getRequestsPerSecond(), 0.00001f);

            simulator.accept(subListener.getValue());
        }
        verifier.accept(listener);
    }

    private Consumer<ActionListener<TaskInfo>> expectSuccessfulRethrottleWithStatuses(
        List<BulkByPaginatedSearchTask.StatusOrException> sliceStatuses
    ) {
        return listener -> {
            TaskInfo taskInfo = captureResponse(TaskInfo.class, listener);
            assertEquals(sliceStatuses, ((BulkByPaginatedSearchTask.Status) taskInfo.status()).getSliceStatuses());
        };
    }

    public void testRethrottleSuccessfulResponse() {
        List<TaskInfo> tasks = new ArrayList<>();
        List<BulkByPaginatedSearchTask.StatusOrException> sliceStatuses = new ArrayList<>(slices);
        for (int i = 0; i < slices; i++) {
            BulkByPaginatedSearchTask.Status status = believeableInProgressStatus(i);
            tasks.add(
                new TaskInfo(
                    new TaskId("test", 123),
                    "test",
                    "test",
                    "test",
                    "test",
                    status,
                    0,
                    0,
                    true,
                    false,
                    new TaskId("test", task.getId()),
                    Collections.emptyMap()
                )
            );
            sliceStatuses.add(new BulkByPaginatedSearchTask.StatusOrException(status));
        }
        rethrottleTestCase(
            slices,
            listener -> listener.onResponse(new ListTasksResponse(tasks, emptyList(), emptyList())),
            expectSuccessfulRethrottleWithStatuses(sliceStatuses)
        );
    }

    public void testRethrottleWithSomeSucceeded() {
        int succeeded = between(1, slices - 1);
        List<BulkByPaginatedSearchTask.StatusOrException> sliceStatuses = new ArrayList<>(slices);
        for (int i = 0; i < succeeded; i++) {
            BulkByPaginatedSearchTask.Status status = believeableCompletedStatus(i);
            task.getLeaderState()
                .onSliceResponse(
                    neverCalledListener(),
                    i,
                    new BulkByScrollResponse(timeValueMillis(10), status, emptyList(), emptyList(), false)
                );
            sliceStatuses.add(new BulkByPaginatedSearchTask.StatusOrException(status));
        }
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = succeeded; i < slices; i++) {
            BulkByPaginatedSearchTask.Status status = believeableInProgressStatus(i);
            tasks.add(
                new TaskInfo(
                    new TaskId("test", 123),
                    "test",
                    "test",
                    "test",
                    "test",
                    status,
                    0,
                    0,
                    true,
                    false,
                    new TaskId("test", task.getId()),
                    Collections.emptyMap()
                )
            );
            sliceStatuses.add(new BulkByPaginatedSearchTask.StatusOrException(status));
        }
        rethrottleTestCase(
            slices - succeeded,
            listener -> listener.onResponse(new ListTasksResponse(tasks, emptyList(), emptyList())),
            expectSuccessfulRethrottleWithStatuses(sliceStatuses)
        );
    }

    public void testRethrottleWithAllSucceeded() {
        List<BulkByPaginatedSearchTask.StatusOrException> sliceStatuses = new ArrayList<>(slices);
        for (int i = 0; i < slices; i++) {
            @SuppressWarnings("unchecked")
            ActionListener<BulkByScrollResponse> listener = i < slices - 1 ? neverCalledListener() : mock(ActionListener.class);
            BulkByPaginatedSearchTask.Status status = believeableCompletedStatus(i);
            task.getLeaderState()
                .onSliceResponse(listener, i, new BulkByScrollResponse(timeValueMillis(10), status, emptyList(), emptyList(), false));
            if (i == slices - 1) {
                // The whole thing succeeded so we should have got the success
                captureResponse(BulkByScrollResponse.class, listener).getStatus();
            }
            sliceStatuses.add(new BulkByPaginatedSearchTask.StatusOrException(status));
        }
        rethrottleTestCase(
            0,
            listener -> { /* There are no async tasks to simulate because the listener is called for us. */},
            expectSuccessfulRethrottleWithStatuses(sliceStatuses)
        );
    }

    private Consumer<ActionListener<TaskInfo>> expectException(Matcher<Exception> exceptionMatcher) {
        return listener -> {
            ArgumentCaptor<Exception> failure = ArgumentCaptor.forClass(Exception.class);
            verify(listener).onFailure(failure.capture());
            assertThat(failure.getValue(), exceptionMatcher);
        };
    }

    public void testRethrottleCatastrophicFailures() {
        Exception e = new Exception();
        rethrottleTestCase(slices, listener -> listener.onFailure(e), expectException(theInstance(e)));
    }

    public void testRethrottleTaskOperationFailure() {
        Exception e = new Exception();
        TaskOperationFailure failure = new TaskOperationFailure("test", 123, e);
        rethrottleTestCase(
            slices,
            listener -> listener.onResponse(new ListTasksResponse(emptyList(), singletonList(failure), emptyList())),
            expectException(hasToString(containsString("Rethrottle of [test:123] failed")))
        );
    }

    public void testRethrottleNodeFailure() {
        FailedNodeException e = new FailedNodeException("test", "test", new Exception());
        rethrottleTestCase(
            slices,
            listener -> listener.onResponse(new ListTasksResponse(emptyList(), emptyList(), singletonList(e))),
            expectException(theInstance(e))
        );
    }

    public void testRethrottleParentTask503AfterCapture() {
        task.getLeaderState().captureRequestsPerSecondForRelocation();

        final Client client = mock(Client.class);
        final String localNodeId = randomAlphaOfLength(5);
        final float newRequestsPerSecond = randomFloatBetween(0.1f, 1000f, true);
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        final ActionListener<TaskInfo> listener = ActionTestUtils.assertNoSuccessListener(e -> {
            listenerCalled.set(true);
            assertThat(e, instanceOf(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        });

        TransportRethrottleAction.rethrottle(logger, localNodeId, client, task, newRequestsPerSecond, listener);
        assertTrue("expected onFailure to be called", listenerCalled.get());
    }

    public void testRethrottleChildTask503AfterCapture() {
        final BulkByPaginatedSearchTask workerTask = new BulkByPaginatedSearchTask(
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            false,
            randomBoolean()
                ? null
                : new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()), randomNonNegativeLong())
        );
        final float initialRps = randomFloatBetween(0.1f, 1000f, true);
        workerTask.setWorker(initialRps, null);
        workerTask.getWorkerState().captureRequestsPerSecondForRelocation();

        final Client client = mock(Client.class);
        final String localNodeId = randomAlphaOfLength(5);
        final float newRequestsPerSecond = randomFloatBetween(0.1f, 1000f, true);
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        final ActionListener<TaskInfo> listener = ActionTestUtils.assertNoSuccessListener(e -> {
            listenerCalled.set(true);
            assertThat(e, instanceOf(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        });

        TransportRethrottleAction.rethrottle(logger, localNodeId, client, workerTask, newRequestsPerSecond, listener);
        assertTrue("expected onFailure to be called", listenerCalled.get());
    }

    public void testFollowRelocationFindsRunningTask() {
        rethrottleAction = spy(createActionWithMockClient());

        final TaskId originalTaskId = new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
        final TaskId relocatedTaskId = new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
        final float rps = randomFloatBetween(1f, 1000f, true);

        final RethrottleRequest request = new RethrottleRequest();
        request.setTargetTaskId(originalTaskId);
        request.setRequestsPerSecond(rps);
        request.setFollowRelocations(true);

        final ListTasksResponse emptyResponse = new ListTasksResponse(emptyList(), emptyList(), emptyList());
        mockGetTaskResponse(new TaskResult(false, createTaskInfo(relocatedTaskId)));

        final ArgumentCaptor<RethrottleRequest> retryCaptor = ArgumentCaptor.forClass(RethrottleRequest.class);
        doNothing().when(rethrottleAction).doExecute(any(Task.class), retryCaptor.capture(), any());

        rethrottleAction.followRelocationAndRethrottle(mock(Task.class), request, emptyResponse, neverCalledListener());

        final RethrottleRequest retry = retryCaptor.getValue();
        assertEquals(relocatedTaskId, retry.getTargetTaskId());
        assertEquals(rps, retry.getRequestsPerSecond(), 0.0f);
        assertTrue(retry.followRelocations());

        final ArgumentCaptor<GetTaskRequest> getTaskCaptor = ArgumentCaptor.forClass(GetTaskRequest.class);
        verify(mockClient).execute(eq(TransportGetTaskAction.TYPE), getTaskCaptor.capture(), any());
        assertEquals(originalTaskId, getTaskCaptor.getValue().getTaskId());
    }

    public void testFollowRelocationTaskCompleted() {
        rethrottleAction = spy(createActionWithMockClient());

        final TaskId originalTaskId = new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());

        final RethrottleRequest request = new RethrottleRequest();
        request.setTargetTaskId(originalTaskId);
        request.setRequestsPerSecond(randomFloatBetween(1f, 1000f, true));
        request.setFollowRelocations(true);

        final ListTasksResponse emptyResponse = new ListTasksResponse(emptyList(), emptyList(), emptyList());
        mockGetTaskResponse(new TaskResult(true, createTaskInfo(originalTaskId)));

        final AtomicReference<ListTasksResponse> responseRef = new AtomicReference<>();
        rethrottleAction.followRelocationAndRethrottle(
            mock(Task.class),
            request,
            emptyResponse,
            assertOnce(assertNoFailureListener(responseRef::set))
        );

        assertThat(responseRef.get(), sameInstance(emptyResponse));
        verify(rethrottleAction, atMost(0)).doExecute(any(), any(), any());
    }

    public void testFollowRelocation404ReturnsEmptyResponse() {
        rethrottleAction = spy(createActionWithMockClient());

        final TaskId originalTaskId = new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());

        final RethrottleRequest request = new RethrottleRequest();
        request.setTargetTaskId(originalTaskId);
        request.setRequestsPerSecond(randomFloatBetween(1f, 1000f, true));
        request.setFollowRelocations(true);

        final ListTasksResponse emptyResponse = new ListTasksResponse(emptyList(), emptyList(), emptyList());
        mockGetTaskFailure(new ResourceNotFoundException("task [" + originalTaskId + "] not found"));

        final AtomicReference<ListTasksResponse> responseRef = new AtomicReference<>();
        rethrottleAction.followRelocationAndRethrottle(
            mock(Task.class),
            request,
            emptyResponse,
            assertOnce(assertNoFailureListener(responseRef::set))
        );

        assertThat(responseRef.get(), sameInstance(emptyResponse));
        verify(rethrottleAction, atMost(0)).doExecute(any(), any(), any());
    }

    public void testFollowRelocationPropagatesOtherFailures() {
        rethrottleAction = spy(createActionWithMockClient());

        final TaskId originalTaskId = new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());

        final RethrottleRequest request = new RethrottleRequest();
        request.setTargetTaskId(originalTaskId);
        request.setRequestsPerSecond(randomFloatBetween(1f, 1000f, true));
        request.setFollowRelocations(true);

        final ListTasksResponse emptyResponse = new ListTasksResponse(emptyList(), emptyList(), emptyList());
        final RuntimeException cause = new RuntimeException("connection refused");
        mockGetTaskFailure(cause);

        final AtomicReference<Exception> failureRef = new AtomicReference<>();
        rethrottleAction.followRelocationAndRethrottle(
            mock(Task.class),
            request,
            emptyResponse,
            assertOnce(assertNoSuccessListener(failureRef::set))
        );

        assertThat(failureRef.get(), sameInstance(cause));
        verify(rethrottleAction, atMost(0)).doExecute(any(), any(), any());
    }

    private BulkByPaginatedSearchTask.Status believeableInProgressStatus(Integer sliceId) {
        return new BulkByPaginatedSearchTask.Status(sliceId, 10, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0, null, timeValueMillis(0));
    }

    private BulkByPaginatedSearchTask.Status believeableCompletedStatus(Integer sliceId) {
        return new BulkByPaginatedSearchTask.Status(sliceId, 10, 10, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0, null, timeValueMillis(0));
    }

    private <T> T captureResponse(Class<T> responseClass, ActionListener<T> listener) {
        ArgumentCaptor<Exception> failure = ArgumentCaptor.forClass(Exception.class);
        // Rethrow any failures just so we get a nice exception if there were any. We don't expect any though.
        verify(listener, atMost(1)).onFailure(failure.capture());
        if (false == failure.getAllValues().isEmpty()) {
            throw new AssertionError(failure.getValue());
        }
        ArgumentCaptor<T> response = ArgumentCaptor.forClass(responseClass);
        verify(listener).onResponse(response.capture());
        return response.getValue();
    }

    private TransportRethrottleAction createActionWithMockClient() {
        mockClient = mock(Client.class);
        ThreadPool clientThreadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        TransportService transportService = mock(TransportService.class);
        ThreadPool transportThreadPool = mock(ThreadPool.class);
        when(transportService.getThreadPool()).thenReturn(transportThreadPool);
        when(transportThreadPool.executor(any())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(transportThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));

        return new TransportRethrottleAction(mock(ClusterService.class), transportService, new ActionFilters(Set.of()), mockClient);
    }

    private void mockGetTaskResponse(TaskResult result) {
        doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(2);
            l.onResponse(new GetTaskResponse(result));
            return null;
        }).when(mockClient).execute(eq(TransportGetTaskAction.TYPE), any(GetTaskRequest.class), any());
    }

    private void mockGetTaskFailure(Exception e) {
        doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(2);
            l.onFailure(e);
            return null;
        }).when(mockClient).execute(eq(TransportGetTaskAction.TYPE), any(GetTaskRequest.class), any());
    }

    private TaskInfo createTaskInfo(TaskId taskId) {
        return new TaskInfo(
            taskId,
            "test",
            taskId.getNodeId(),
            "indices:data/write/reindex",
            "test",
            null,
            0,
            0,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
    }

}
