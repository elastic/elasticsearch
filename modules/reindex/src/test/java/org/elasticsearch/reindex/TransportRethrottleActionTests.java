/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.theInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportRethrottleActionTests extends ESTestCase {
    private int slices;
    private BulkByScrollTask task;

    @Before
    public void createTask() {
        slices = between(2, 50);
        task = new BulkByScrollTask(1, "test_type", "test_action", "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        task.setWorkerCount(slices);
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
        List<BulkByScrollTask.StatusOrException> sliceStatuses
    ) {
        return listener -> {
            TaskInfo taskInfo = captureResponse(TaskInfo.class, listener);
            assertEquals(sliceStatuses, ((BulkByScrollTask.Status) taskInfo.status()).getSliceStatuses());
        };
    }

    public void testRethrottleSuccessfulResponse() {
        List<TaskInfo> tasks = new ArrayList<>();
        List<BulkByScrollTask.StatusOrException> sliceStatuses = new ArrayList<>(slices);
        for (int i = 0; i < slices; i++) {
            BulkByScrollTask.Status status = believeableInProgressStatus(i);
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
            sliceStatuses.add(new BulkByScrollTask.StatusOrException(status));
        }
        rethrottleTestCase(
            slices,
            listener -> listener.onResponse(new ListTasksResponse(tasks, emptyList(), emptyList())),
            expectSuccessfulRethrottleWithStatuses(sliceStatuses)
        );
    }

    public void testRethrottleWithSomeSucceeded() {
        int succeeded = between(1, slices - 1);
        List<BulkByScrollTask.StatusOrException> sliceStatuses = new ArrayList<>(slices);
        for (int i = 0; i < succeeded; i++) {
            BulkByScrollTask.Status status = believeableCompletedStatus(i);
            task.getLeaderState()
                .onSliceResponse(neverCalled(), i, new BulkByScrollResponse(timeValueMillis(10), status, emptyList(), emptyList(), false));
            sliceStatuses.add(new BulkByScrollTask.StatusOrException(status));
        }
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = succeeded; i < slices; i++) {
            BulkByScrollTask.Status status = believeableInProgressStatus(i);
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
            sliceStatuses.add(new BulkByScrollTask.StatusOrException(status));
        }
        rethrottleTestCase(
            slices - succeeded,
            listener -> listener.onResponse(new ListTasksResponse(tasks, emptyList(), emptyList())),
            expectSuccessfulRethrottleWithStatuses(sliceStatuses)
        );
    }

    public void testRethrottleWithAllSucceeded() {
        List<BulkByScrollTask.StatusOrException> sliceStatuses = new ArrayList<>(slices);
        for (int i = 0; i < slices; i++) {
            @SuppressWarnings("unchecked")
            ActionListener<BulkByScrollResponse> listener = i < slices - 1 ? neverCalled() : mock(ActionListener.class);
            BulkByScrollTask.Status status = believeableCompletedStatus(i);
            task.getLeaderState()
                .onSliceResponse(listener, i, new BulkByScrollResponse(timeValueMillis(10), status, emptyList(), emptyList(), false));
            if (i == slices - 1) {
                // The whole thing succeeded so we should have got the success
                captureResponse(BulkByScrollResponse.class, listener).getStatus();
            }
            sliceStatuses.add(new BulkByScrollTask.StatusOrException(status));
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

    private BulkByScrollTask.Status believeableInProgressStatus(Integer sliceId) {
        return new BulkByScrollTask.Status(sliceId, 10, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0, null, timeValueMillis(0));
    }

    private BulkByScrollTask.Status believeableCompletedStatus(Integer sliceId) {
        return new BulkByScrollTask.Status(sliceId, 10, 10, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0, null, timeValueMillis(0));
    }

    private <T> ActionListener<T> neverCalled() {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                throw new RuntimeException("Expected no interactions but got [" + response + "]");
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("Expected no interations but was received a failure", e);
            }
        };
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
}
