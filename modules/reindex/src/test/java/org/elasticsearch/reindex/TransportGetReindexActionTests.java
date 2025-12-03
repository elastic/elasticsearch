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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.reindex.TransportGetReindexAction.notFoundException;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportGetReindexActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("TransportGetReindexActionTests");
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    private Client getAssertingClient(
        TaskId taskId,
        boolean waitForCompletion,
        TimeValue timeout,
        @Nullable GetTaskResponse toRespond,
        @Nullable Exception toThrow
    ) {
        return new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertEquals(TransportGetTaskAction.TYPE, action);
                assertThat(request, instanceOf(GetTaskRequest.class));
                GetTaskRequest getTaskRequest = (GetTaskRequest) request;
                assertEquals(taskId, getTaskRequest.getTaskId());
                assertEquals(timeout, getTaskRequest.getTimeout());
                assertEquals(waitForCompletion, getTaskRequest.getWaitForCompletion());

                if (toThrow != null) {
                    listener.onFailure(toThrow);
                } else if (toRespond != null) {
                    listener.onResponse((Response) toRespond);
                } else {
                    fail("Either toRespond or toThrow must be provided");
                }
            }
        };
    }

    public void testReindexTaskFound() {
        TaskInfo reindexTask = new TaskInfo(
            new TaskId("node0", 123),
            "test",
            "node0",
            ReindexAction.NAME,
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        TaskResult taskResult = new TaskResult(true, reindexTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);

        TaskId taskId = new TaskId("node1", 123);
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(taskId);
        request.setWaitForCompletion(true);
        request.setTimeout(TimeValue.timeValueSeconds(30));

        TransportGetReindexAction action = new TransportGetReindexAction(
            mock(),
            mock(),
            getAssertingClient(taskId, getTaskResponse, null)
        );
        ActionListener<GetReindexResponse> listener = mock();

        action.doExecute(mock(Task.class), request, listener);

        GetReindexResponse expectedResponse = new GetReindexResponse(taskResult);
        verify(listener).onResponse(eq(expectedResponse));
    }

    public void testTaskFoundButNotReindex() {
        TaskInfo otherTask = new TaskInfo(
            new TaskId("node0", 123),
            "test",
            "node0",
            "not reindex action",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        TaskResult taskResult = new TaskResult(true, otherTask);
        GetTaskResponse getTaskResponse = new GetTaskResponse(taskResult);

        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 123));

        TransportGetReindexAction action = new TransportGetReindexAction(
            mock(),
            mock(),
            getAssertingClient(request, getTaskResponse, null)
        );
        ActionListener<GetReindexResponse> listener = mock();
        action.doExecute(mock(Task.class), request, listener);

        ResourceNotFoundException expectedException = notFoundException(request.getTaskId());
        verify(listener).onFailure(
            argThat(e -> e instanceof ResourceNotFoundException && e.getMessage().equals(expectedException.getMessage()))
        );
    }

    public void testGetReindexTaskNotFound() {
        GetReindexRequest request = new GetReindexRequest();
        request.setTaskId(new TaskId("node1", 123));
        request.setWaitForCompletion(true);
        request.setTimeout(TimeValue.timeValueSeconds(30));

        TransportGetReindexAction action = new TransportGetReindexAction(
            mock(),
            mock(),
            getAssertingClient(request, null, new ResourceNotFoundException("task not found"))
        );
        ActionListener<GetReindexResponse> listener = mock();

        action.doExecute(mock(Task.class), request, listener);

        ResourceNotFoundException expectedException = notFoundException(request.getTaskId());
        verify(listener).onFailure(
            argThat(e -> e instanceof ResourceNotFoundException && e.getMessage().equals(expectedException.getMessage()))
        );
    }
}
