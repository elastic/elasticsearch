/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetTaskActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ProjectResolver projectResolver;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(TransportGetTaskActionTests.class.getSimpleName());
        projectResolver = TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext());
    }

    @After
    public final void shutdownTestNodes() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testGetTaskActionWithMultiProjectEnabled() {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var nodeId = "node1";
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    var getResponse = new GetResponse(
                        new GetResult(
                            TaskResultsService.TASK_INDEX,
                            getRequest.id(),
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                            1,
                            false,
                            null,
                            null,
                            null
                        )
                    );
                    ((ActionListener<GetResponse>) listener).onResponse(getResponse);
                } else {
                    fail(new AssertionError("Unexpected call"));
                }
            }
        };
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        var project1 = randomUniqueProjectId();
        var project2 = randomUniqueProjectId();
        var project1Task = registerRandomTaskWithProjectId(taskManager, project1);
        var project2Task = registerRandomTaskWithProjectId(taskManager, project2);
        var taskWithNoProjectIdHeader = registerRandomTaskWithProjectId(taskManager, null);
        {
            var taskId = new TaskId(nodeId, project1Task.getId());
            var result = executeGetTaskWithProjectId(taskManager, getTaskAction, taskId, project1);
            assertNull(result.exception);
            assertNotNull(result.response);
            assertThat(result.response.getTask().getTask().taskId(), equalTo(taskId));
        }
        {
            var taskId = new TaskId(nodeId, project2Task.getId());
            var result = executeGetTaskWithProjectId(taskManager, getTaskAction, taskId, project2);
            assertNull(result.exception);
            assertNotNull(result.response);
            assertThat(result.response.getTask().getTask().taskId(), equalTo(taskId));
        }
        {
            var taskToGet = randomFrom(project1Task, taskWithNoProjectIdHeader);
            var result = executeGetTaskWithProjectId(taskManager, getTaskAction, new TaskId(nodeId, taskToGet.getId()), project2);
            assertNull(result.response);
            assertNotNull(result.exception);
            var exception = ExceptionsHelper.unwrap(result.exception, ResourceNotFoundException.class);
            assertNotNull(result.exception.toString(), exception);
        }
        {
            var taskToGet = randomFrom(project2Task, taskWithNoProjectIdHeader);
            var result = executeGetTaskWithProjectId(taskManager, getTaskAction, new TaskId(nodeId, taskToGet.getId()), project1);
            assertNull(result.response);
            assertNotNull(result.exception);
            var exception = ExceptionsHelper.unwrap(result.exception, ResourceNotFoundException.class);
            assertNotNull(result.exception.toString(), exception);
        }
        {
            var result = executeGetTaskWithProjectId(
                taskManager,
                getTaskAction,
                new TaskId(nodeId, taskWithNoProjectIdHeader.getId()),
                randomFrom(project1, project2)
            );
            assertNull(result.response);
            assertNotNull(result.exception);
            var exception = ExceptionsHelper.unwrap(result.exception, ResourceNotFoundException.class);
            assertNotNull(result.exception.toString(), exception);
        }
    }

    private Task registerRandomTaskWithProjectId(TaskManager taskManager, ProjectId projectId) {
        if (projectId == null) {
            try (var ignore = threadPool.getThreadContext().newStoredContext()) {
                return taskManager.register("task", "action", new BulkRequest());
            }
        }
        AtomicReference<Task> task = new AtomicReference<>();
        projectResolver.executeOnProject(projectId, () -> task.set(taskManager.register("task", "action", new BulkRequest())));
        return task.get();
    }

    record GetTaskResult(GetTaskResponse response, Exception exception) {}

    private GetTaskResult executeGetTaskWithProjectId(
        TaskManager taskManager,
        TransportGetTaskAction getTaskAction,
        TaskId taskId,
        ProjectId projectId
    ) {
        var future = new TestPlainActionFuture<GetTaskResponse>();
        projectResolver.executeOnProject(
            projectId,
            () -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future)
        );
        try {
            var resp = future.get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            return new GetTaskResult(resp, null);
        } catch (Exception e) {
            return new GetTaskResult(null, e);
        }
    }
}
