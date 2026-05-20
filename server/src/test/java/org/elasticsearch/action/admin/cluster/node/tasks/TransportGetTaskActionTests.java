/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
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
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
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

    public void testGetTaskFollowsReindexRelocation() throws Exception {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId(nodeId, 100);
        var relocatedTaskId = new TaskId("nodeB", 200);

        long originalStartTime = randomLongBetween(0, 100_000);
        long relocatedStartTime = randomLongBetween(originalStartTime + 1, originalStartTime + 100_000);
        long relocatedRunningTimeNanos = randomLongBetween(1, TimeUnit.HOURS.toNanos(1));

        TaskResult originalStoredResult = completedTaskResult(
            originalTaskId.toString(),
            ReindexAction.NAME,
            relocatedErrorBytes(relocatedTaskId.toString()),
            originalStartTime,
            0
        );
        TaskResult relocatedResult = runningTaskResult(
            relocatedTaskId.toString(),
            ReindexAction.NAME,
            "relocated reindex task",
            relocatedStartTime,
            relocatedRunningTimeNanos,
            originalTaskId,
            originalStartTime
        );

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        var getTaskAction = buildGetTaskAction(nodeId, new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(getRequest.id(), originalStoredResult));
                } else if (TransportGetTaskAction.TYPE.equals(action)) {
                    ((ActionListener<GetTaskResponse>) listener).onResponse(new GetTaskResponse(relocatedResult));
                } else {
                    fail("Unexpected action: " + action);
                }
            }
        }, taskManager);

        GetTaskResponse response = executeGetTask(taskManager, getTaskAction, originalTaskId);
        TaskResult result = response.getTask();
        TaskInfo info = result.getTask();

        assertThat("should not be completed", result.isCompleted(), equalTo(false));
        assertThat("task ID should be the relocated task", info.taskId(), equalTo(relocatedTaskId));
        assertThat("node should be the relocated node", info.node(), equalTo(relocatedTaskId.getNodeId()));
        assertThat("action should be reindex", info.action(), equalTo(ReindexAction.NAME));
        assertThat("description should be from the relocated task", info.description(), equalTo("relocated reindex task"));
        assertThat("start time should be from the original task", info.startTime(), equalTo(originalStartTime));
        assertThat("original task ID should reference the original", info.originalTaskId(), equalTo(originalTaskId));
        assertThat("original start time should be from the original task", info.originalStartTimeMillis(), equalTo(originalStartTime));
        long expectedRunningTime = relocatedRunningTimeNanos + TimeUnit.MILLISECONDS.toNanos(relocatedStartTime - originalStartTime);
        assertThat(
            "running time should cover the full duration including relocation gap",
            info.runningTimeNanos(),
            equalTo(expectedRunningTime)
        );
        assertThat("should not be cancelled", info.cancelled(), equalTo(false));
        assertThat("no error on running task", result.getError(), nullValue());
        assertThat("no response on running task", result.getResponse(), nullValue());
    }

    public void testGetTaskDoesNotFollowRelocationForNonReindexTask() throws Exception {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId(nodeId, 100);

        TaskResult originalStoredResult = completedTaskResult(
            originalTaskId.toString(),
            "some_other_action",
            relocatedErrorBytes("nodeB:200")
        );

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        var getTaskAction = buildGetTaskAction(nodeId, new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(getRequest.id(), originalStoredResult));
                } else if (TransportGetTaskAction.TYPE.equals(action)) {
                    fail("Should not follow relocation for non-reindex tasks");
                } else {
                    fail("Unexpected action: " + action);
                }
            }
        }, taskManager);

        GetTaskResponse response = executeGetTask(taskManager, getTaskAction, originalTaskId);
        TaskResult result = response.getTask();
        assertThat("should return the original task", result.getTask().taskId(), equalTo(originalTaskId));
        assertThat("should be completed", result.isCompleted(), equalTo(true));
        assertThat("action should be the non-reindex action", result.getTask().action(), equalTo("some_other_action"));
        assertThat("error should be preserved (relocation not followed)", result.getError(), notNullValue());
    }

    /**
     * Tests that multi-hop relocation chains (A→B→C) are followed correctly via the {@code doExecute}
     * relocation-aware listener wrapping. The mock client dispatches {@code TransportGetTaskAction.TYPE}
     * through the real action so each hop goes through {@code doExecute} and gets its own wrapping.
     * All task IDs share the same node so routing stays local.
     */
    public void testGetTaskFollowsMultiHopRelocationChain() throws Exception {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId(nodeId, 100);
        var secondTaskId = new TaskId(nodeId, 200);
        var finalTaskId = new TaskId(nodeId, 300);

        long originalStartTime = randomLongBetween(0, 100_000);
        long secondStartTime = randomLongBetween(originalStartTime + 1, originalStartTime + 100_000);
        long finalStartTime = randomLongBetween(secondStartTime + 1, secondStartTime + 100_000);
        long finalRunningTimeNanos = randomLongBetween(1, TimeUnit.HOURS.toNanos(1));

        TaskResult originalStoredResult = completedTaskResult(
            originalTaskId.toString(),
            ReindexAction.NAME,
            relocatedErrorBytes(secondTaskId.toString()),
            originalStartTime,
            0
        );
        TaskResult secondStoredResult = completedTaskResult(
            secondTaskId.toString(),
            ReindexAction.NAME,
            "intermediate reindex task",
            relocatedErrorBytes(finalTaskId.toString()),
            secondStartTime,
            0,
            originalTaskId,
            originalStartTime
        );
        TaskResult finalResult = runningTaskResult(
            finalTaskId.toString(),
            ReindexAction.NAME,
            "final reindex task",
            finalStartTime,
            finalRunningTimeNanos,
            originalTaskId,
            originalStartTime
        );

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        AtomicReference<TransportGetTaskAction> actionRef = new AtomicReference<>();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    String docId = getRequest.id();
                    if (docId.equals(originalTaskId.toString())) {
                        ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(docId, originalStoredResult));
                    } else if (docId.equals(secondTaskId.toString())) {
                        ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(docId, secondStoredResult));
                    } else if (docId.equals(finalTaskId.toString())) {
                        ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(docId, finalResult));
                    } else {
                        listener.onFailure(new ResourceNotFoundException("task not found in .tasks: " + docId));
                    }
                } else if (TransportGetTaskAction.TYPE.equals(action)) {
                    taskManager.registerAndExecute(
                        "transport",
                        actionRef.get(),
                        (GetTaskRequest) request,
                        null,
                        (ActionListener<GetTaskResponse>) listener
                    );
                } else {
                    fail("Unexpected action: " + action);
                }
            }
        };

        var getTaskAction = buildGetTaskAction(nodeId, client, taskManager);
        actionRef.set(getTaskAction);

        GetTaskResponse response = executeGetTask(taskManager, getTaskAction, originalTaskId);
        TaskResult result = response.getTask();
        TaskInfo info = result.getTask();
        assertThat("task ID should be the final task in the chain", info.taskId(), equalTo(finalTaskId));
        assertThat("final task should be running", result.isCompleted(), equalTo(false));
        assertThat("action should be reindex", info.action(), equalTo(ReindexAction.NAME));
        assertThat("start time should be from the original task", info.startTime(), equalTo(originalStartTime));
        assertThat("original task ID should reference the first task in the chain", info.originalTaskId(), equalTo(originalTaskId));
        assertThat(
            "original start time should be from the first task in the chain",
            info.originalStartTimeMillis(),
            equalTo(originalStartTime)
        );
        long expectedRunningTime = finalRunningTimeNanos + TimeUnit.MILLISECONDS.toNanos(finalStartTime - originalStartTime);
        assertThat("running time should cover full chain", info.runningTimeNanos(), equalTo(expectedRunningTime));
        assertThat("no error on running task", result.getError(), nullValue());
        assertThat("no response on running task", result.getResponse(), nullValue());
    }

    public void testGetTaskDoesNotFollowRelocationWhenDisabled() throws Exception {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId(nodeId, 100);
        var relocatedTaskId = new TaskId("nodeB", 200);

        TaskResult originalStoredResult = completedTaskResult(
            originalTaskId.toString(),
            ReindexAction.NAME,
            relocatedErrorBytes(relocatedTaskId.toString())
        );

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        var getTaskAction = buildGetTaskAction(nodeId, new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(getRequest.id(), originalStoredResult));
                } else if (TransportGetTaskAction.TYPE.equals(action)) {
                    fail("Should not follow relocation when follow_relocations=false");
                } else {
                    fail("Unexpected action: " + action);
                }
            }
        }, taskManager);

        var future = new TestPlainActionFuture<GetTaskResponse>();
        var request = new GetTaskRequest().setTaskId(originalTaskId).setFollowRelocations(false);
        taskManager.registerAndExecute("transport", getTaskAction, request, null, future);
        GetTaskResponse response = future.get(10, TimeUnit.SECONDS);

        TaskResult result = response.getTask();
        assertThat("should return the original task", result.getTask().taskId(), equalTo(originalTaskId));
        assertThat("should be completed", result.isCompleted(), equalTo(true));
        assertThat("action should be reindex", result.getTask().action(), equalTo(ReindexAction.NAME));
        assertThat("error should be preserved (relocation not followed)", result.getError(), notNullValue());
    }

    public void testGetTaskSurfacesFailureWhenRelocatedTaskNotFound() {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId(nodeId, 100);
        var relocatedTaskId = new TaskId("nodeB", 200);

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        var getTaskAction = buildGetTaskAction(
            nodeId,
            buildClientThatFailsRelocationLookup(originalTaskId, relocatedTaskId, new ResourceNotFoundException("task not found")),
            taskManager
        );

        var thrown = expectGetTaskFailure(taskManager, getTaskAction, originalTaskId);
        var rnfe = ExceptionsHelper.unwrap(thrown, ResourceNotFoundException.class);
        assertThat("relocation lookup failure should be surfaced", rnfe, notNullValue());
        assertThat(
            "failure metadata should pinpoint the task we were following relocation from",
            ((ResourceNotFoundException) rnfe).getMetadata("es.following_relocation_from"),
            equalTo(List.of(originalTaskId.toString()))
        );
    }

    public void testGetTaskSurfacesNonElasticsearchFailure() {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId(nodeId, 100);
        var relocatedTaskId = new TaskId("nodeB", 200);

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        var getTaskAction = buildGetTaskAction(
            nodeId,
            buildClientThatFailsRelocationLookup(originalTaskId, relocatedTaskId, new IllegalStateException("non-Elasticsearch failure")),
            taskManager
        );

        var thrown = expectGetTaskFailure(taskManager, getTaskAction, originalTaskId);
        assertThat(
            "non-ES relocation lookup failure should be surfaced unchanged",
            ExceptionsHelper.unwrap(thrown, IllegalStateException.class),
            notNullValue()
        );
    }

    private NodeClient buildClientThatFailsRelocationLookup(
        TaskId originalTaskId,
        TaskId relocatedTaskId,
        Exception relocationLookupFailure
    ) {
        TaskResult originalStoredResult = completedTaskResult(
            originalTaskId.toString(),
            ReindexAction.NAME,
            relocatedErrorBytes(relocatedTaskId.toString())
        );
        return new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(getRequest.id(), originalStoredResult));
                } else if (TransportGetTaskAction.TYPE.equals(action)) {
                    listener.onFailure(relocationLookupFailure);
                } else {
                    fail("Unexpected action: " + action);
                }
            }
        };
    }

    private Exception expectGetTaskFailure(TaskManager taskManager, TransportGetTaskAction action, TaskId taskId) {
        var future = new TestPlainActionFuture<GetTaskResponse>();
        taskManager.registerAndExecute("transport", action, new GetTaskRequest().setTaskId(taskId), null, future);
        return expectThrows(ExecutionException.class, future::get);
    }

    public void testGetCompletedRelocatedTaskDirectlyReturnsOwnStartTime() throws Exception {
        var nodeId = "nodeA";
        var originalTaskId = new TaskId("nodeB", 100);
        var relocatedTaskId = new TaskId(nodeId, 200);

        long originalStartTime = randomLongBetween(0, 100_000);
        long relocatedStartTime = randomLongBetween(originalStartTime + 1, originalStartTime + 100_000);
        long relocatedRunningTimeNanos = randomLongBetween(1, TimeUnit.HOURS.toNanos(1));

        TaskResult relocatedStoredResult = new TaskResult(
            true,
            taskInfo(
                relocatedTaskId.toString(),
                ReindexAction.NAME,
                "completed relocated reindex",
                relocatedStartTime,
                relocatedRunningTimeNanos,
                originalTaskId,
                originalStartTime
            )
        );

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        var getTaskAction = buildGetTaskAction(nodeId, new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    ((ActionListener<GetResponse>) listener).onResponse(buildTasksIndexResponse(getRequest.id(), relocatedStoredResult));
                } else {
                    fail("Unexpected action: " + action);
                }
            }
        }, taskManager);

        GetTaskResponse response = executeGetTask(taskManager, getTaskAction, relocatedTaskId);
        TaskResult result = response.getTask();
        TaskInfo info = result.getTask();

        assertThat("should be completed", result.isCompleted(), equalTo(true));
        assertThat("task ID should be the relocated task itself", info.taskId(), equalTo(relocatedTaskId));
        assertThat("start_time should be the relocated task's own", info.startTime(), equalTo(relocatedStartTime));
        assertThat("running time should be the relocated task's own", info.runningTimeNanos(), equalTo(relocatedRunningTimeNanos));
        assertThat("original_task_id should reference the original", info.originalTaskId(), equalTo(originalTaskId));
        assertThat("original_start_time should be from the original task", info.originalStartTimeMillis(), equalTo(originalStartTime));
        assertThat("no error on successfully completed task", result.getError(), nullValue());
    }

    // --- Helpers for multi-project tests ---

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

    // --- Helpers for relocation tests ---

    private TransportGetTaskAction buildGetTaskAction(String nodeId, NodeClient client, TaskManager taskManager) {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(transportService.getTaskManager()).thenReturn(taskManager);
        return new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            client,
            NamedXContentRegistry.EMPTY,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
    }

    private GetTaskResponse executeGetTask(TaskManager taskManager, TransportGetTaskAction action, TaskId taskId) throws Exception {
        var future = new TestPlainActionFuture<GetTaskResponse>();
        taskManager.registerAndExecute("transport", action, new GetTaskRequest().setTaskId(taskId), null, future);
        return future.get(10, TimeUnit.SECONDS);
    }

    private static GetResponse buildTasksIndexResponse(String docId, TaskResult taskResult) {
        BytesReference source;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            taskResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError("failed to serialize task result", e);
        }
        return new GetResponse(
            new GetResult(
                TaskResultsService.TASK_INDEX,
                docId,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                1,
                true,
                source,
                null,
                null
            )
        );
    }

    private static TaskResult completedTaskResult(String taskIdStr, String action, BytesReference errorBytes) {
        return completedTaskResult(taskIdStr, action, errorBytes, System.currentTimeMillis(), 0);
    }

    private static TaskResult completedTaskResult(
        String taskIdStr,
        String action,
        BytesReference errorBytes,
        long startTimeMillis,
        long runningTimeNanos
    ) {
        return new TaskResult(true, taskInfo(taskIdStr, action, startTimeMillis, runningTimeNanos), errorBytes, null);
    }

    private static TaskResult completedTaskResult(
        String taskIdStr,
        String action,
        String description,
        BytesReference errorBytes,
        long startTimeMillis,
        long runningTimeNanos,
        TaskId originalTaskId,
        long originalStartTimeMillis
    ) {
        return new TaskResult(
            true,
            taskInfo(taskIdStr, action, description, startTimeMillis, runningTimeNanos, originalTaskId, originalStartTimeMillis),
            errorBytes,
            null
        );
    }

    private static TaskResult runningTaskResult(String taskIdStr, String action) {
        return runningTaskResult(taskIdStr, action, System.currentTimeMillis(), 0);
    }

    private static TaskResult runningTaskResult(String taskIdStr, String action, long startTimeMillis, long runningTimeNanos) {
        return new TaskResult(false, taskInfo(taskIdStr, action, startTimeMillis, runningTimeNanos));
    }

    private static TaskResult runningTaskResult(
        String taskIdStr,
        String action,
        String description,
        long startTimeMillis,
        long runningTimeNanos,
        TaskId originalTaskId,
        long originalStartTimeMillis
    ) {
        return new TaskResult(
            false,
            taskInfo(taskIdStr, action, description, startTimeMillis, runningTimeNanos, originalTaskId, originalStartTimeMillis)
        );
    }

    private static TaskInfo taskInfo(String taskIdStr, String action) {
        return taskInfo(taskIdStr, action, System.currentTimeMillis(), 0);
    }

    private static TaskInfo taskInfo(String taskIdStr, String action, long startTimeMillis, long runningTimeNanos) {
        TaskId taskId = new TaskId(taskIdStr);
        return new TaskInfo(
            taskId,
            "transport",
            taskId.getNodeId(),
            action,
            "test task",
            null,
            startTimeMillis,
            runningTimeNanos,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
    }

    private static TaskInfo taskInfo(
        String taskIdStr,
        String action,
        String description,
        long startTimeMillis,
        long runningTimeNanos,
        TaskId originalTaskId,
        long originalStartTimeMillis
    ) {
        TaskId taskId = new TaskId(taskIdStr);
        return new TaskInfo(
            taskId,
            "transport",
            taskId.getNodeId(),
            action,
            description,
            null,
            startTimeMillis,
            runningTimeNanos,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            originalTaskId,
            originalStartTimeMillis
        );
    }

    private static BytesReference relocatedErrorBytes(String relocatedTaskId) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            builder.field("type", "task_relocated_exception");
            builder.field("reason", "Task was relocated");
            builder.field("relocated_task_id", relocatedTaskId);
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError("failed to create relocation error bytes", e);
        }
    }
}
