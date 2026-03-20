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
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexTaskManagementFeatures;
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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    /**
     * Verifies that using the task management Get API to retrieve a reindex task triggers a deprecation warning.
     * Callers should use the dedicated reindex API ({@code GET /_reindex/<task_id>}) instead.
     */
    public void testDeprecationWarningForReindexGetTask() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var client = mock(NodeClient.class);

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        // Register a reindexing task to be retrieved
        Task runningTask = taskManager.register("task", ReindexAction.NAME, new GetTaskRequest());
        TaskId taskId = new TaskId(nodeId, runningTask.getId());

        // Calls the getTask API with a request to find the reindexing task above. We expect the deprecation logger to be invoked
        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
        assertWarnings(
            "Using the task management APIs to get reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, GET /_reindex/<task_id>."
        );
    }

    /**
     * When the cluster does not report the reindex task-management feature, GET on a running reindex task must not log a deprecation.
     */
    public void testNoDeprecationWarningForReindexGetTaskWhenFeatureUnsupported() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var client = mock(NodeClient.class);

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(
            false
        );

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        Task runningTask = taskManager.register("task", ReindexAction.NAME, new GetTaskRequest());
        TaskId taskId = new TaskId(nodeId, runningTask.getId());

        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
    }

    /**
     * GET on a non-reindex running task must not log the reindex deprecation even when the cluster reports the feature as supported.
     */
    public void testNoDeprecationWarningForNonReindexGetTask() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var client = mock(NodeClient.class);

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        Task runningTask = taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        TaskId taskId = new TaskId(nodeId, runningTask.getId());

        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
    }

    /**
     * A running task with a null action must not trigger the reindex GET deprecation logger
     */
    public void testNoDeprecationWhenRunningTaskHasNullAction() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var client = mock(NodeClient.class);

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        Task runningTask = taskManager.register("task", null, new GetTaskRequest());
        TaskId taskId = new TaskId(nodeId, runningTask.getId());

        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
    }

    /**
     * When the task is not running, GET loads a completed task from the tasks index; a stored reindex task must emit the deprecation
     * when the cluster supports the feature.
     */
    public void testDeprecationWarningWhenLoadingFinishedReindexTaskFromIndex() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var nodeId = "node1";
        TaskId taskId = new TaskId(nodeId, 42L);
        TaskInfo storedInfo = new TaskInfo(
            taskId,
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        BytesReference source = buildCompletedTaskResultSource(storedInfo);

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool, projectResolver) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    var getRequest = (GetRequest) request;
                    assertEquals(TaskResultsService.TASK_INDEX, getRequest.index());
                    assertEquals(taskId.toString(), getRequest.id());
                    ((ActionListener<GetResponse>) listener).onResponse(getResponseForStoredTask(taskId, source));
                } else {
                    fail("unexpected action " + action);
                }
            }
        };

        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
        assertWarnings(
            "Using the task management APIs to get reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, GET /_reindex/<task_id>."
        );
    }

    /**
     * Loading a finished reindex task from the tasks index must not log when the cluster does not report the feature.
     */
    public void testNoDeprecationWhenLoadingFinishedReindexTaskFromIndex() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var nodeId = "node1";
        TaskId taskId = new TaskId(nodeId, 43L);
        TaskInfo storedInfo = new TaskInfo(
            taskId,
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        BytesReference source = buildCompletedTaskResultSource(storedInfo);

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool, projectResolver) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    ((ActionListener<GetResponse>) listener).onResponse(getResponseForStoredTask(taskId, source));
                } else {
                    fail("unexpected action " + action);
                }
            }
        };

        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(
            false
        );

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
    }

    /**
     * Loading a finished non-reindex task from the tasks index must not log the reindex deprecation when the feature is supported.
     */
    public void testNoDeprecationWhenLoadingFinishedNonReindexTaskFromIndex() throws Exception {
        var transportService = mock(TransportService.class);
        var clusterService = mock(ClusterService.class);
        var nodeId = "node1";
        TaskId taskId = new TaskId(nodeId, 44L);
        TaskInfo storedInfo = new TaskInfo(
            taskId,
            "transport",
            nodeId,
            TransportListTasksAction.TYPE.name(),
            null,
            null,
            0L,
            0L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        BytesReference source = buildCompletedTaskResultSource(storedInfo);

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool, projectResolver) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportGetAction.TYPE.equals(action)) {
                    ((ActionListener<GetResponse>) listener).onResponse(getResponseForStoredTask(taskId, source));
                } else {
                    fail("unexpected action " + action);
                }
            }
        };

        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
            client,
            NamedXContentRegistry.EMPTY,
            projectResolver
        );

        var future = new TestPlainActionFuture<GetTaskResponse>();
        assertBusy(() -> taskManager.registerAndExecute("transport", getTaskAction, new GetTaskRequest().setTaskId(taskId), null, future));
    }

    private static BytesReference buildCompletedTaskResultSource(TaskInfo taskInfo) throws IOException {
        TaskResult stored = new TaskResult(true, taskInfo);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            stored.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder);
        }
    }

    private static GetResponse getResponseForStoredTask(TaskId taskId, BytesReference source) {
        return new GetResponse(
            new GetResult(
                TaskResultsService.TASK_INDEX,
                taskId.toString(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                1L,
                true,
                source,
                null,
                null
            )
        );
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
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);

        TransportGetTaskAction getTaskAction = new TransportGetTaskAction(
            threadPool,
            transportService,
            new ActionFilters(emptySet()),
            clusterService,
            featureService,
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
