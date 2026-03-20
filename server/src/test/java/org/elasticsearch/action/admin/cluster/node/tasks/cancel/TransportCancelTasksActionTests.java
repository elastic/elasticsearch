/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.TransportGetTaskActionTests;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ReindexTaskManagementFeatures;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCancelTasksActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(TransportGetTaskActionTests.class.getSimpleName());
    }

    @After
    public final void shutdownTestNodes() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    /**
     * Verifies that cancelling a specific reindex task via the task management Cancel API triggers a deprecation warning.
     * Callers should use the dedicated reindex API ({@code POST /_reindex/<task_id>/_cancel}) instead.
     */
    public void testCancellingReindexingTaskThrowsDeprecationWarning() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        // Register a cancellable reindex task
        Task reindexTask = taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());

        CancelTasksRequest request = new CancelTasksRequest();
        // Cancel only the reindexing task above
        request.setTargetTaskId(new TaskId(nodeId, reindexTask.getId()));
        cancelTasksAction.processTasks(request);
        assertWarnings(
            "Using the task management APIs to cancel reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, POST /_reindex/<task_id>/_cancel."
        );
    }

    /**
     * Verifies that cancelling all tasks via the task management Cancel API logs the reindex deprecation at most once when the cluster
     * contains a mix of reindex and non-reindex cancellable tasks (including multiple reindex tasks).
     */
    public void testCancellingAllTasksIncludingReindexingTaskThrowsDeprecationWarning() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());
        taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());

        CancelTasksRequest request = new CancelTasksRequest();
        cancelTasksAction.processTasks(request);
        assertWarnings(
            "Using the task management APIs to cancel reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, POST /_reindex/<task_id>/_cancel."
        );
    }

    /**
     * When the cluster does not report {@link ReindexTaskManagementFeatures#RELOCATE_ON_SHUTDOWN_NODE_FEATURE}, no deprecation is logged
     * for task-management cancel of reindex tasks.
     */
    public void testCancellingReindexingTaskDoesNotWarnWhenRelocateFeatureUnsupported() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(
            false
        );

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        Task reindexTask = taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());
        CancelTasksRequest request = new CancelTasksRequest();
        request.setTargetTaskId(new TaskId(nodeId, reindexTask.getId()));
        cancelTasksAction.processTasks(request);
    }

    /**
     * When cancelling all tasks and the cluster does not report {@link ReindexTaskManagementFeatures#RELOCATE_ON_SHUTDOWN_NODE_FEATURE},
     * no deprecation is logged even when multiple reindex tasks are registered.
     */
    public void testCancellingAllTasksDoesNotWarnWhenFeatureUnsupported() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(
            false
        );

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());
        taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());

        CancelTasksRequest request = new CancelTasksRequest();
        cancelTasksAction.processTasks(request);
    }

    /**
     * Cancelling a non-reindex cancellable task must not log the reindex task-management deprecation
     */
    public void testCancellingNonReindexTaskDoesNotWarn() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        Task listTasksTask = taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        CancelTasksRequest request = new CancelTasksRequest();
        request.setTargetTaskId(new TaskId(nodeId, listTasksTask.getId()));
        cancelTasksAction.processTasks(request);
    }

    /**
     * Cancelling all cancellable tasks when only non-reindex tasks are registered must not log the reindex task-management deprecation.
     */
    public void testCancellingAllNonReindexTasksDoesNotWarn() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());
        taskManager.register("transport", TransportListTasksAction.TYPE.name(), new ListTasksRequest());

        CancelTasksRequest request = new CancelTasksRequest();
        cancelTasksAction.processTasks(request);
    }

    /**
     * Trying to cancel a missing task must not log a deprecation before failing with {@link ResourceNotFoundException}.
     */
    public void testMissingCancellableTaskDoesNotLogDeprecation() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        CancelTasksRequest request = new CancelTasksRequest();
        request.setTargetTaskId(new TaskId(nodeId, Long.MAX_VALUE));
        expectThrows(ResourceNotFoundException.class, () -> cancelTasksAction.processTasks(request));
    }

    /**
     * Trying to cancel a non-cancellable task must not log a deprecation before failing with {@link IllegalArgumentException}.
     */
    public void testCancelNonCancellableTaskDoesNotLogDeprecation() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        Task plainTask = taskManager.register(
            "get",
            TransportGetTaskAction.TYPE.name(),
            new GetTaskRequest().setTaskId(new TaskId(nodeId, 1L))
        );
        CancelTasksRequest request = new CancelTasksRequest();
        request.setTargetTaskId(new TaskId(nodeId, plainTask.getId()));
        expectThrows(IllegalArgumentException.class, () -> cancelTasksAction.processTasks(request));
    }

    /**
     * Cancel-all with no cancellable tasks registered must not log a deprecation.
     */
    public void testCancellingAllTasksWhenNoTasksAreCancellableDoesNotLogDeprecation() {
        var clusterService = mock(ClusterService.class);
        var transportService = mock(TransportService.class);
        var actionFilters = new ActionFilters(emptySet());

        var nodeId = "node1";
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(nodeId));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Task.HEADERS_TO_COPY);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        CancelTasksRequest request = new CancelTasksRequest();
        cancelTasksAction.processTasks(request);
    }
}
