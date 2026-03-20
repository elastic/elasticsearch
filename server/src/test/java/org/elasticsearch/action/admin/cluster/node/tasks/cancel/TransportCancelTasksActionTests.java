/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.action.admin.cluster.node.tasks.TransportGetTaskActionTests;
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
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

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
     * Verifies that cancelling all tasks (including a reindex task) via the task management Cancel API triggers a deprecation warning.
     * Callers should use the dedicated reindex API ({@code POST /_reindex/<task_id>/_cancel}) for reindex tasks instead.
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
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

        TransportCancelTasksAction cancelTasksAction = new TransportCancelTasksAction(
            clusterService,
            transportService,
            actionFilters,
            featureService
        );

        // Register a cancellable reindex task
        taskManager.register("reindex", ReindexAction.NAME, new ReindexRequest());

        // Don't specify a specific task to cancel on the request so it attempts to cancel them all
        CancelTasksRequest request = new CancelTasksRequest();
        cancelTasksAction.processTasks(request);
        assertWarnings(
            "Using the task management APIs to cancel reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, POST /_reindex/<task_id>/_cancel."
        );
    }

    /**
     * When the cluster does not report {@link ReindexTaskManagementFeatures#REINDEX_PIT_SEARCH_FEATURE}, no deprecation is logged
     * for task-management cancel of reindex tasks.
     */
    public void testCancellingReindexingTaskDoesNotWarnWhenPitSearchFeatureUnsupported() {
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
        when(featureService.clusterHasFeature(any(), eq(ReindexTaskManagementFeatures.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(false);

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
}
