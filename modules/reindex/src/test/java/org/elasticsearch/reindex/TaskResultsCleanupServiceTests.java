/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.multiproject.TestOnlyMultiProjectResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;

public class TaskResultsCleanupServiceTests extends ESTestCase {

    private static final ProjectId P1 = ProjectId.fromId("p1");
    private static final ProjectId P2 = ProjectId.fromId("p2");
    private static final ProjectId P3 = ProjectId.fromId("p3");
    private static final ProjectId P4 = ProjectId.fromId("p4");

    private TestThreadPool threadPool;
    private String localNodeId;
    private ProjectResolver projectResolver;
    private TaskResultsCleanupService service;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
        localNodeId = randomIdentifier();
        projectResolver = new TestOnlyMultiProjectResolver(() -> threadPool);
    }

    @After
    public void stopService() {
        if (service != null) {
            service.stop();
            service.close();
        }
        terminate(threadPool);
    }

    /**
     * Every round of the cleanup must run each project's delete-by-query in that project's context. The next round is scheduled from the
     * completion of the previous one, which happens in the context of the last project, so a next round that inherited that context would
     * fail in {@link ProjectResolver#executeOnProject} because a project is already set.
     */
    public void testEveryRoundCleansUpEachLocalProject() throws Exception {
        // Complete from another thread that inherits the project context of the request, as the response of a real delete-by-query does.
        final Queue<ProjectId> deleteByQueryProjects = new ConcurrentLinkedQueue<>();
        final Client client = mockDeleteByQueryClient(
            projectResolver,
            deleteByQueryProjects,
            listener -> threadPool.generic().execute(() -> listener.onResponse(emptyDeleteByQueryResponse()))
        );
        service = createService(projectResolver, localNodeId, threadPool, client);
        service.start();

        // p1 and p4 have the primary of .tasks on the local node, p2 on another node and p3 has no .tasks index
        final ClusterState state = clusterState(Map.of(P1, localNodeId, P2, randomIdentifier(), P4, localNodeId), P3);
        service.clusterChanged(new ClusterChangedEvent(getTestName(), state, ClusterState.EMPTY_STATE));

        assertBusy(() -> {
            assertThat(Collections.frequency(deleteByQueryProjects, P1), greaterThanOrEqualTo(2));
            assertThat(Collections.frequency(deleteByQueryProjects, P4), greaterThanOrEqualTo(2));
        });
        assertThat(Set.copyOf(deleteByQueryProjects), equalTo(Set.of(P1, P4)));
    }

    /**
     * Stopping and starting the cleanup again while a delete-by-query is still running must not leave two chains of cleanups: the
     * completion of the delete-by-query of the stopped chain must not schedule another cleanup. Everything runs on the test thread, and the
     * scheduled cleanups are collected rather than run, so that the test is deterministic.
     */
    public void testStoppedChainDoesNotScheduleNextCleanupAfterRestart() {
        final ThreadPool threadPoolSpy = Mockito.spy(threadPool);
        Mockito.doReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE).when(threadPoolSpy).generic();
        final List<Runnable> scheduledCleanups = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            scheduledCleanups.add(invocation.getArgument(0));
            return Mockito.mock(Scheduler.ScheduledCancellable.class);
        }).when(threadPoolSpy).schedule(any(Runnable.class), any(TimeValue.class), any(Executor.class));

        // the first delete-by-query is held back, the following ones complete immediately
        final AtomicBoolean blockDeleteCall = new AtomicBoolean(true);
        final List<ActionListener<BulkByPaginatedSearchResponse>> heldListeners = new ArrayList<>();
        final List<ProjectId> deleteByQueryProjects = new ArrayList<>();
        final Client client = mockDeleteByQueryClient(projectResolver, deleteByQueryProjects, listener -> {
            if (blockDeleteCall.compareAndSet(true, false)) {
                heldListeners.add(listener);
            } else {
                listener.onResponse(emptyDeleteByQueryResponse());
            }
        });
        service = createService(projectResolver, localNodeId, threadPoolSpy, client);
        service.start();
        final ClusterState state = clusterState(Map.of(P1, localNodeId));

        // the first chain starts a delete-by-query that does not complete
        service.clusterChanged(new ClusterChangedEvent(getTestName(), state, ClusterState.EMPTY_STATE));
        assertThat(heldListeners, hasSize(1));
        assertThat(scheduledCleanups, hasSize(0));

        // the cleanup is stopped, and started again, which starts a second chain with a first cleanup that completes
        service.stopCleanup();
        assertFalse(service.isCleanupRunning());
        service.clusterChanged(new ClusterChangedEvent(getTestName(), state, ClusterState.EMPTY_STATE));
        assertTrue(service.isCleanupRunning());
        assertThat(deleteByQueryProjects, hasSize(2));
        assertThat(scheduledCleanups, hasSize(1));

        // the delete-by-query of the first chain completes, which must not schedule anything because that chain was stopped
        heldListeners.get(0).onResponse(emptyDeleteByQueryResponse());
        assertThat(scheduledCleanups, hasSize(1));

        // the second chain carries on, and is the only one
        scheduledCleanups.get(0).run();
        assertThat(deleteByQueryProjects, hasSize(3));
        assertThat(scheduledCleanups, hasSize(2));
    }

    private static TaskResultsCleanupService createService(
        ProjectResolver projectResolver,
        String localNodeId,
        ThreadPool threadPool,
        Client client
    ) {
        final Set<Setting<?>> registeredSettings = Set.of(
            TaskResultsCleanupService.TASK_RESULTS_RETENTION_PERIOD_SETTING,
            TaskResultsCleanupService.TASK_RESULTS_CLEANUP_INTERVAL_SETTING
        );
        final Settings settings = Settings.builder()
            // the shortest interval the setting allows
            .put(TaskResultsCleanupService.TASK_RESULTS_CLEANUP_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        final ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, registeredSettings));
        return new TaskResultsCleanupService(clusterService, projectResolver, localNodeId, threadPool, client);
    }

    /**
     * A client that records the project of each delete-by-query and leaves the completion of its listener to {@code responder}.
     *
     * @param projectResolver the resolver that tells which project a delete-by-query runs in, from the thread context of the caller
     * @param deleteByQueryProjects where to record the project each delete-by-query ran in, as seen by the {@code projectResolver} at the
     *                              time the request was made. It must be thread-safe if the requests are made from several threads.
     */
    @SuppressWarnings("unchecked")
    private static Client mockDeleteByQueryClient(
        ProjectResolver projectResolver,
        Collection<ProjectId> deleteByQueryProjects,
        Consumer<ActionListener<BulkByPaginatedSearchResponse>> responder
    ) {
        final Client client = Mockito.mock(Client.class);
        Mockito.doAnswer(invocation -> {
            deleteByQueryProjects.add(projectResolver.getProjectId());
            final ActionListener<BulkByPaginatedSearchResponse> listener = invocation.getArgument(2);
            responder.accept(listener);
            return null;
        }).when(client).execute(same(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any(ActionListener.class));
        return client;
    }

    /**
     * @param primaryNodeByProject the projects that have a {@code .tasks} index, with the node that has its primary
     * @param projectsWithoutTasksIndex projects that have no {@code .tasks} index
     */
    private static ClusterState clusterState(Map<ProjectId, String> primaryNodeByProject, ProjectId... projectsWithoutTasksIndex) {
        final Metadata.Builder metadata = Metadata.builder();
        final GlobalRoutingTable.Builder routingTable = GlobalRoutingTable.builder();
        primaryNodeByProject.forEach((project, primaryNodeId) -> {
            final IndexMetadata tasksIndex = IndexMetadata.builder(TASK_INDEX)
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .build();
            metadata.put(ProjectMetadata.builder(project).put(tasksIndex, false));
            final Index index = tasksIndex.getIndex();
            routingTable.put(
                project,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(
                                TestShardRouting.newShardRouting(new ShardId(index, 0), primaryNodeId, true, ShardRoutingState.STARTED)
                            )
                            .build()
                    )
            );
        });
        for (ProjectId project : projectsWithoutTasksIndex) {
            metadata.put(ProjectMetadata.builder(project));
            routingTable.put(project, RoutingTable.builder());
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable.build()).build();
    }

    private static BulkByPaginatedSearchResponse emptyDeleteByQueryResponse() {
        final BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
            null,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            0f,
            null,
            TimeValue.ZERO
        );
        return new BulkByPaginatedSearchResponse(TimeValue.ZERO, status, List.of(), List.of(), false);
    }
}
