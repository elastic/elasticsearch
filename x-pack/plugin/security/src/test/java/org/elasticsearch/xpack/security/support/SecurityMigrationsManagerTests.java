/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.action.SecurityMigrationAction;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.support.SecurityMigrations.Manager.INITIAL_RETRY_DELAY;
import static org.elasticsearch.xpack.security.support.SecurityMigrations.Manager.MAX_RETRY_DELAY;
import static org.elasticsearch.xpack.security.support.SecurityMigrations.Manager.MAX_SECURITY_MIGRATION_ATTEMPT_COUNT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityMigrationsManagerTests extends ESTestCase {

    private record Execution(
        ProjectId projectId,
        Object origin,
        SecurityMigrationAction.Request request,
        ActionListener<ActionResponse.Empty> listener
    ) {}

    private ThreadPool threadPool;
    // Drives the Manager's scheduled retries, so that tests control time instead of waiting for real delays
    private DeterministicTaskQueue taskQueue;
    private ClusterService clusterService;
    private SecurityIndexManager securityIndexManager;
    private TriConsumer<ProjectId, SecurityIndexManager.IndexState, SecurityIndexManager.IndexState> stateListener;
    private List<Execution> executions;
    private ProjectId projectId;
    private TaskManager taskManager;
    private List<CancellableTask> cancelledTasks;

    @Before
    public void setUpManager() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        executions = new ArrayList<>();
        projectId = randomUniqueProjectId();
        cancelledTasks = new ArrayList<>();
        // Real task manager, except for cancelTaskAndDescendants: the real one needs a TaskCancellationService backed by a
        // TransportService. Propagating the cancellation to descendants is covered by the task manager's own tests.
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of()) {
            @Override
            public void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> l) {
                cancelledTasks.add(task);
                cancel(task, reason, () -> l.onResponse(null));
            }
        };

        taskQueue = new DeterministicTaskQueue();
        final ThreadContext threadContext = taskQueue.getThreadPool().getThreadContext();
        final ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        final NoOpClient client = new NoOpClient(taskQueue.getThreadPool()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertSame(SecurityMigrationAction.INSTANCE, action);
                executions.add(
                    new Execution(
                        projectResolver.getProjectId(),
                        threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME),
                        (SecurityMigrationAction.Request) request,
                        (ActionListener<ActionResponse.Empty>) listener
                    )
                );
            }
        };

        // Mocked: the real classes require the full security plugin initialization
        securityIndexManager = mock(SecurityIndexManager.class);
        final SecuritySystemIndices systemIndices = mock(SecuritySystemIndices.class);
        when(systemIndices.getMainIndexManager()).thenReturn(securityIndexManager);
        // Like the real manager, never return null: projects without a stubbed state (e.g. the default project in the cluster state)
        // have no security index. Mocked because building an unavailable IndexState requires the real manager's internals.
        when(securityIndexManager.getProject(any())).thenReturn(mock(SecurityIndexManager.IndexState.class));

        new SecurityMigrations.Manager(clusterService, client, taskManager, projectResolver, systemIndices);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<TriConsumer<ProjectId, SecurityIndexManager.IndexState, SecurityIndexManager.IndexState>> captor =
            ArgumentCaptor.forClass(TriConsumer.class);
        verify(securityIndexManager).addStateListener(captor.capture());
        stateListener = captor.getValue();
    }

    @After
    public void tearDownManager() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testStartsPendingMigrationOnMaster() {
        notifyStateChange(indexState(0, false));

        assertThat(executions, hasSize(1));
        final Execution execution = executions.get(0);
        assertEquals(projectId, execution.projectId());
        assertEquals(0, execution.request().getMigrationVersion());
        assertTrue(execution.request().isMigrationNeeded());
    }

    public void testSkipsAllMigrationsOnNewIndex() {
        notifyStateChange(indexState(0, true));

        assertThat(executions, hasSize(1));
        assertEquals(SecurityMigrations.highestMigrationVersion(), executions.get(0).request().getMigrationVersion());
        assertFalse(executions.get(0).request().isMigrationNeeded());
    }

    public void testRunsAsSystemUserRegardlessOfCallerContext() {
        try (var ignored = taskQueue.getThreadPool().getThreadContext().stashWithOrigin(randomAlphaOfLength(8))) {
            notifyStateChange(indexState(0, false));
        }

        assertThat(executions, hasSize(1));
        assertThat(executions.get(0).origin(), nullValue());
    }

    public void testDoesNotStartConcurrentMigrations() {
        final SecurityIndexManager.IndexState state = indexState(0, false);
        final int notifications = randomIntBetween(2, 10);
        for (int i = 0; i < notifications; i++) {
            notifyStateChange(state);
        }

        assertThat(executions, hasSize(1));
    }

    public void testRechecksStateWhenMigrationFinishes() {
        notifyStateChange(indexState(0, false));
        assertThat(executions, hasSize(1));

        final SecurityIndexManager.IndexState pendingState = indexState(SecurityMigrations.MIGRATIONS_BY_VERSION.firstKey(), false);
        notifyStateChange(pendingState);
        assertThat(executions, hasSize(1));

        when(securityIndexManager.getProject(projectId)).thenReturn(pendingState);
        executions.get(0).listener().onResponse(ActionResponse.Empty.INSTANCE);
        assertThat(executions, hasSize(2));
        assertEquals(SecurityMigrations.MIGRATIONS_BY_VERSION.firstKey().intValue(), executions.get(1).request().getMigrationVersion());

        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(SecurityMigrations.highestMigrationVersion(), false));
        executions.get(1).listener().onResponse(ActionResponse.Empty.INSTANCE);
        assertThat(executions, hasSize(2));
    }

    /**
     * A failed run must not be retried straight away: the index state is unchanged, so an immediate re-check would retry in a
     * tight loop and burn through the attempt budget. The next attempt is driven by the next state change instead.
     */
    public void testTransientFailureDoesNotExhaustAttempts() {
        final SecurityIndexManager.IndexState state = indexState(0, false);
        when(securityIndexManager.getProject(projectId)).thenReturn(state);
        notifyStateChange(state);
        assertThat(executions, hasSize(1));

        executions.get(0).listener().onFailure(new IllegalStateException("index temporarily unavailable"));
        assertThat(executions, hasSize(1));

        notifyStateChange(state);
        assertThat(executions, hasSize(2));
        assertEquals(0, executions.get(1).request().getMigrationVersion());

        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(SecurityMigrations.highestMigrationVersion(), false));
        executions.get(1).listener().onResponse(ActionResponse.Empty.INSTANCE);
        assertThat(executions, hasSize(2));
    }

    /**
     * The index state may not change after a failure (e.g. for an index that was never migrated), so a failed run is retried
     * after a delay that doubles on every consecutive failure, up to a maximum.
     */
    public void testRetriesFailedMigrationWithBackoff() {
        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(0, false));
        notifyStateChange(indexState(0, false));
        assertThat(executions, hasSize(1));

        long expectedDelay = INITIAL_RETRY_DELAY.millis();
        for (int failures = 1; failures <= 12; failures++) {
            final long failedAt = taskQueue.getCurrentTimeMillis();
            executions.get(failures - 1).listener().onFailure(new IllegalStateException("index temporarily unavailable"));
            taskQueue.runAllRunnableTasks();
            assertThat(executions, hasSize(failures));

            runNextScheduledRetry();
            assertThat(executions, hasSize(failures + 1));
            assertEquals(expectedDelay, taskQueue.getCurrentTimeMillis() - failedAt);
            expectedDelay = Math.min(expectedDelay * 2, MAX_RETRY_DELAY.millis());
        }
        assertEquals(MAX_RETRY_DELAY.millis(), expectedDelay);
    }

    public void testSuccessfulRunResetsRetryDelay() {
        final int pendingVersion = SecurityMigrations.MIGRATIONS_BY_VERSION.firstKey();
        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(0, false));
        notifyStateChange(indexState(0, false));
        executions.get(0).listener().onFailure(new IllegalStateException("migration failed"));
        runNextScheduledRetry();
        executions.get(1).listener().onFailure(new IllegalStateException("migration failed"));
        runNextScheduledRetry();
        assertThat(executions, hasSize(3));

        // A successful run that leaves migrations pending starts the next run straight away
        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(pendingVersion, false));
        executions.get(2).listener().onResponse(ActionResponse.Empty.INSTANCE);
        assertThat(executions, hasSize(4));

        final long failedAt = taskQueue.getCurrentTimeMillis();
        executions.get(3).listener().onFailure(new IllegalStateException("migration failed"));
        runNextScheduledRetry();
        assertThat(executions, hasSize(5));
        assertEquals(INITIAL_RETRY_DELAY.millis(), taskQueue.getCurrentTimeMillis() - failedAt);
    }

    public void testScheduledRetryIsCancelledWhenNoLongerMaster() {
        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(0, false));
        notifyStateChange(indexState(0, false));
        executions.get(0).listener().onFailure(new IllegalStateException("migration failed"));
        assertTrue(taskQueue.hasDeferredTasks());

        // Lose and regain mastership while the index is briefly unavailable, so that regaining it does not start a run itself
        when(securityIndexManager.getProject(projectId)).thenReturn(mock(SecurityIndexManager.IndexState.class));
        setMaster(false);
        setMaster(true);
        assertThat(executions, hasSize(1));

        // The retry scheduled by the former mastership must not fire
        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(0, false));
        taskQueue.runAllTasks();
        assertThat(executions, hasSize(1));
    }

    public void testStopsRetryingAfterMaxAttempts() {
        final SecurityIndexManager.IndexState state = indexState(0, false);
        when(securityIndexManager.getProject(projectId)).thenReturn(state);
        for (int attempt = 0; attempt <= MAX_SECURITY_MIGRATION_ATTEMPT_COUNT; attempt++) {
            notifyStateChange(state);
            assertThat(executions, hasSize(attempt + 1));
            executions.get(attempt).listener().onFailure(new IllegalStateException("migration failed"));
        }

        notifyStateChange(state);
        assertThat(executions, hasSize(MAX_SECURITY_MIGRATION_ATTEMPT_COUNT + 1));
    }

    /**
     * During a rolling upgrade a `security-migration` persistent task created by an older master may still be running. A new run
     * must not overlap it; once the legacy task is gone, a new run resumes from the version it left.
     */
    public void testDoesNotStartWhileLegacyTaskRunning() {
        final int migrationsVersion = SecurityMigrations.MIGRATIONS_BY_VERSION.firstKey();
        notifyStateChange(indexState(migrationsVersion, false, true));
        assertThat(executions, empty());

        notifyStateChange(indexState(migrationsVersion, false, false));
        assertThat(executions, hasSize(1));
        assertEquals(migrationsVersion, executions.get(0).request().getMigrationVersion());
    }

    public void testLegacyTaskRunningDoesNotConsumeAttempts() {
        final SecurityIndexManager.IndexState legacyRunning = indexState(0, false, true);
        for (int i = 0; i <= MAX_SECURITY_MIGRATION_ATTEMPT_COUNT; i++) {
            notifyStateChange(legacyRunning);
        }
        assertThat(executions, empty());

        notifyStateChange(indexState(0, false, false));
        assertThat(executions, hasSize(1));
    }

    public void testRunsAsChildOfRegisteredTaskAndUnregistersOnCompletion() {
        notifyStateChange(indexState(0, false));
        assertThat(executions, hasSize(1));
        final CancellableTask task = taskManager.getCancellableTask(executions.get(0).request().getParentTask().getId());
        assertNotNull(task);
        assertEquals(clusterService.localNode().getId(), executions.get(0).request().getParentTask().getNodeId());

        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(SecurityMigrations.highestMigrationVersion(), false));
        if (randomBoolean()) {
            executions.get(0).listener().onResponse(ActionResponse.Empty.INSTANCE);
        } else {
            executions.get(0).listener().onFailure(new IllegalStateException("migration failed"));
        }
        assertNull(taskManager.getCancellableTask(task.getId()));
    }

    /**
     * A change of master alone does not change the index state, so the index state listener may not fire (for instance on an
     * index that has never been migrated). The new master must still pick up pending migrations.
     */
    public void testRechecksPendingMigrationsWhenBecomingMaster() {
        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(0, false));
        setMaster(false);
        assertThat(executions, empty());

        setMaster(true);
        assertThat(executions, hasSize(1));
        assertEquals(projectId, executions.get(0).projectId());
        assertEquals(0, executions.get(0).request().getMigrationVersion());
    }

    /**
     * A run in flight on a node that is no longer the elected master must be cancelled, so that it does not overlap the run the
     * new master starts. Its late completion must not affect a newer run.
     */
    public void testCancelsMigrationInProgressWhenNoLongerMaster() {
        final SecurityIndexManager.IndexState state = indexState(0, false);
        when(securityIndexManager.getProject(projectId)).thenReturn(state);
        notifyStateChange(state);
        assertThat(executions, hasSize(1));
        final CancellableTask task = taskManager.getCancellableTask(executions.get(0).request().getParentTask().getId());

        setMaster(false);
        assertThat(cancelledTasks, contains(task));
        assertTrue(task.isCancelled());

        // Regaining mastership starts a new run even though the cancelled one has not completed yet
        setMaster(true);
        assertThat(executions, hasSize(2));

        executions.get(0).listener().onFailure(new TaskCancelledException("cancelled"));
        notifyStateChange(state);
        assertThat(executions, hasSize(2));
        assertNotNull(taskManager.getCancellableTask(executions.get(1).request().getParentTask().getId()));
    }

    public void testDoesNotRunOnNonMasterNode() {
        final ClusterState state = clusterService.state();
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(null))
        );

        notifyStateChange(indexState(0, false));

        assertThat(executions, empty());
    }

    private void runNextScheduledRetry() {
        assertTrue("expected a scheduled retry", taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
    }

    private void setMaster(boolean localNodeIsMaster) {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(localNodeIsMaster ? state.nodes().getLocalNodeId() : null));
        if (state.metadata().hasProject(projectId) == false) {
            builder.putProjectMetadata(ProjectMetadata.builder(projectId));
        }
        ClusterServiceUtils.setState(clusterService, builder);
    }

    private void notifyStateChange(SecurityIndexManager.IndexState newState) {
        stateListener.apply(projectId, null, newState);
    }

    private SecurityIndexManager.IndexState indexState(int migrationsVersion, boolean createdOnLatestVersion) {
        return indexState(migrationsVersion, createdOnLatestVersion, false);
    }

    private SecurityIndexManager.IndexState indexState(
        int migrationsVersion,
        boolean createdOnLatestVersion,
        boolean legacyMigrationTaskRunning
    ) {
        final Set<NodeFeature> allMigrationFeatures = SecurityMigrations.MIGRATIONS_BY_VERSION.values()
            .stream()
            .flatMap(migration -> migration.nodeFeaturesRequired().stream())
            .collect(Collectors.toSet());
        return securityIndexManager.new IndexState(
            projectId, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), true, true, true, true, createdOnLatestVersion,
            SecurityIndexManager.RoleMappingsCleanupMigrationStatus.READY, legacyMigrationTaskRunning, migrationsVersion, null,
            Integer.MAX_VALUE, ".security-7", ClusterHealthStatus.GREEN, IndexMetadata.State.OPEN, "uuid", allMigrationFeatures
        );
    }
}
