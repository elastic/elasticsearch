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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
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

import static org.elasticsearch.xpack.security.support.SecurityMigrations.Manager.MAX_SECURITY_MIGRATION_ATTEMPT_COUNT;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
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
    private ClusterService clusterService;
    private SecurityIndexManager securityIndexManager;
    private TriConsumer<ProjectId, SecurityIndexManager.IndexState, SecurityIndexManager.IndexState> stateListener;
    private List<Execution> executions;
    private ProjectId projectId;

    @Before
    public void setUpManager() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        executions = new ArrayList<>();
        projectId = randomUniqueProjectId();

        final ThreadContext threadContext = threadPool.getThreadContext();
        final ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        final NoOpClient client = new NoOpClient(threadPool) {
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

        new SecurityMigrations.Manager(clusterService, client, projectResolver, systemIndices);

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
        try (var ignored = threadPool.getThreadContext().stashWithOrigin(randomAlphaOfLength(8))) {
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
        if (randomBoolean()) {
            executions.get(0).listener().onResponse(ActionResponse.Empty.INSTANCE);
        } else {
            executions.get(0).listener().onFailure(new IllegalStateException("migration failed"));
        }
        assertThat(executions, hasSize(2));
        assertEquals(SecurityMigrations.MIGRATIONS_BY_VERSION.firstKey().intValue(), executions.get(1).request().getMigrationVersion());

        when(securityIndexManager.getProject(projectId)).thenReturn(indexState(SecurityMigrations.highestMigrationVersion(), false));
        executions.get(1).listener().onResponse(ActionResponse.Empty.INSTANCE);
        assertThat(executions, hasSize(2));
    }

    public void testStopsRetryingAfterMaxAttempts() {
        final SecurityIndexManager.IndexState state = indexState(0, false);
        when(securityIndexManager.getProject(projectId)).thenReturn(state);
        notifyStateChange(state);
        int previousSize;
        do {
            previousSize = executions.size();
            executions.get(previousSize - 1).listener().onFailure(new IllegalStateException("migration failed"));
        } while (executions.size() > previousSize);
        assertThat(executions, hasSize(MAX_SECURITY_MIGRATION_ATTEMPT_COUNT + 1));

        notifyStateChange(state);
        assertThat(executions, hasSize(MAX_SECURITY_MIGRATION_ATTEMPT_COUNT + 1));
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

    private void notifyStateChange(SecurityIndexManager.IndexState newState) {
        stateListener.apply(projectId, null, newState);
    }

    private SecurityIndexManager.IndexState indexState(int migrationsVersion, boolean createdOnLatestVersion) {
        final Set<NodeFeature> allMigrationFeatures = SecurityMigrations.MIGRATIONS_BY_VERSION.values()
            .stream()
            .flatMap(migration -> migration.nodeFeaturesRequired().stream())
            .collect(Collectors.toSet());
        return securityIndexManager.new IndexState(
            projectId, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), true, true, true, true, createdOnLatestVersion,
            SecurityIndexManager.RoleMappingsCleanupMigrationStatus.READY, false, migrationsVersion, null, Integer.MAX_VALUE, ".security-7",
            ClusterHealthStatus.GREEN, IndexMetadata.State.OPEN, "uuid", allMigrationFeatures
        );
    }
}
