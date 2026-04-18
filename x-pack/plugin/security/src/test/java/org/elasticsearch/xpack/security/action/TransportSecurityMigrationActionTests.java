/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.SecurityMigrationAction;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionResponse;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSecurityMigrationActionTests extends ESTestCase {
    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager securityIndexManager;
    private SecurityIndexManager.IndexState projectIndex;

    private int updateIndexMigrationVersionActionInvocations;
    private int refreshActionInvocations;

    private boolean updateVersionShouldThrowException = false;
    private boolean refreshIndexShouldThrowException = false;

    private static final ProjectId PROJECT_ID = ProjectId.DEFAULT;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(any())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        updateIndexMigrationVersionActionInvocations = 0;
        refreshActionInvocations = 0;
        client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof RefreshRequest) {
                    if (refreshIndexShouldThrowException) {
                        if (randomBoolean()) {
                            listener.onFailure(new IllegalStateException("Refresh index failed"));
                        } else {
                            listener.onResponse((Response) new BroadcastResponse(1, 0, 1, List.of()));
                        }
                    } else {
                        refreshActionInvocations++;
                        listener.onResponse((Response) new BroadcastResponse(1, 1, 0, List.of()));
                    }
                } else if (request instanceof UpdateIndexMigrationVersionAction.Request) {
                    if (updateVersionShouldThrowException) {
                        listener.onFailure(new IllegalStateException("Update version failed"));
                    } else {
                        updateIndexMigrationVersionActionInvocations++;
                        listener.onResponse((Response) new UpdateIndexMigrationVersionResponse());
                    }
                } else {
                    fail("Unexpected client request: " + request.getClass().getSimpleName());
                }
            }
        };
        securityIndexManager = mock(SecurityIndexManager.class);
        projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndexManager.getProject(PROJECT_ID)).thenReturn(projectIndex);
        when(projectIndex.getConcreteIndexName()).thenReturn(".security-7");
    }

    private TransportSecurityMigrationAction createAction(TreeMap<Integer, SecurityMigrations.SecurityMigration> migrations) {
        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(org.elasticsearch.tasks.TaskManager.class));
        ClusterService clusterService = mock(ClusterService.class);
        SecurityMigrations.Holder holder = new SecurityMigrations.Holder(securityIndexManager, migrations);
        return new TransportSecurityMigrationAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Set.of()),
            client,
            holder
        );
    }

    private SecurityMigrationAction.Request request(int version, boolean migrationNeeded) {
        return new SecurityMigrationAction.Request(TimeValue.THIRTY_SECONDS, PROJECT_ID, version, migrationNeeded);
    }

    public void testSuccessfulMigration() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(0, true),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        assertTrue(completed.get());
        assertNull(failure.get());
        assertEquals(2, updateIndexMigrationVersionActionInvocations);
        assertEquals(3, refreshActionInvocations);
        assertEquals(2, migrateInvocations[0]);
    }

    public void testNoMigrationMeetsRequirements() {
        final int[] migrateInvocationsCounter = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(
                Map.of(
                    1,
                    generateMigration(migrateInvocationsCounter, false),
                    2,
                    generateMigration(migrateInvocationsCounter, false),
                    3,
                    generateMigration(migrateInvocationsCounter, false)
                )
            )
        );

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(0, true),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        assertTrue(completed.get());
        assertNull(failure.get());
        assertEquals(0, updateIndexMigrationVersionActionInvocations);
        assertEquals(1, refreshActionInvocations);
        assertEquals(0, migrateInvocationsCounter[0]);
    }

    public void testPartialMigration() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(
                Map.of(
                    1,
                    generateMigration(migrateInvocations, true),
                    2,
                    generateMigration(migrateInvocations, true),
                    3,
                    generateMigration(migrateInvocations, false),
                    4,
                    generateMigration(migrateInvocations, false),
                    5,
                    generateMigration(migrateInvocations, true)
                )
            )
        );

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(0, true),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        assertTrue(completed.get());
        assertNull(failure.get());
        assertEquals(3, refreshActionInvocations);
        assertEquals(2, updateIndexMigrationVersionActionInvocations);
        assertEquals(2, migrateInvocations[0]);
    }

    public void testMigrationNotNeededSetsVersion() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(5, false),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        assertTrue(completed.get());
        assertNull(failure.get());
        assertEquals(1, updateIndexMigrationVersionActionInvocations);
        assertEquals(0, refreshActionInvocations);
        assertEquals(0, migrateInvocations[0]);
    }

    public void testMigrationThrowsRuntimeException() {
        when(projectIndex.isReadyForSecurityMigration(any())).thenReturn(true);
        TransportSecurityMigrationAction action = createAction(new TreeMap<>(Map.of(1, new SecurityMigrations.SecurityMigration() {
            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                throw new IllegalStateException("Oh no, this is a terrible state");
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of();
            }

            @Override
            public int minMappingVersion() {
                return 0;
            }
        })));

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(0, true),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        assertFalse(completed.get());
        assertNotNull(failure.get());
    }

    public void testUpdateMigrationVersionThrowsException() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );
        updateVersionShouldThrowException = true;

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(0, true),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        assertFalse(completed.get());
        assertNotNull(failure.get());
    }

    public void testRefreshSecurityIndexThrowsExceptionDoesNotStopMigration() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );
        refreshIndexShouldThrowException = true;

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            mock(Task.class),
            request(0, true),
            mock(ClusterState.class),
            ActionListener.wrap(r -> completed.set(true), failure::set)
        );

        // Refresh failures are non-fatal; migration should still complete
        assertTrue(completed.get());
        assertNull(failure.get());
    }

    private SecurityMigrations.SecurityMigration generateMigration(int[] migrateInvocationsCounter, boolean isEligible) {
        SecurityMigrations.SecurityMigration migration = new SecurityMigrations.SecurityMigration() {
            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                migrateInvocationsCounter[0]++;
                listener.onResponse(null);
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of();
            }

            @Override
            public int minMappingVersion() {
                return 0;
            }
        };
        when(projectIndex.isReadyForSecurityMigration(migration)).thenReturn(isEligible);
        return migration;
    }
}
