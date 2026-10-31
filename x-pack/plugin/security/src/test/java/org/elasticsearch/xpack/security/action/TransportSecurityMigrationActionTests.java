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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionResponse;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSecurityMigrationActionTests extends ESTestCase {
    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager securityIndexManager;

    private int updateIndexMigrationVersionActionInvocations;
    private int refreshActionInvocations;

    private boolean updateVersionShouldThrowException = false;

    private boolean refreshIndexShouldThrowException = false;

    private CancellableTask task;
    private SecurityIndexManager.IndexState projectIndex;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        updateIndexMigrationVersionActionInvocations = 0;
        refreshActionInvocations = 0;
        task = new CancellableTask(randomNonNegativeLong(), "transport", SecurityMigrationAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
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
                    fail("Unexpected client request");
                }

            }
        };
        securityIndexManager = mock(SecurityIndexManager.class);
        projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndexManager.forCurrentProject()).thenReturn(projectIndex);
    }

    public void testSuccessfulMigration() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );

        assertCompleted(execute(action, new SecurityMigrationAction.Request(0, true)));
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

        assertCompleted(execute(action, new SecurityMigrationAction.Request(0, true)));
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

        assertCompleted(execute(action, new SecurityMigrationAction.Request(0, true)));
        assertEquals(3, refreshActionInvocations);
        assertEquals(2, updateIndexMigrationVersionActionInvocations);
        assertEquals(2, migrateInvocations[0]);
    }

    public void testNoMigrationNeeded() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );

        assertCompleted(execute(action, new SecurityMigrationAction.Request(7, true)));
        assertEquals(0, updateIndexMigrationVersionActionInvocations);
        assertEquals(1, refreshActionInvocations);
        assertEquals(0, migrateInvocations[0]);
    }

    public void testMigrationSkippedOnNewIndex() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );

        assertCompleted(execute(action, new SecurityMigrationAction.Request(2, false)));
        assertEquals(1, updateIndexMigrationVersionActionInvocations);
        assertEquals(0, refreshActionInvocations);
        assertEquals(0, migrateInvocations[0]);
    }

    public void testMigrationThrowsRuntimeException() {
        SecurityIndexManager.IndexState projectIndex = Mockito.mock(SecurityIndexManager.IndexState.class);
        when(securityIndexManager.forCurrentProject()).thenReturn(projectIndex);
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

        assertThat(assertFailed(execute(action, new SecurityMigrationAction.Request(0, true))), instanceOf(IllegalStateException.class));
    }

    public void testUpdateMigrationVersionThrowsException() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );
        updateVersionShouldThrowException = true;
        assertThat(assertFailed(execute(action, new SecurityMigrationAction.Request(0, true))), instanceOf(IllegalStateException.class));
    }

    public void testRefreshSecurityIndexThrowsException() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );
        refreshIndexShouldThrowException = true;
        assertCompleted(execute(action, new SecurityMigrationAction.Request(0, true)));
    }

    public void testCancelledTask() {
        final int[] migrateInvocations = new int[1];
        TransportSecurityMigrationAction action = createAction(
            new TreeMap<>(Map.of(1, generateMigration(migrateInvocations, true), 2, generateMigration(migrateInvocations, true)))
        );
        TaskCancelHelper.cancel(task, "test");

        assertThat(assertFailed(execute(action, new SecurityMigrationAction.Request(0, true))), instanceOf(TaskCancelledException.class));
        assertEquals(0, updateIndexMigrationVersionActionInvocations);
        assertEquals(0, migrateInvocations[0]);
    }

    public void testRequestIsLocalOnly() {
        expectThrows(AssertionError.class, () -> new SecurityMigrationAction.Request(0, true).writeTo(null));
    }

    private TransportSecurityMigrationAction createAction(TreeMap<Integer, SecurityMigrations.SecurityMigration> migrationByVersion) {
        return new TransportSecurityMigrationAction(
            new TaskManager(Settings.EMPTY, threadPool, Set.of()),
            new ActionFilters(Set.of()),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            client,
            securityIndexManager,
            migrationByVersion
        );
    }

    private PlainActionFuture<ActionResponse.Empty> execute(
        TransportSecurityMigrationAction action,
        SecurityMigrationAction.Request request
    ) {
        final PlainActionFuture<ActionResponse.Empty> future = new PlainActionFuture<>();
        action.doExecute(task, request, future);
        assertTrue(future.isDone());
        return future;
    }

    private static void assertCompleted(PlainActionFuture<ActionResponse.Empty> future) {
        assertSame(ActionResponse.Empty.INSTANCE, future.actionGet());
    }

    private static Exception assertFailed(PlainActionFuture<ActionResponse.Empty> future) {
        return expectThrows(Exception.class, future::actionGet);
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
