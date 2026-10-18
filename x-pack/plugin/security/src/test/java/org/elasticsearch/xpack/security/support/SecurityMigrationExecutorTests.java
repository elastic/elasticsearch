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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.support.SecurityMigrationTaskParams;
import org.elasticsearch.xpack.security.action.SecurityMigrationAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SecurityMigrationExecutorTests extends ESTestCase {

    private ThreadPool threadPool;
    private List<SecurityMigrationAction.Request> requests;
    private Exception failure;
    private SecurityMigrationExecutor executor;
    // Mocked: a real AllocatedPersistentTask needs the persistent tasks service and task manager to be initialized
    private AllocatedPersistentTask task;

    @Before
    public void setUpExecutor() {
        threadPool = new TestThreadPool(getTestName());
        requests = new ArrayList<>();
        failure = null;
        task = mock(AllocatedPersistentTask.class);
        executor = new SecurityMigrationExecutor(
            SecurityMigrationTaskParams.TASK_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertSame(SecurityMigrationAction.INSTANCE, action);
                    requests.add((SecurityMigrationAction.Request) request);
                    if (failure != null) {
                        listener.onFailure(failure);
                    } else {
                        listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                    }
                }
            }
        );
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testDelegatesToSecurityMigrationAction() {
        final int migrationVersion = randomNonNegativeInt();
        final boolean migrationNeeded = randomBoolean();

        executor.nodeOperation(task, new SecurityMigrationTaskParams(migrationVersion, migrationNeeded), mock(PersistentTaskState.class));

        assertEquals(1, requests.size());
        assertEquals(migrationVersion, requests.get(0).getMigrationVersion());
        assertEquals(migrationNeeded, requests.get(0).isMigrationNeeded());
        verify(task).markAsCompleted();
        verify(task, never()).markAsFailed(any());
    }

    public void testMarksTaskAsFailedWhenActionFails() {
        failure = new IllegalStateException("migration failed");

        executor.nodeOperation(task, new SecurityMigrationTaskParams(0, true), mock(PersistentTaskState.class));

        assertEquals(1, requests.size());
        verify(task).markAsFailed(same(failure));
        verify(task, never()).markAsCompleted();
    }

    public void testDoesNotOptIntoAutomaticReassignmentOnShutdown() {
        assertFalse(executor.automaticReassignmentOnShutdown());
    }
}
