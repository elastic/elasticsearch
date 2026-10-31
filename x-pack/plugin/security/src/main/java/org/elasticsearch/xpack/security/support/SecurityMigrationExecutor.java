/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.core.security.support.SecurityMigrationTaskParams;
import org.elasticsearch.xpack.security.action.SecurityMigrationAction;

import java.util.concurrent.Executor;

/// Legacy executor for `security-migration` persistent tasks, kept so that tasks created by an older master during a rolling
/// upgrade can still run. It delegates to [SecurityMigrationAction].
///
/// TODO: remove this executor, together with [SecurityMigrationTaskParams] and its named writeable/xcontent registrations,
/// once no supported version can create a `security-migration` persistent task anymore.
public class SecurityMigrationExecutor extends PersistentTasksExecutor<SecurityMigrationTaskParams> {

    private final Client client;

    public SecurityMigrationExecutor(String taskName, Executor executor, Client client) {
        super(taskName, executor);
        this.client = client;
    }

    /// Intentionally kept `false`.
    ///
    /// Security migrations are a run-to-completion one-shot set of steps, not a continuously-running workflow, so there is
    /// no meaningful service gap to close by reassigning early. Opting in the automated shutdown would increase the chance
    /// of concurrent execution during the reassignment overlap and could produce conflicts and unnecessary failure-and-retry
    /// cycles (e.g. in [SecurityMigrations.RoleMetadataFlattenedMigration]).
    @Override
    public boolean automaticReassignmentOnShutdown() {
        return false;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, SecurityMigrationTaskParams params, PersistentTaskState state) {
        client.execute(
            SecurityMigrationAction.INSTANCE,
            new SecurityMigrationAction.Request(params.getMigrationVersion(), params.isMigrationNeeded()),
            ActionListener.wrap(response -> task.markAsCompleted(), task::markAsFailed)
        );
    }
}
