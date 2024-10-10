/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.core.security.support.SecurityMigrationTaskParams;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;

public class SecurityMigrationExecutor extends PersistentTasksExecutor<SecurityMigrationTaskParams> {

    private static final Logger logger = LogManager.getLogger(SecurityMigrationExecutor.class);
    private final SecurityIndexManager securityIndexManager;
    private final Client client;
    private final TreeMap<Integer, SecurityMigrations.SecurityMigration> migrationByVersion;

    public SecurityMigrationExecutor(
        String taskName,
        Executor executor,
        SecurityIndexManager securityIndexManager,
        Client client,
        TreeMap<Integer, SecurityMigrations.SecurityMigration> migrationByVersion
    ) {
        super(taskName, executor);
        this.securityIndexManager = securityIndexManager;
        this.client = client;
        this.migrationByVersion = migrationByVersion;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, SecurityMigrationTaskParams params, PersistentTaskState state) {
        ActionListener<Void> listener = ActionListener.wrap((res) -> task.markAsCompleted(), (exception) -> {
            logger.warn("Security migration failed: " + exception);
            task.markAsFailed(exception);
        });

        if (params.isMigrationNeeded() == false) {
            updateMigrationVersion(
                params.getMigrationVersion(),
                securityIndexManager.getConcreteIndexName(),
                ActionListener.wrap(response -> {
                    logger.info("Security migration not needed. Setting current version to: [" + params.getMigrationVersion() + "]");
                    listener.onResponse(response);
                }, listener::onFailure)
            );
            return;
        }

        applyOutstandingMigrations(task, params.getMigrationVersion(), listener);
    }

    private void applyOutstandingMigrations(AllocatedPersistentTask task, int currentMigrationVersion, ActionListener<Void> listener) {
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("Security migration task cancelled"));
            return;
        }
        Map.Entry<Integer, SecurityMigrations.SecurityMigration> migrationEntry = migrationByVersion.higherEntry(currentMigrationVersion);

        // Check if all nodes can support feature and that the cluster is on a compatible mapping version
        if (migrationEntry != null && securityIndexManager.isReadyForSecurityMigration(migrationEntry.getValue())) {
            migrationEntry.getValue()
                .migrate(
                    securityIndexManager,
                    client,
                    ActionListener.wrap(
                        response -> updateMigrationVersion(
                            migrationEntry.getKey(),
                            securityIndexManager.getConcreteIndexName(),
                            new ThreadedActionListener<>(
                                this.getExecutor(),
                                ActionListener.wrap(
                                    updateResponse -> applyOutstandingMigrations(task, migrationEntry.getKey(), listener),
                                    listener::onFailure
                                )
                            )
                        ),
                        listener::onFailure
                    )
                );
        } else {
            logger.info("Security migrations applied until version: [" + currentMigrationVersion + "]");
            listener.onResponse(null);
        }
    }

    private void updateMigrationVersion(int migrationVersion, String indexName, ActionListener<Void> listener) {
        client.execute(
            UpdateIndexMigrationVersionAction.INSTANCE,
            new UpdateIndexMigrationVersionAction.Request(TimeValue.MAX_VALUE, migrationVersion, indexName),
            ActionListener.wrap((response) -> {
                listener.onResponse(null);
            }, listener::onFailure)
        );
    }
}
