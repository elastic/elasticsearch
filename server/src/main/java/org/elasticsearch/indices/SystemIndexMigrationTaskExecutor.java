/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 * {@link PersistentTasksExecutor} responsible for executing pending {@link SystemIndexMigrationTask} implementations and calling
 * {@link UpdateIndexMigrationVersionAction} after each applied migration with the latest version.
 */
public class SystemIndexMigrationTaskExecutor extends PersistentTasksExecutor<SystemIndexMigrationTaskParams> {

    private static final Logger logger = LogManager.getLogger(SystemIndexMigrationTaskExecutor.class);
    private final Client client;
    private final SystemIndices systemIndices;

    public SystemIndexMigrationTaskExecutor(Executor executor, Client client, SystemIndices systemIndices) {
        super(SystemIndexMigrationTaskParams.TASK_NAME, executor);
        this.client = client;
        this.systemIndices = systemIndices;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, SystemIndexMigrationTaskParams params, PersistentTaskState state) {
        applyMigrations(
            task,
            params.getIndexName(),
            Arrays.stream(params.getMigrationVersions()).iterator(),
            ActionListener.wrap((res) -> task.markAsCompleted(), (exception) -> {
                logger.warn("System index migration failed: " + exception);
                task.markAsFailed(exception);
            })
        );
    }

    private void applyMigrations(
        AllocatedPersistentTask task,
        String indexName,
        Iterator<Integer> versionIterator,
        ActionListener<Void> listener
    ) {
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("System migration task cancelled"));
            return;
        }
        SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(indexName);
        if (descriptor != null && versionIterator.hasNext()) {
            Integer migrationVersionToApply = versionIterator.next();
            descriptor.getMigrationTaskByVersion(migrationVersionToApply)
                .migrate(
                    ActionListener.wrap(
                        response -> updateMigrationVersion(
                            migrationVersionToApply,
                            indexName,
                            new ThreadedActionListener<>(
                                this.getExecutor(),
                                ActionListener.wrap(
                                    (res) -> applyMigrations(task, indexName, versionIterator, listener),
                                    listener::onFailure
                                )
                            )
                        ),
                        listener::onFailure
                    )
                );
        } else {
            int lastMigrationVersion = descriptor != null ? descriptor.getMostRecentMigrationVersion() : 0;
            logger.info("System index migrations applied for [" + indexName + "] until version [" + lastMigrationVersion + "]");
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
