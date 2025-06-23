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
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.core.security.support.SecurityMigrationTaskParams;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

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
                securityIndexManager.forCurrentProject().getConcreteIndexName(),
                listener.delegateFailureAndWrap((l, response) -> {
                    logger.info("Security migration not needed. Setting current version to: [" + params.getMigrationVersion() + "]");
                    l.onResponse(response);
                })
            );
            return;
        }

        refreshSecurityIndex(
            new ThreadedActionListener<>(
                this.getExecutor(),
                listener.delegateFailureIgnoreResponseAndWrap(l -> applyOutstandingMigrations(task, params.getMigrationVersion(), l))
            )
        );
    }

    private void applyOutstandingMigrations(
        AllocatedPersistentTask task,
        int currentMigrationVersion,
        ActionListener<Void> migrationsListener
    ) {
        if (task.isCancelled()) {
            migrationsListener.onFailure(new TaskCancelledException("Security migration task cancelled"));
            return;
        }
        Map.Entry<Integer, SecurityMigrations.SecurityMigration> migrationEntry = migrationByVersion.higherEntry(currentMigrationVersion);

        // Check if all nodes can support feature and that the cluster is on a compatible mapping version
        final IndexState projectSecurityIndex = securityIndexManager.forCurrentProject();
        if (migrationEntry != null && projectSecurityIndex.isReadyForSecurityMigration(migrationEntry.getValue())) {
            migrationEntry.getValue()
                .migrate(
                    securityIndexManager,
                    client,
                    migrationsListener.delegateFailureIgnoreResponseAndWrap(
                        updateVersionListener -> updateMigrationVersion(
                            migrationEntry.getKey(),
                            projectSecurityIndex.getConcreteIndexName(),
                            new ThreadedActionListener<>(
                                this.getExecutor(),
                                updateVersionListener.delegateFailureIgnoreResponseAndWrap(refreshListener -> {
                                    refreshSecurityIndex(
                                        new ThreadedActionListener<>(
                                            this.getExecutor(),
                                            refreshListener.delegateFailureIgnoreResponseAndWrap(
                                                l -> applyOutstandingMigrations(task, migrationEntry.getKey(), l)
                                            )
                                        )
                                    );
                                })
                            )
                        )
                    )
                );
        } else {
            logger.info("Security migrations applied until version: [" + currentMigrationVersion + "]");
            migrationsListener.onResponse(null);
        }
    }

    /**
     * Refresh security index to make sure that docs that were migrated are visible to the next migration and to prevent version conflicts
     * or unexpected behaviour by APIs relying on migrated docs.
     */
    private void refreshSecurityIndex(ActionListener<Void> listener) {
        RefreshRequest refreshRequest = new RefreshRequest(securityIndexManager.forCurrentProject().getConcreteIndexName());
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, RefreshAction.INSTANCE, refreshRequest, ActionListener.wrap(response -> {
            if (response.getFailedShards() != 0) {
                // Log a warning but do not stop migration, since this is not a critical operation
                logger.warn("Failed to refresh security index during security migration {}", Arrays.toString(response.getShardFailures()));
            }
            listener.onResponse(null);
        }, exception -> {
            // Log a warning but do not stop migration, since this is not a critical operation
            logger.warn("Failed to refresh security index during security migration", exception);
            listener.onResponse(null);
        }));
    }

    private void updateMigrationVersion(int migrationVersion, String indexName, ActionListener<Void> listener) {
        client.execute(
            UpdateIndexMigrationVersionAction.INSTANCE,
            new UpdateIndexMigrationVersionAction.Request(TimeValue.MAX_VALUE, migrationVersion, indexName),
            listener.delegateFailureIgnoreResponseAndWrap(l -> l.onResponse(null))
        );
    }
}
