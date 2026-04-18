/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.SecurityMigrationAction;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityMigrations;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Transport implementation of {@link SecurityMigrationAction}. Runs on the elected master, applies any outstanding
 * migrations in ascending version order, refreshing the security index between steps and committing progress to
 * cluster state after each migration so that interrupted runs can resume from the last committed version.
 */
public class TransportSecurityMigrationAction extends TransportMasterNodeAction<SecurityMigrationAction.Request, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportSecurityMigrationAction.class);

    private final Client client;
    private final SecurityIndexManager securityIndexManager;
    private final TreeMap<Integer, SecurityMigrations.SecurityMigration> migrationByVersion;
    private final Executor executor;

    @Inject
    public TransportSecurityMigrationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        SecurityMigrations.Holder migrations
    ) {
        super(
            SecurityMigrationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SecurityMigrationAction.Request::new,
            in -> ActionResponse.Empty.INSTANCE,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.client = client;
        this.securityIndexManager = migrations.securityIndexManager();
        this.migrationByVersion = migrations.migrationByVersion();
        this.executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected void masterOperation(
        Task task,
        SecurityMigrationAction.Request request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        final SecurityIndexManager.IndexState projectSecurityIndex = securityIndexManager.getProject(request.getProjectId());
        final ActionListener<Void> completionListener = listener.delegateFailureIgnoreResponseAndWrap(
            l -> l.onResponse(ActionResponse.Empty.INSTANCE)
        );

        if (request.isMigrationNeeded() == false) {
            updateMigrationVersion(
                request.getMigrationVersion(),
                projectSecurityIndex.getConcreteIndexName(),
                completionListener.delegateFailureAndWrap((l, response) -> {
                    logger.info("Security migration not needed. Setting current version to [{}]", request.getMigrationVersion());
                    l.onResponse(response);
                })
            );
            return;
        }

        refreshSecurityIndex(
            projectSecurityIndex,
            new ThreadedActionListener<>(
                executor,
                completionListener.delegateFailureIgnoreResponseAndWrap(
                    l -> applyOutstandingMigrations(projectSecurityIndex, request.getMigrationVersion(), l)
                )
            )
        );
    }

    private void applyOutstandingMigrations(
        SecurityIndexManager.IndexState projectSecurityIndex,
        int currentMigrationVersion,
        ActionListener<Void> migrationsListener
    ) {
        final Map.Entry<Integer, SecurityMigrations.SecurityMigration> migrationEntry = migrationByVersion.higherEntry(
            currentMigrationVersion
        );
        // Check if all nodes can support feature and that the cluster is on a compatible mapping version
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
                                executor,
                                updateVersionListener.delegateFailureIgnoreResponseAndWrap(
                                    refreshListener -> refreshSecurityIndex(
                                        projectSecurityIndex,
                                        new ThreadedActionListener<>(
                                            executor,
                                            refreshListener.delegateFailureIgnoreResponseAndWrap(
                                                l -> applyOutstandingMigrations(projectSecurityIndex, migrationEntry.getKey(), l)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                );
        } else {
            logger.info("Security migrations applied until version: [{}]", currentMigrationVersion);
            migrationsListener.onResponse(null);
        }
    }

    /**
     * Refresh security index to make sure that docs that were migrated are visible to the next migration and to prevent version conflicts
     * or unexpected behaviour by APIs relying on migrated docs.
     */
    private void refreshSecurityIndex(SecurityIndexManager.IndexState projectSecurityIndex, ActionListener<Void> listener) {
        final RefreshRequest refreshRequest = new RefreshRequest(projectSecurityIndex.getConcreteIndexName());
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

    @Override
    protected ClusterBlockException checkBlock(SecurityMigrationAction.Request request, ClusterState state) {
        return null;
    }
}
