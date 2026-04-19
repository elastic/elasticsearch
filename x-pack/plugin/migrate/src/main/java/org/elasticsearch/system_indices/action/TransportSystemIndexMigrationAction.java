/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.system_indices.task.SystemIndexMigrator;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Runs the system index migration on the elected master node. Created fresh for every invocation of
 * {@link SystemIndexMigrationAction}; progress is stored in cluster state via
 * {@link org.elasticsearch.system_indices.task.FeatureMigrationResults} so callers can poll for status.
 */
public class TransportSystemIndexMigrationAction extends TransportMasterNodeAction<
    SystemIndexMigrationAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportSystemIndexMigrationAction.class);

    private final Client client;
    private final SystemIndices systemIndices;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public TransportSystemIndexMigrationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        SystemIndices systemIndices,
        IndexScopedSettings indexScopedSettings
    ) {
        super(
            SystemIndexMigrationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SystemIndexMigrationAction.Request::new,
            in -> ActionResponse.Empty.INSTANCE,
            threadPool.generic()
        );
        this.client = client;
        this.systemIndices = systemIndices;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected void masterOperation(
        Task task,
        SystemIndexMigrationAction.Request request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        logger.debug("starting system index migration for project [{}]", request.getProjectId());
        var migrator = new SystemIndexMigrator(
            client,
            clusterService,
            systemIndices,
            indexScopedSettings,
            threadPool,
            request.getProjectId()
        );
        migrator.run(listener.delegateFailureIgnoreResponseAndWrap(l -> l.onResponse(ActionResponse.Empty.INSTANCE)));
    }

    @Override
    protected ClusterBlockException checkBlock(SystemIndexMigrationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
