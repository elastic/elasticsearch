/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;

public class TransportPutDataSourceAction extends AcknowledgedTransportMasterNodeProjectAction<PutDataSourceAction.Request> {
    private final DataSourceService dataSourceService;
    private final EncryptionService encryptionService;

    @Inject
    public TransportPutDataSourceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DataSourceService dataSourceService,
        ProjectResolver projectResolver,
        DataSourceCredentials credentials,
        EncryptionService encryptionService
    ) {
        super(
            PutDataSourceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDataSourceAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.dataSourceService = dataSourceService;
        this.encryptionService = encryptionService;
        // EsqlPlugin couples the datasources feature to the project-encryption-key feature, so an
        // EncryptionService is always bound when this action is registered — hence a hard injection
        // rather than optional. This action is constructed on every node at startup; push the service
        // into the shared DataSourceCredentials so the data-node decryption step (the lazy wrappers in
        // DataSourceModule) reads through the same instance.
        credentials.setEncryptionService(encryptionService);
    }

    @Override
    protected void doExecute(Task task, PutDataSourceAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // Coord-side pre-check; the task body re-validates under CAS.
        try {
            dataSourceService.validatePutDataSource(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutDataSourceAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        dataSourceService.putDataSource(state.projectId(), request, encryptionService, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataSourceAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
