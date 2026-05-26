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
    private final DataSourceCredentials credentials;

    // Optional Guice injection: the encryption plugin only binds an EncryptionService when its PEK
    // feature flag is on, so the binding may be absent. When unset, putDataSource rejects a
    // secret-bearing request with 503 (DataSourceService.applyEncryption) rather than storing
    // plaintext; the data-node decryption step is likewise strict about encrypted blobs without a key.
    private volatile EncryptionService encryptionService;

    @Inject(optional = true)
    public void setEncryptionService(EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
        // Push the service into the shared DataSourceCredentials so the data-node decryption step
        // (called from the lazy wrappers in DataSourceModule) sees the same binding.
        credentials.setEncryptionService(encryptionService);
    }

    @Inject
    public TransportPutDataSourceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DataSourceService dataSourceService,
        ProjectResolver projectResolver,
        DataSourceCredentials credentials
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
        this.credentials = credentials;
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
