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
import org.elasticsearch.xpack.esql.datasources.FederationLicense;

public class TransportDeleteDataSourceAction extends AcknowledgedTransportMasterNodeProjectAction<DeleteDataSourceAction.Request> {
    private final DataSourceService dataSourceService;
    private final FederationLicense federationLicense;

    @Inject
    public TransportDeleteDataSourceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DataSourceService dataSourceService,
        ProjectResolver projectResolver,
        FederationLicense federationLicense
    ) {
        super(
            DeleteDataSourceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDataSourceAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.dataSourceService = dataSourceService;
        this.federationLicense = federationLicense;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDataSourceAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        federationLicense.check();
        dataSourceService.deleteDataSources(
            state.projectId(),
            request.masterNodeTimeout(),
            request.ackTimeout(),
            java.util.Arrays.asList(request.names()),
            listener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataSourceAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
