/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetLicenseAction extends TransportMasterNodeReadAction<GetLicenseRequest, GetLicenseResponse> {

    @Inject
    public TransportGetLicenseAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetLicenseAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetLicenseRequest::new,
            indexNameExpressionResolver,
            GetLicenseResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetLicenseRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final GetLicenseRequest request,
        ClusterState state,
        final ActionListener<GetLicenseResponse> listener
    ) throws ElasticsearchException {
        listener.onResponse(new GetLicenseResponse(LicenseService.getLicense(state.metadata())));
    }
}
