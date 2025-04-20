/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetBasicStatusAction extends TransportMasterNodeReadAction<GetBasicStatusRequest, GetBasicStatusResponse> {

    @Inject
    public TransportGetBasicStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            GetBasicStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetBasicStatusRequest::new,
            GetBasicStatusResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetBasicStatusRequest request,
        ClusterState state,
        ActionListener<GetBasicStatusResponse> listener
    ) throws Exception {
        LicensesMetadata licensesMetadata = state.metadata().custom(LicensesMetadata.TYPE);
        if (licensesMetadata == null) {
            listener.onResponse(new GetBasicStatusResponse(true));
        } else {
            License license = licensesMetadata.getLicense();
            listener.onResponse(new GetBasicStatusResponse(license == null || License.LicenseType.isBasic(license.type()) == false));
        }

    }

    @Override
    protected ClusterBlockException checkBlock(GetBasicStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
