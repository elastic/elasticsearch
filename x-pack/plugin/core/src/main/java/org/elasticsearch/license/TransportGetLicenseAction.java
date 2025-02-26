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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetLicenseAction extends TransportMasterNodeReadAction<GetLicenseRequest, GetLicenseResponse> {

    private final LicenseService licenseService;

    @Inject
    public TransportGetLicenseAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        LicenseService licenseService
    ) {
        super(
            GetLicenseAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetLicenseRequest::new,
            GetLicenseResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseService = licenseService;
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
        if (licenseService instanceof ClusterStateLicenseService clusterStateLicenseService) {
            listener.onResponse(new GetLicenseResponse(clusterStateLicenseService.getLicense(state.metadata())));
        } else {
            listener.onResponse(new GetLicenseResponse(licenseService.getLicense()));
        }
    }
}
