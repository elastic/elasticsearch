/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPostStartTrialAction extends TransportMasterNodeAction<PostStartTrialRequest, PostStartTrialResponse> {

    private final MutableLicenseService licenseService;

    @Inject
    public TransportPostStartTrialAction(
        TransportService transportService,
        ClusterService clusterService,
        MutableLicenseService licenseService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            PostStartTrialAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PostStartTrialRequest::new,
            PostStartTrialResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseService = licenseService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PostStartTrialRequest request,
        ClusterState state,
        ActionListener<PostStartTrialResponse> listener
    ) throws Exception {
        if (state.nodes().isMixedVersionCluster()) {
            throw new IllegalStateException(
                "Please ensure all nodes are on the same version before starting your trial, the highest node version in this cluster is ["
                    + state.nodes().getMaxNodeVersion()
                    + "] and the lowest node version is ["
                    + state.nodes().getMinNodeVersion()
                    + "]"
            );
        }
        licenseService.startTrialLicense(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PostStartTrialRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
