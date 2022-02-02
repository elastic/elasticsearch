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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPostStartBasicAction extends TransportMasterNodeAction<PostStartBasicRequest, PostStartBasicResponse> {

    private final LicenseService licenseService;

    @Inject
    public TransportPostStartBasicAction(
        TransportService transportService,
        ClusterService clusterService,
        LicenseService licenseService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PostStartBasicAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PostStartBasicRequest::new,
            indexNameExpressionResolver,
            PostStartBasicResponse::new,
            ThreadPool.Names.SAME
        );
        this.licenseService = licenseService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PostStartBasicRequest request,
        ClusterState state,
        ActionListener<PostStartBasicResponse> listener
    ) throws Exception {
        licenseService.startBasicLicense(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PostStartBasicRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
