/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteLicenseAction extends TransportMasterNodeAction<DeleteLicenseRequest, DeleteLicenseResponse> {

    private final LicensesService licensesService;

    @Inject
    public TransportDeleteLicenseAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        LicensesService licensesService, ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteLicenseAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, DeleteLicenseRequest::new);
        this.licensesService = licensesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected DeleteLicenseResponse newResponse() {
        return new DeleteLicenseResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteLicenseRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(final DeleteLicenseRequest request, ClusterState state, final ActionListener<DeleteLicenseResponse>
            listener) throws ElasticsearchException {
        licensesService.removeLicense(request, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new DeleteLicenseResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }
}
