/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.license.plugin.core.LicensesService.DeleteLicenseRequestHolder;

public class TransportDeleteLicenseAction extends TransportMasterNodeOperationAction<DeleteLicenseRequest, DeleteLicenseResponse> {

    private final LicensesManagerService licensesManagerService;

    @Inject
    public TransportDeleteLicenseAction(Settings settings, TransportService transportService, ClusterService clusterService, LicensesManagerService licensesManagerService,
                                        ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, DeleteLicenseAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.licensesManagerService = licensesManagerService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected DeleteLicenseRequest newRequest() {
        return new DeleteLicenseRequest();
    }

    @Override
    protected DeleteLicenseResponse newResponse() {
        return new DeleteLicenseResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteLicenseRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(final DeleteLicenseRequest request, ClusterState state, final ActionListener<DeleteLicenseResponse> listener) throws ElasticsearchException {
        final DeleteLicenseRequestHolder requestHolder = new DeleteLicenseRequestHolder(request, "delete licenses []");
        licensesManagerService.removeLicenses(requestHolder, new ActionListener<ClusterStateUpdateResponse>() {
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