/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

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
import org.elasticsearch.license.plugin.core.ElasticsearchLicenseException;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.license.plugin.core.LicensesService.PutLicenseRequestHolder;

public class TransportPutLicenseAction extends TransportMasterNodeOperationAction<PutLicenseRequest, PutLicenseResponse> {

    private final LicensesManagerService licensesManagerService;

    @Inject
    public TransportPutLicenseAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     LicensesManagerService licensesManagerService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, PutLicenseAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.licensesManagerService = licensesManagerService;
    }


    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutLicenseRequest newRequest() {
        return new PutLicenseRequest();
    }

    @Override
    protected PutLicenseResponse newResponse() {
        return new PutLicenseResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutLicenseRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(final PutLicenseRequest request, ClusterState state, final ActionListener<PutLicenseResponse> listener) throws ElasticsearchException {
        final PutLicenseRequestHolder requestHolder = new PutLicenseRequestHolder(request, "put licenses []");
        LicensesStatus status = licensesManagerService.registerLicenses(requestHolder, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new PutLicenseResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });

        switch (status) {
            case VALID:
                return;
            case INVALID:
                throw new ElasticsearchLicenseException("Found Invalid License(s)");
            case EXPIRED:
                throw new ElasticsearchLicenseException("Found Expired License(s)");
        }
    }

}
