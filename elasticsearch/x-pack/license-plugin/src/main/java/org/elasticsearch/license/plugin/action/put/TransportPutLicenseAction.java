/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.license.plugin.core.LicensesService.LicensesUpdateResponse;

public class TransportPutLicenseAction extends TransportMasterNodeAction<PutLicenseRequest, PutLicenseResponse> {

    private final LicensesService licensesService;

    @Inject
    public TransportPutLicenseAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     LicensesService licensesService, ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutLicenseAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                PutLicenseRequest::new);
        this.licensesService = licensesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PutLicenseResponse newResponse() {
        return new PutLicenseResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutLicenseRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, "");
    }

    @Override
    protected void masterOperation(final PutLicenseRequest request, ClusterState state, final ActionListener<PutLicenseResponse>
            listener) throws ElasticsearchException {
        licensesService.registerLicense(request, new ActionListener<LicensesUpdateResponse>() {
            @Override
            public void onResponse(LicensesUpdateResponse licensesUpdateResponse) {
                listener.onResponse(new PutLicenseResponse(licensesUpdateResponse.isAcknowledged(), licensesUpdateResponse.status(),
                        licensesUpdateResponse.acknowledgementHeader(), licensesUpdateResponse.acknowledgeMessages()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}
