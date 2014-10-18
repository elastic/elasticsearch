/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.license.plugin.core.trial.TrialLicenseUtils;
import org.elasticsearch.license.plugin.core.trial.TrialLicenses;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetLicenseAction extends TransportMasterNodeReadOperationAction<GetLicenseRequest, GetLicenseResponse> {

    private final LicensesService licensesService;

    @Inject
    public TransportGetLicenseAction(Settings settings, TransportService transportService, ClusterService clusterService, LicensesService licensesService,
                                          ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, GetLicenseAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.licensesService = licensesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected GetLicenseRequest newRequest() {
        return new GetLicenseRequest();
    }

    @Override
    protected GetLicenseResponse newResponse() {
        return new GetLicenseResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(GetLicenseRequest request, ClusterState state) {
        //TODO: do the right checkBlock
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(final GetLicenseRequest request, ClusterState state, final ActionListener<GetLicenseResponse> listener) throws ElasticsearchException {
        //TODO: Move this functionality to license service
        MetaData metaData = state.metaData();
        LicensesMetaData licenses = metaData.custom(LicensesMetaData.TYPE);
        if (licenses != null) {
            ESLicenseManager esLicenseManager = licensesService.getEsLicenseManager();
            ESLicenses esLicenses = esLicenseManager.fromSignaturesAsIs(licenses.getSignatures());
            TrialLicenses trialLicenses = TrialLicenseUtils.fromEncodedTrialLicenses(licenses.getEncodedTrialLicenses());
            listener.onResponse(new GetLicenseResponse(esLicenses, trialLicenses));
        } else {
            listener.onResponse(new GetLicenseResponse());
        }
    }
}