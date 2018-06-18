/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.IndexUpgradeService;
import org.elasticsearch.xpack.core.upgrade.UpgradeActionRequired;

import java.util.Map;

public class TransportIndexUpgradeInfoAction extends TransportMasterNodeReadAction<IndexUpgradeInfoAction.Request,
        IndexUpgradeInfoAction.Response> {

    private final IndexUpgradeService indexUpgradeService;
    private final XPackLicenseState licenseState;


    @Inject
    public TransportIndexUpgradeInfoAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexUpgradeService indexUpgradeService,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           XPackLicenseState licenseState) {
        super(settings, IndexUpgradeInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
            IndexUpgradeInfoAction.Request::new, indexNameExpressionResolver);
        this.indexUpgradeService = indexUpgradeService;
        this.licenseState = licenseState;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected IndexUpgradeInfoAction.Response newResponse() {
        return new IndexUpgradeInfoAction.Response();
    }

    @Override
    protected ClusterBlockException checkBlock(IndexUpgradeInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(final IndexUpgradeInfoAction.Request request, ClusterState state,
                                         final ActionListener<IndexUpgradeInfoAction.Response> listener) {
        if (licenseState.isUpgradeAllowed()) {
            Map<String, UpgradeActionRequired> results =
                    indexUpgradeService.upgradeInfo(request.indices(), request.indicesOptions(), state);
            listener.onResponse(new IndexUpgradeInfoAction.Response(results));
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.UPGRADE));
        }
    }
}
