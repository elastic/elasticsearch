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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoResponse;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.IndexUpgradeService;

import java.util.Map;

public class TransportIndexUpgradeInfoAction
    extends TransportMasterNodeReadAction<IndexUpgradeInfoRequest, IndexUpgradeInfoResponse> {

    private final IndexUpgradeService indexUpgradeService;
    private final XPackLicenseState licenseState;


    @Inject
    public TransportIndexUpgradeInfoAction(TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexUpgradeService indexUpgradeService,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           XPackLicenseState licenseState) {
        super(IndexUpgradeInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
            IndexUpgradeInfoRequest::new, indexNameExpressionResolver);
        this.indexUpgradeService = indexUpgradeService;
        this.licenseState = licenseState;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected IndexUpgradeInfoResponse newResponse() {
        return new IndexUpgradeInfoResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(IndexUpgradeInfoRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(final IndexUpgradeInfoRequest request, ClusterState state,
                                         final ActionListener<IndexUpgradeInfoResponse> listener) {
        if (licenseState.isUpgradeAllowed()) {
            Map<String, UpgradeActionRequired> results =
                    indexUpgradeService.upgradeInfo(request.indices(), request.indicesOptions(), state);
            listener.onResponse(new IndexUpgradeInfoResponse(results));
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.UPGRADE));
        }
    }
}
