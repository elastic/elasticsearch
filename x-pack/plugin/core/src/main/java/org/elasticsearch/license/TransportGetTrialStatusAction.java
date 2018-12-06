/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetTrialStatusAction extends TransportMasterNodeReadAction<GetTrialStatusRequest, GetTrialStatusResponse> {

    @Inject
    public TransportGetTrialStatusAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetTrialStatusAction.NAME, transportService, clusterService, threadPool, actionFilters,
                GetTrialStatusRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetTrialStatusResponse newResponse() {
        return new GetTrialStatusResponse();
    }

    @Override
    protected void masterOperation(GetTrialStatusRequest request, ClusterState state,
                                   ActionListener<GetTrialStatusResponse> listener) throws Exception {
        LicensesMetaData licensesMetaData = state.metaData().custom(LicensesMetaData.TYPE);
        listener.onResponse(new GetTrialStatusResponse(licensesMetaData == null || licensesMetaData.isEligibleForTrial()));

    }

    @Override
    protected ClusterBlockException checkBlock(GetTrialStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
