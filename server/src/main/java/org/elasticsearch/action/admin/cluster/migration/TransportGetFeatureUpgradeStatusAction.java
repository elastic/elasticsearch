/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport class for the get feature upgrade status action
 */
public class TransportGetFeatureUpgradeStatusAction extends TransportMasterNodeAction<
        GetFeatureUpgradeStatusRequest,
        GetFeatureUpgradeStatusResponse> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetFeatureUpgradeStatusAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetFeatureUpgradeStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetFeatureUpgradeStatusRequest::new,
            indexNameExpressionResolver,
            GetFeatureUpgradeStatusResponse::new,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(Task task, GetFeatureUpgradeStatusRequest request, ClusterState state,
                                   ActionListener<GetFeatureUpgradeStatusResponse> listener) throws Exception {
        List<GetFeatureUpgradeStatusResponse.IndexVersion> indexVersions = new ArrayList<>();
        indexVersions.add(new GetFeatureUpgradeStatusResponse.IndexVersion(".security-7", "7.1.1"));
        List<GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus> features = new ArrayList<>();
        features.add(new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
            "security",
            "7.1.1",
            "UPGRADE_NEEDED",
            indexVersions
        ));
        listener.onResponse(new GetFeatureUpgradeStatusResponse(features, "UPGRADE_NEEDED"));
    }

    @Override
    protected ClusterBlockException checkBlock(GetFeatureUpgradeStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
