/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionService;

import static org.elasticsearch.xpack.application.EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT;
import static org.elasticsearch.xpack.application.EnterpriseSearch.BEHAVIORAL_ANALYTICS_DEPRECATION_MESSAGE;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class TransportPutAnalyticsCollectionAction extends TransportMasterNodeAction<
    PutAnalyticsCollectionAction.Request,
    PutAnalyticsCollectionAction.Response> {

    private final AnalyticsCollectionService analyticsCollectionService;

    @Inject
    public TransportPutAnalyticsCollectionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        AnalyticsCollectionService analyticsCollectionService,
        FeatureService featureService
    ) {
        super(
            PutAnalyticsCollectionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutAnalyticsCollectionAction.Request::new,
            PutAnalyticsCollectionAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.analyticsCollectionService = analyticsCollectionService;
    }

    @Override
    protected ClusterBlockException checkBlock(PutAnalyticsCollectionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutAnalyticsCollectionAction.Request request,
        ClusterState state,
        ActionListener<PutAnalyticsCollectionAction.Response> listener
    ) {
        DeprecationLogger.getLogger(TransportDeleteAnalyticsCollectionAction.class)
            .warn(DeprecationCategory.API, BEHAVIORAL_ANALYTICS_API_ENDPOINT, BEHAVIORAL_ANALYTICS_DEPRECATION_MESSAGE);
        analyticsCollectionService.putAnalyticsCollection(state, request, listener);
    }

}
