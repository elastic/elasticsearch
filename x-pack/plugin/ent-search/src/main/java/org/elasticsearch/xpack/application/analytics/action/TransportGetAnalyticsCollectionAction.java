/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
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
public class TransportGetAnalyticsCollectionAction extends TransportMasterNodeReadAction<
    GetAnalyticsCollectionAction.Request,
    GetAnalyticsCollectionAction.Response> {

    private final AnalyticsCollectionService analyticsCollectionService;

    @Inject
    public TransportGetAnalyticsCollectionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        AnalyticsCollectionService analyticsCollectionService
    ) {
        super(
            GetAnalyticsCollectionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAnalyticsCollectionAction.Request::new,
            GetAnalyticsCollectionAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.analyticsCollectionService = analyticsCollectionService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetAnalyticsCollectionAction.Request request,
        ClusterState state,
        ActionListener<GetAnalyticsCollectionAction.Response> listener
    ) {
        DeprecationLogger.getLogger(TransportDeleteAnalyticsCollectionAction.class)
            .warn(DeprecationCategory.API, BEHAVIORAL_ANALYTICS_API_ENDPOINT, BEHAVIORAL_ANALYTICS_DEPRECATION_MESSAGE);
        analyticsCollectionService.getAnalyticsCollection(state, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(GetAnalyticsCollectionAction.Request request, ClusterState state) {
        return null;
    }

}
