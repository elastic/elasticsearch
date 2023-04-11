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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionService;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

public class TransportGetAnalyticsCollectionAction extends TransportMasterNodeReadAction<
    GetAnalyticsCollectionAction.Request,
    GetAnalyticsCollectionAction.Response> {

    private final AnalyticsCollectionService analyticsCollectionService;

    private final XPackLicenseState licenseState;

    @Inject
    public TransportGetAnalyticsCollectionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AnalyticsCollectionService analyticsCollectionService,
        XPackLicenseState licenseState
    ) {
        super(
            GetAnalyticsCollectionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAnalyticsCollectionAction.Request::new,
            indexNameExpressionResolver,
            GetAnalyticsCollectionAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.analyticsCollectionService = analyticsCollectionService;
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetAnalyticsCollectionAction.Request request,
        ClusterState state,
        ActionListener<GetAnalyticsCollectionAction.Response> listener
    ) {
        LicenseUtils.runIfSupportedLicense(
            licenseState,
            () -> analyticsCollectionService.getAnalyticsCollection(state, request, listener),
            listener::onFailure
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetAnalyticsCollectionAction.Request request, ClusterState state) {
        return null;
    }

}
