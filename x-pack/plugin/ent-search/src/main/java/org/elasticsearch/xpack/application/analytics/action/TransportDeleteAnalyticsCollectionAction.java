/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionService;

public class TransportDeleteAnalyticsCollectionAction extends AcknowledgedTransportMasterNodeAction<
    DeleteAnalyticsCollectionAction.Request> {

    private final AnalyticsCollectionService analyticsCollectionService;

    @Inject
    public TransportDeleteAnalyticsCollectionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AnalyticsCollectionService analyticsCollectionService
    ) {
        super(
            DeleteAnalyticsCollectionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteAnalyticsCollectionAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.analyticsCollectionService = analyticsCollectionService;
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteAnalyticsCollectionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteAnalyticsCollectionAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        analyticsCollectionService.deleteAnalyticsCollection(state, request, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
