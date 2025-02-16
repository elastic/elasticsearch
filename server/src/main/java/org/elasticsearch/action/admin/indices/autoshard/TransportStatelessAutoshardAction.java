/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.autoshard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataAutoshardIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportStatelessAutoshardAction extends TransportMasterNodeAction<AutoshardIndexRequest, AutoshardIndexResponse> {
    public static final ActionType<AutoshardIndexResponse> TYPE = new ActionType<>("indices:admin/autoshard");
    private static final Logger logger = LogManager.getLogger(TransportStatelessAutoshardAction.class);

    private final MetadataAutoshardIndexService autoshardIndexService;

    @Inject
    public TransportStatelessAutoshardAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataAutoshardIndexService autoshardIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AutoshardIndexRequest::new,
            AutoshardIndexResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.autoshardIndexService = autoshardIndexService;
    }

    @Override
    protected ClusterBlockException checkBlock(AutoshardIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.indices());
    }

    @Override
    protected void masterOperation(
        Task task,
        final AutoshardIndexRequest request,
        final ClusterState state,
        final ActionListener<AutoshardIndexResponse> listener
    ) {
        final AutoshardIndexClusterStateUpdateRequest updateRequest = buildUpdateRequest(request);
        final String indexName = request.index();

        autoshardIndexService.autoshardIndex(
            request.masterNodeTimeout(),
            request.ackTimeout(),
            request.ackTimeout(),
            updateRequest,
            listener.map(response -> new AutoshardIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged()))
        );
    }

    private AutoshardIndexClusterStateUpdateRequest buildUpdateRequest(AutoshardIndexRequest request) {
        return new AutoshardIndexClusterStateUpdateRequest(request.cause(), request.index());
    }
}
