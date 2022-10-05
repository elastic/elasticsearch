/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.action;

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
import org.elasticsearch.searchengines.SearchEngineMetadataService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class PutSearchEngineTransportAction extends AcknowledgedTransportMasterNodeAction<PutSearchEngineAction.Request> {

    private final SearchEngineMetadataService searchEngineMetadataService;

    @Inject
    public PutSearchEngineTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchEngineMetadataService searchEngineMetadataService
    ) {
        super(
            PutSearchEngineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutSearchEngineAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );

        this.searchEngineMetadataService = searchEngineMetadataService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutSearchEngineAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        searchEngineMetadataService.putSearchEngine(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutSearchEngineAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
