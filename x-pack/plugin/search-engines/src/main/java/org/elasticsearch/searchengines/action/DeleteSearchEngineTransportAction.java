/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
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

import java.util.EnumSet;
import java.util.List;

public class DeleteSearchEngineTransportAction extends AcknowledgedTransportMasterNodeAction<DeleteSearchEngineAction.Request> {

    private final SearchEngineMetadataService searchEngineMetadataService;

    @Inject
    public DeleteSearchEngineTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchEngineMetadataService searchEngineMetadataService
    ) {
        super(
            DeleteSearchEngineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteSearchEngineAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );

        this.searchEngineMetadataService = searchEngineMetadataService;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteSearchEngineAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        List<String> engineNames = getSearchEnginesNames(indexNameExpressionResolver, state, request.getNames(), request.indicesOptions());
        request.setResolved(engineNames);
        searchEngineMetadataService.deleteSearchEngine(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSearchEngineAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static List<String> getSearchEnginesNames(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState currentState,
        String[] names,
        IndicesOptions indicesOptions
    ) {
        indicesOptions = updateIndicesOptions(indicesOptions);
        return indexNameExpressionResolver.searchEngineNames(currentState, indicesOptions, names);
    }

    private static IndicesOptions updateIndicesOptions(IndicesOptions indicesOptions) {
        EnumSet<IndicesOptions.WildcardStates> expandWildcards = indicesOptions.expandWildcards();
        expandWildcards.add(IndicesOptions.WildcardStates.OPEN);
        indicesOptions = new IndicesOptions(indicesOptions.options(), expandWildcards);

        return indicesOptions;
    }
}
