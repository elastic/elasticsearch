/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class GetSearchEngineTransportAction extends TransportMasterNodeReadAction<
    GetSearchEngineAction.Request,
    GetSearchEngineAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(GetSearchEngineTransportAction.class);

    @Inject
    public GetSearchEngineTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSearchEngineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSearchEngineAction.Request::new,
            indexNameExpressionResolver,
            GetSearchEngineAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetSearchEngineAction.Request request,
        ClusterState state,
        ActionListener<GetSearchEngineAction.Response> listener) {
        List<SearchEngine> searchEngines = getSearchEngines(request, state);
        listener.onResponse(new GetSearchEngineAction.Response(searchEngines));
    }

    private List<SearchEngine> getSearchEngines(GetSearchEngineAction.Request request, ClusterState clusterState) {
        List<String> results = getSearchEnginesNames(
            indexNameExpressionResolver,
            clusterState,
            request.getNames(),
            request.indicesOptions()
        );
        Map<String, SearchEngine> contentIndices = clusterState.metadata().searchEngines();

        return results.stream().map(contentIndices::get).sorted(Comparator.comparing(SearchEngine::getName)).toList();
    }

    @Override
    protected ClusterBlockException checkBlock(GetSearchEngineAction.Request request, ClusterState state) {
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
