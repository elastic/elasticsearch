/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.sql.action.compute.planner.PlanNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * For simplicity, we run this on a single local shard for now
 */
public class TransportComputeAction extends TransportAction<ComputeRequest, ComputeResponse> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SearchService searchService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    @Inject
    public TransportComputeAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(ComputeAction.NAME, actionFilters, transportService.getTaskManager());
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.searchService = searchService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ComputeRequest request, ActionListener<ComputeResponse> listener) {
        try {
            asyncAction(task, request, listener);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void asyncAction(Task task, ComputeRequest request, ActionListener<ComputeResponse> listener) throws IOException {
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), request);
        List<SearchContext> searchContexts = new ArrayList<>();
        for (Index index : indices) {
            IndexService indexService = searchService.getIndicesService().indexServiceSafe(index);
            for (IndexShard indexShard : indexService) {
                ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(indexShard.shardId(), 0, AliasFilter.EMPTY);
                SearchContext context = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
                searchContexts.add(context);
            }
        }

        boolean success = false;
        try {
            searchContexts.stream().forEach(SearchContext::preProcess);

            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                searchContexts.stream()
                    .map(SearchContext::getSearchExecutionContext)
                    .map(
                        sec -> new LocalExecutionPlanner.IndexReaderReference(
                            sec.getIndexReader(),
                            new ShardId(sec.index(), sec.getShardId())
                        )
                    )
                    .collect(Collectors.toList())
            );

            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(
                new PlanNode.OutputNode(request.plan(), (l, p) -> request.getPageConsumer().accept(p))
            );
            Driver.start(threadPool.executor(ThreadPool.Names.SEARCH), localExecutionPlan.createDrivers())
                .addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        Releasables.close(searchContexts);
                        listener.onResponse(new ComputeResponse());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Releasables.close(searchContexts);
                        listener.onFailure(e);
                    }
                });
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(searchContexts);
            }
        }
    }
}
