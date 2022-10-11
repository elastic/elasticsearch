/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
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
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.analyzer.Avg;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.session.Configuration;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportEsqlQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlQueryResponse> {

    private final PlanExecutor planExecutor;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SearchService searchService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    @Inject
    public TransportEsqlQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchService searchService,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        super(EsqlQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new);
        this.planExecutor = planExecutor;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.searchService = searchService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        // TODO: create more realistic function registry
        FunctionRegistry functionRegistry = new FunctionRegistry(FunctionRegistry.def(Avg.class, Avg::new, "AVG"));
        Configuration configuration = new Configuration(
            request.zoneId() != null ? request.zoneId() : ZoneOffset.UTC,
            null,
            null,
            x -> Collections.emptySet()
        );
        new EsqlSession(planExecutor.indexResolver(), functionRegistry, configuration).execute(request.query(), ActionListener.wrap(r -> {
            runCompute(r, listener.map(pages -> {
                List<ColumnInfo> columns = r.output().stream().map(c -> new ColumnInfo(c.qualifiedName(), c.dataType().esType())).toList();
                return new EsqlQueryResponse(columns, pagesToValues(pages));
            }));
        }, listener::onFailure));
    }

    private List<List<Object>> pagesToValues(List<Page> pages) {
        List<List<Object>> result = new ArrayList<>();
        for (Page page : pages) {
            for (int i = 0; i < page.getPositionCount(); i++) {
                List<Object> row = new ArrayList<>(page.getBlockCount());
                for (int b = 0; b < page.getBlockCount(); b++) {
                    Block block = page.getBlock(b);
                    row.add(block.getObject(i));
                }
                result.add(row);
            }
        }
        return result;
    }

    private void runCompute(PhysicalPlan physicalPlan, ActionListener<List<Page>> listener) throws IOException {
        Set<String> indexNames = physicalPlan.collect(l -> l instanceof EsQueryExec)
            .stream()
            .map(qe -> ((EsQueryExec) qe).index().name())
            .collect(Collectors.toSet());
        Index[] indices = indexNameExpressionResolver.concreteIndices(
            clusterService.state(),
            IndicesOptions.STRICT_EXPAND_OPEN,
            indexNames.toArray(String[]::new)
        );
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

            final List<Page> results = Collections.synchronizedList(new ArrayList<>());
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(
                new OutputExec(physicalPlan, (l, p) -> { results.add(p); })
            );
            List<Driver> drivers = localExecutionPlan.createDrivers();
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            logger.info("using {} drivers", drivers.size());
            Driver.start(threadPool.executor(ThreadPool.Names.SEARCH), drivers).addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    Releasables.close(searchContexts);
                    listener.onResponse(new ArrayList<>(results));
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
