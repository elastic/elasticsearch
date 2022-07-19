/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSink;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSource;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.exchange.PassthroughExchanger;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;

import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * For simplicity, we run this on a single local shard for now
 */
public class TransportComputeAction extends TransportSingleShardAction<ComputeRequest, ComputeResponse> {

    private final SearchService searchService;

    @Inject
    public TransportComputeAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ComputeAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ComputeRequest::new,
            ThreadPool.Names.SEARCH
        );
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ComputeRequest request, ActionListener<ComputeResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        ActionListener<QueryBuilder> rewriteListener = ActionListener.wrap(rewrittenQuery -> {
            request.query(rewrittenQuery);
            super.doExecute(task, request, listener);
        }, listener::onFailure);

        assert request.query() != null;
        LongSupplier timeProvider = () -> request.nowInMillis;
        Rewriteable.rewriteAndFetch(request.query(), searchService.getRewriteContext(timeProvider), rewriteListener);
    }

    @Override
    protected void asyncShardOperation(ComputeRequest request, ShardId shardId, ActionListener<ComputeResponse> listener) {
        IndexService indexService = searchService.getIndicesService().indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        indexShard.awaitShardSearchActive(b -> {
            try {
                threadPool.executor(getExecutor(request, shardId)).execute(new ActionRunnable<>(listener) {
                    @Override
                    protected void doRun() throws Exception {
                        runCompute(request, shardId, listener);
                    }
                });
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        });
    }

    private void runCompute(ComputeRequest request, ShardId shardId, ActionListener<ComputeResponse> listener) throws IOException {
        ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(shardId, request.nowInMillis, AliasFilter.EMPTY);
        SearchContext context = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        boolean success = false;
        try {

            ExchangeSource luceneExchangeSource = new ExchangeSource();
            LuceneCollector luceneCollector = new LuceneCollector(
                new ExchangeSink(new PassthroughExchanger(luceneExchangeSource, 1), sink -> luceneExchangeSource.finish())
            );

            // TODO: turn aggs into operator chain and pass to driver
            Aggs aggs = request.aggs;

            // only release search context once driver actually completed
            Driver driver = new Driver(List.of(
                new ExchangeSourceOperator(luceneExchangeSource),
                new NumericDocValuesExtractor(context.getSearchExecutionContext().getIndexReader(), 0, 1, "count"),
                new LongTransformer(2, i -> i + 1),
                new LongGroupingOperator(3, BigArrays.NON_RECYCLING_INSTANCE),
                new LongMaxOperator(4),
                new PageConsumerOperator(request.getPageConsumer())),
                () -> Releasables.close(context));

            threadPool.generic().execute(driver);

            listener.onResponse(new ComputeResponse());

            context.parsedQuery(context.getSearchExecutionContext().toQuery(request.query()));
            context.size(0); // no hits needed
            context.preProcess();

            context.queryCollectors().put(TransportComputeAction.class, luceneCollector);
            // run query, invoking collector
            QueryPhase.execute(context);
            luceneCollector.finish();
            success = true;
        } finally {
            context.queryCollectors().remove(TransportComputeAction.class);
            if (success == false) {
                Releasables.close(context);
            }
        }
    }

    @Override
    protected ComputeResponse shardOperation(ComputeRequest request, ShardId shardId) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Writeable.Reader<ComputeResponse> getResponseReader() {
        return ComputeResponse::new;
    }

    @Override
    protected boolean resolveIndex(ComputeRequest request) {
        return true;
    }

    @Override
    protected ShardsIterator shards(
        ClusterState state,
        TransportSingleShardAction<ComputeRequest, ComputeResponse>.InternalRequest request
    ) {
        return clusterService.operationRouting().getShards(clusterService.state(), request.concreteIndex(), 0, "_only_local");
    }
}
