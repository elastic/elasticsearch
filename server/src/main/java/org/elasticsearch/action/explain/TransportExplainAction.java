/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Explain transport action. Computes the explain on the targeted shard.
 */
// TODO: AggregatedDfs. Currently the idf can be different then when executing a normal search with explain.
public class TransportExplainAction extends TransportSingleShardAction<ExplainRequest, ExplainResponse> {

    private final SearchService searchService;

    @Inject
    public TransportExplainAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ExplainAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ExplainRequest::new,
            ThreadPool.Names.GET
        );
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ExplainRequest request, ActionListener<ExplainResponse> listener) {
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
    protected boolean resolveIndex(ExplainRequest request) {
        return true;
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(state, request.request().index());
        final AliasFilter aliasFilter = searchService.buildAliasFilter(state, request.concreteIndex(), indicesAndAliases);
        request.request().filteringAlias(aliasFilter);
    }

    @Override
    protected void asyncShardOperation(ExplainRequest request, ShardId shardId, ActionListener<ExplainResponse> listener)
        throws IOException {
        IndexService indexService = searchService.getIndicesService().indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        indexShard.awaitShardSearchActive(b -> {
            try {
                super.asyncShardOperation(request, shardId, listener);
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        });
    }

    @Override
    protected ExplainResponse shardOperation(ExplainRequest request, ShardId shardId) throws IOException {
        ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(shardId, request.nowInMillis, request.filteringAlias());
        SearchContext context = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        Engine.GetResult result = null;
        try {
            // No need to check the type, IndexShard#get does it for us
            result = context.indexShard().get(new Engine.Get(false, false, request.id()));
            if (result.exists() == false) {
                return new ExplainResponse(shardId.getIndexName(), request.id(), false);
            }
            context.parsedQuery(context.getSearchExecutionContext().toQuery(request.query()));
            context.preProcess();
            int topLevelDocId = result.docIdAndVersion().docId + result.docIdAndVersion().docBase;
            Explanation explanation = context.searcher().explain(context.rewrittenQuery(), topLevelDocId);
            for (RescoreContext ctx : context.rescore()) {
                Rescorer rescorer = ctx.rescorer();
                explanation = rescorer.explain(topLevelDocId, context.searcher(), ctx, explanation);
            }
            if (request.storedFields() != null || (request.fetchSourceContext() != null && request.fetchSourceContext().fetchSource())) {
                // Advantage is that we're not opening a second searcher to retrieve the _source. Also
                // because we are working in the same searcher in engineGetResult we can be sure that a
                // doc isn't deleted between the initial get and this call.
                GetResult getResult = context.indexShard()
                    .getService()
                    .get(result, request.id(), request.storedFields(), request.fetchSourceContext());
                return new ExplainResponse(shardId.getIndexName(), request.id(), true, explanation, getResult);
            } else {
                return new ExplainResponse(shardId.getIndexName(), request.id(), true, explanation);
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Could not explain", e);
        } finally {
            Releasables.close(result, context);
        }
    }

    @Override
    protected Writeable.Reader<ExplainResponse> getResponseReader() {
        return ExplainResponse::new;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
            .getShards(
                clusterService.state(),
                request.concreteIndex(),
                request.request().id(),
                request.request().routing(),
                request.request().preference()
            );
    }

    @Override
    protected String getExecutor(ExplainRequest request, ShardId shardId) {
        IndexService indexService = searchService.getIndicesService().indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled()
            ? ThreadPool.Names.SEARCH_THROTTLED
            : super.getExecutor(request, shardId);
    }
}
