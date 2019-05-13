/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.explain;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;

/**
 * Explain transport action. Computes the explain on the targeted shard.
 */
// TODO: AggregatedDfs. Currently the idf can be different then when executing a normal search with explain.
public class TransportExplainAction extends TransportSingleShardAction<ExplainRequest, ExplainResponse> {

    private final SearchService searchService;

    @Inject
    public TransportExplainAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  SearchService searchService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ExplainAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                ExplainRequest::new, ThreadPool.Names.GET);
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ExplainRequest request, ActionListener<ExplainResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(task, request, listener);
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
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetaData().routingRequired(request.concreteIndex())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
        }
    }

    @Override
    protected void asyncShardOperation(ExplainRequest request, ShardId shardId,
                                       ActionListener<ExplainResponse> listener) throws IOException {
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
        ShardSearchLocalRequest shardSearchLocalRequest = new ShardSearchLocalRequest(shardId, request.nowInMillis,
                request.filteringAlias());
        SearchContext context = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        Engine.GetResult result = null;
        try {
            // No need to check the type, IndexShard#get does it for us
            Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(request.id()));
            result = context.indexShard().get(new Engine.Get(false, false, request.type(), request.id(), uidTerm));
            if (!result.exists()) {
                return new ExplainResponse(shardId.getIndexName(), request.type(), request.id(), false);
            }
            context.parsedQuery(context.getQueryShardContext().toQuery(request.query()));
            context.preProcess(true);
            int topLevelDocId = result.docIdAndVersion().docId + result.docIdAndVersion().docBase;
            Explanation explanation = context.searcher().explain(context.query(), topLevelDocId);
            for (RescoreContext ctx : context.rescore()) {
                Rescorer rescorer = ctx.rescorer();
                explanation = rescorer.explain(topLevelDocId, context.searcher(), ctx, explanation);
            }
            if (request.storedFields() != null || (request.fetchSourceContext() != null && request.fetchSourceContext().fetchSource())) {
                // Advantage is that we're not opening a second searcher to retrieve the _source. Also
                // because we are working in the same searcher in engineGetResult we can be sure that a
                // doc isn't deleted between the initial get and this call.
                GetResult getResult = context.indexShard().getService().get(result, request.id(), request.type(), request.storedFields(),
                    request.fetchSourceContext());
                return new ExplainResponse(shardId.getIndexName(), request.type(), request.id(), true, explanation, getResult);
            } else {
                return new ExplainResponse(shardId.getIndexName(), request.type(), request.id(), true, explanation);
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
        return clusterService.operationRouting().getShards(
                clusterService.state(), request.concreteIndex(), request.request().id(), request.request().routing(),
            request.request().preference()
        );
    }

    @Override
    protected String getExecutor(ExplainRequest request, ShardId shardId) {
        IndexService indexService = searchService.getIndicesService().indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled() ? ThreadPool.Names.SEARCH_THROTTLED : super.getExecutor(request,
            shardId);
    }
}
