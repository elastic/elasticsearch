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
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Explain transport action. Computes the explain on the targeted shard.
 */
// TODO: AggregatedDfs. Currently the idf can be different then when executing a normal search with explain.
public class TransportExplainAction extends TransportSingleShardAction<ExplainRequest, ExplainResponse> {

    private final SearchService searchService;

    @Inject
    public TransportExplainAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, SearchService searchService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ExplainAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                ExplainRequest::new, ThreadPool.Names.GET);
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    @Override
    protected boolean resolveIndex(ExplainRequest request) {
        return true;
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        final AliasFilter aliasFilter = searchService.buildAliasFilter(state, request.concreteIndex(),
            request.request().index());
        request.request().filteringAlias(aliasFilter);
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetaData().routingRequired(request.concreteIndex(), request.request().type())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
        }
    }

    @Override
    protected ExplainResponse shardOperation(ExplainRequest request, ShardId shardId) throws IOException {
        ShardSearchLocalRequest shardSearchLocalRequest = new ShardSearchLocalRequest(shardId,
            new String[]{request.type()}, request.nowInMillis, request.filteringAlias());
        SearchContext context = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        Engine.GetResult result = null;
        try {
            Term uidTerm = context.mapperService().createUidTerm(request.type(), request.id());
            if (uidTerm == null) {
                return new ExplainResponse(shardId.getIndexName(), request.type(), request.id(), false);
            }
            result = context.indexShard().get(new Engine.Get(false, request.type(), request.id(), uidTerm));
            if (!result.exists()) {
                return new ExplainResponse(shardId.getIndexName(), request.type(), request.id(), false);
            }
            context.parsedQuery(context.getQueryShardContext().toQuery(request.query()));
            context.preProcess(true);
            int topLevelDocId = result.docIdAndVersion().docId + result.docIdAndVersion().context.docBase;
            Explanation explanation = context.searcher().explain(context.query(), topLevelDocId);
            for (RescoreContext ctx : context.rescore()) {
                Rescorer rescorer = ctx.rescorer();
                explanation = rescorer.explain(topLevelDocId, context.searcher(), ctx, explanation);
            }
            if (request.storedFields() != null || (request.fetchSourceContext() != null && request.fetchSourceContext().fetchSource())) {
                // Advantage is that we're not opening a second searcher to retrieve the _source. Also
                // because we are working in the same searcher in engineGetResult we can be sure that a
                // doc isn't deleted between the initial get and this call.
                GetResult getResult = context.indexShard().getService().get(result, request.id(), request.type(), request.storedFields(), request.fetchSourceContext());
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
    protected ExplainResponse newResponse() {
        return new ExplainResponse();
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting().getShards(
                clusterService.state(), request.concreteIndex(), request.request().id(), request.request().routing(), request.request().preference()
        );
    }
}
