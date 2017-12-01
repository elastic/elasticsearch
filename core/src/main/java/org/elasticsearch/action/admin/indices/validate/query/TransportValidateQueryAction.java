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

package org.elasticsearch.action.admin.indices.validate.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportValidateQueryAction extends TransportBroadcastAction<ValidateQueryRequest, ValidateQueryResponse, ShardValidateQueryRequest, ShardValidateQueryResponse> {

    private final SearchService searchService;

    @Inject
    public TransportValidateQueryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
            TransportService transportService, SearchService searchService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ValidateQueryAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ValidateQueryRequest::new, ShardValidateQueryRequest::new, ThreadPool.Names.SEARCH);
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(task, request, listener);
    }

    @Override
    protected ShardValidateQueryRequest newShardRequest(int numShards, ShardRouting shard, ValidateQueryRequest request) {
        final AliasFilter aliasFilter = searchService.buildAliasFilter(clusterService.state(), shard.getIndexName(),
            request.indices());
        return new ShardValidateQueryRequest(shard.shardId(), aliasFilter, request);
    }

    @Override
    protected ShardValidateQueryResponse newShardResponse() {
        return new ShardValidateQueryResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, ValidateQueryRequest request, String[] concreteIndices) {
        final String routing;
        if (request.allShards()) {
            routing = null;
        } else {
            // Random routing to limit request to a single shard
            routing = Integer.toString(Randomness.get().nextInt(1000));
        }
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, routing, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, "_local");
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ValidateQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ValidateQueryRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected ValidateQueryResponse newResponse(ValidateQueryRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        boolean valid = true;
        List<ShardOperationFailedException> shardFailures = null;
        List<QueryExplanation> queryExplanations = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                ShardValidateQueryResponse validateQueryResponse = (ShardValidateQueryResponse) shardResponse;
                valid = valid && validateQueryResponse.isValid();
                if (request.explain() || request.rewrite() || request.allShards()) {
                    if (queryExplanations == null) {
                        queryExplanations = new ArrayList<>();
                    }
                    queryExplanations.add(new QueryExplanation(
                            validateQueryResponse.getIndex(),
                            request.allShards() ? validateQueryResponse.getShardId().getId() : QueryExplanation.RANDOM_SHARD,
                            validateQueryResponse.isValid(),
                            validateQueryResponse.getExplanation(),
                            validateQueryResponse.getError()
                    ));
                }
                successfulShards++;
            }
        }
        return new ValidateQueryResponse(valid, queryExplanations, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardValidateQueryResponse shardOperation(ShardValidateQueryRequest request) throws IOException {
        boolean valid;
        String explanation = null;
        String error = null;
        ShardSearchLocalRequest shardSearchLocalRequest = new ShardSearchLocalRequest(request.shardId(), request.types(),
            request.nowInMillis(), request.filteringAliases());
        SearchContext searchContext = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        try {
            ParsedQuery parsedQuery = searchContext.getQueryShardContext().toQuery(request.query());
            searchContext.parsedQuery(parsedQuery);
            searchContext.preProcess(request.rewrite());
            valid = true;
            explanation = explain(searchContext, request.rewrite());
        } catch (QueryShardException|ParsingException e) {
            valid = false;
            error = e.getDetailedMessage();
        } catch (AssertionError|IOException e) {
            valid = false;
            error = e.getMessage();
        } finally {
            Releasables.close(searchContext);
        }

        return new ShardValidateQueryResponse(request.shardId(), valid, explanation, error);
    }

    private String explain(SearchContext context, boolean rewritten) throws IOException {
        Query query = context.query();
        if (rewritten && query instanceof MatchNoDocsQuery) {
            return context.parsedQuery().query().toString();
        } else {
            return query.toString();
        }
    }
}
