/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.LongSupplier;

public class TransportValidateQueryAction extends TransportBroadcastAction<
        ValidateQueryRequest,
        ValidateQueryResponse,
        ShardValidateQueryRequest,
        ShardValidateQueryResponse> {

    private final SearchService searchService;

    @Inject
    public TransportValidateQueryAction(ClusterService clusterService,
            TransportService transportService, SearchService searchService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ValidateQueryAction.NAME, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ValidateQueryRequest::new, ShardValidateQueryRequest::new, ThreadPool.Names.SEARCH);
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        LongSupplier timeProvider = () -> request.nowInMillis;
        ActionListener<org.elasticsearch.index.query.QueryBuilder> rewriteListener = ActionListener.wrap(rewrittenQuery -> {
            request.query(rewrittenQuery);
            super.doExecute(task, request, listener);
        },
            ex -> {
            if (ex instanceof IndexNotFoundException ||
                ex instanceof IndexClosedException) {
                listener.onFailure(ex);
                return;
            }
            List<QueryExplanation> explanations = new ArrayList<>();
            explanations.add(new QueryExplanation(null,
                QueryExplanation.RANDOM_SHARD,
                false,
                null,
                ex.getMessage()));
            listener.onResponse(
                new ValidateQueryResponse(
                    false,
                    explanations,
                    // totalShards is documented as "the total shards this request ran against",
                    // which is 0 since the failure is happening on the coordinating node.
                    0,
                    0 ,
                    0,
                    null));
        });
        if (request.query() == null) {
            rewriteListener.onResponse(request.query());
        } else {
            Rewriteable.rewriteAndFetch(request.query(), searchService.getRewriteContext(timeProvider),
                rewriteListener);
        }
    }

    @Override
    protected ShardValidateQueryRequest newShardRequest(int numShards, ShardRouting shard, ValidateQueryRequest request) {
        final ClusterState clusterState = clusterService.state();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        final AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, shard.getIndexName(), indicesAndAliases);
        return new ShardValidateQueryRequest(shard.shardId(), aliasFilter, request);
    }

    @Override
    protected ShardValidateQueryResponse readShardResponse(StreamInput in) throws IOException {
        return new ShardValidateQueryResponse(in);
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
    protected ValidateQueryResponse newResponse(ValidateQueryRequest request, AtomicReferenceArray<?> shardsResponses,
                                                ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        boolean valid = true;
        List<DefaultShardOperationFailedException> shardFailures = null;
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
    protected ShardValidateQueryResponse shardOperation(ShardValidateQueryRequest request, Task task) throws IOException {
        boolean valid;
        String explanation = null;
        String error = null;
        ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(request.shardId(),
            request.nowInMillis(), request.filteringAliases());
        SearchContext searchContext = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        try {
            ParsedQuery parsedQuery = searchContext.getSearchExecutionContext().toQuery(request.query());
            searchContext.parsedQuery(parsedQuery);
            searchContext.preProcess(request.rewrite());
            valid = true;
            explanation = explain(searchContext, request.rewrite());
        } catch (QueryShardException|ParsingException e) {
            valid = false;
            error = e.getDetailedMessage();
        } catch (AssertionError e) {
            valid = false;
            error = e.getMessage();
        } finally {
            Releasables.close(searchContext);
        }

        return new ShardValidateQueryResponse(request.shardId(), valid, explanation, error);
    }

    private String explain(SearchContext context, boolean rewritten) {
        Query query = context.query();
        if (rewritten && query instanceof MatchNoDocsQuery) {
            return context.parsedQuery().query().toString();
        } else {
            return query.toString();
        }
    }
}
