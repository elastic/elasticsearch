/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.NodeSearchRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;
    private final Client client;

    SearchQueryThenFetchAsyncAction(
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        Client client
    ) {
        super(
            "query",
            logger,
            namedWriteableRegistry,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            resultConsumer,
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        this.topDocsSize = getTopDocsSize(request);
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.progressListener = task.getProgressListener();
        this.client = client;

        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request.source());
        }
    }

    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final Transport.Connection connection,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        ShardSearchRequest request = rewriteShardSearchRequest(super.buildShardSearchRequest(shardIt, listener.requestIndex));
        getSearchTransport().sendExecuteQuery(connection, request, getTask(), listener);
    }

    protected void executePhaseOnDataNode(
        List<ShardRequest> requests,
        Transport.Connection connection,
        PendingExecutions pendingExecutions
    ) {
        List<ShardSearchRequest> shardSearchRequests = new ArrayList<>(requests.size());
        for (ShardRequest request : requests) {
            shardSearchRequests.add(super.buildShardSearchRequest(request.shardIt(), request.shardIndex()));
        }
        NodeSearchRequest nodeSearchRequest = new NodeSearchRequest(shardSearchRequests);
        getTask().setResponseAsRequestConsumer(shardSearchResponseAsRequest -> {
            pendingExecutions.releaseOnePermit();
            SearchPhaseResult result = shardSearchResponseAsRequest.getResult();
            if (shardSearchResponseAsRequest.getResult() != null) {
                SearchShardIterator it;
                for (int i = 0; i < shardIterators.length; i++) {
                    if (shardIterators[i].shardId().equals(shardSearchResponseAsRequest.getShardId())) {
                        it = shardIterators[i];
                        result.setShardIndex(i);
                        try {
                            onShardResult(result, it);
                        } catch (Exception exc) {
                            SearchShardTarget shard = new SearchShardTarget(
                                connection.getNode().getId(),
                                shardSearchResponseAsRequest.getShardId(),
                                null
                            );
                            onShardFailure(i, shard, it, exc);
                        }

                        return;// TODO MP check this logic later
                    }
                }
                assert false;
                // onShardFailure(shardIndex, shard, shardIt, shardSearchResponseAsRequest.getError());
            } else {
                SearchShardIterator shardIt = null;
                int shardIndex = 0;
                for (int i = 0; i < shardIterators.length; i++) {
                    if (shardIterators[i].shardId().equals(shardSearchResponseAsRequest.getShardId())) {
                        shardIt = shardIterators[i];
                        shardIndex = i;
                        break;
                    }
                }
                SearchShardTarget shard = new SearchShardTarget(
                    connection.getNode().getId(),
                    shardSearchResponseAsRequest.getShardId(),
                    null
                );
                onShardFailure(shardIndex, shard, shardIt, shardSearchResponseAsRequest.getError());
            }
        });
        getSearchTransport().sendExecuteQuery(connection, nodeSearchRequest, getTask(), new ActionListener<TransportResponse.Empty>() {
            @Override
            public void onResponse(TransportResponse.Empty response) {
                // todo do nothing?
            }

            @Override
            public void onFailure(Exception e) {
                pendingExecutions.releaseAllPermits();
                for (ShardRequest request : requests) {
                    onShardFailure(request.shardIndex(), request.shard(), request.shardIt(), e);
                }
            }
        });
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onShardResult(SearchPhaseResult result, SearchShardIterator shardIt) {
        QuerySearchResult queryResult = result.queryResult();
        if (queryResult.isNull() == false
            // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
            && getRequest().scroll() == null
            // top docs are already consumed if the query was cancelled or in error.
            && queryResult.hasConsumedTopDocs() == false
            && queryResult.topDocs() != null
            && queryResult.topDocs().topDocs.getClass() == TopFieldDocs.class) {
            TopFieldDocs topDocs = (TopFieldDocs) queryResult.topDocs().topDocs;
            if (bottomSortCollector == null) {
                synchronized (this) {
                    if (bottomSortCollector == null) {
                        bottomSortCollector = new BottomSortValuesCollector(topDocsSize, topDocs.fields);
                    }
                }
            }
            bottomSortCollector.consumeTopDocs(topDocs, queryResult.sortValueFormats());
        }
        super.onShardResult(result, shardIt);
    }

    static SearchPhase nextPhase(
        Client client,
        AbstractSearchAsyncAction<?> context,
        SearchPhaseResults<SearchPhaseResult> queryResults,
        AggregatedDfs aggregatedDfs
    ) {
        var rankFeaturePhaseCoordCtx = RankFeaturePhase.coordinatorContext(context.getRequest().source(), client);
        if (rankFeaturePhaseCoordCtx == null) {
            return new FetchSearchPhase(queryResults, aggregatedDfs, context, null);
        }
        return new RankFeaturePhase(queryResults, aggregatedDfs, context, rankFeaturePhaseCoordCtx);
    }

    @Override
    protected SearchPhase getNextPhase() {
        return nextPhase(client, this, results, null);
    }

    private ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        if (bottomSortCollector == null) {
            return request;
        }

        // disable tracking total hits if we already reached the required estimation.
        if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE && bottomSortCollector.getTotalHits() > trackTotalHitsUpTo) {
            request.source(request.source().shallowCopy().trackTotalHits(false));
        }

        // set the current best bottom field doc
        if (bottomSortCollector.getBottomSortValues() != null) {
            request.setBottomSortValues(bottomSortCollector.getBottomSortValues());
        }
        return request;
    }
}
