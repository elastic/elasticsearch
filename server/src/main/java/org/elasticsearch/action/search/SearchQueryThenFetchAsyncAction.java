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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;
    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;

    SearchQueryThenFetchAsyncAction(final Logger logger, final SearchTransportService searchTransportService,
                                    final BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                    final Map<String, AliasFilter> aliasFilter,
                                    final Map<String, Float> concreteIndexBoosts, final Map<String, Set<String>> indexRoutings,
                                    final SearchPhaseController searchPhaseController, final Executor executor,
                                    final SearchRequest request, final ActionListener<SearchResponse> listener,
                                    final GroupShardsIterator<SearchShardIterator> shardsIts,
                                    final TransportSearchAction.SearchTimeProvider timeProvider,
                                    ClusterState clusterState, SearchTask task, SearchResponse.Clusters clusters,
                                    Consumer<Exception> onPartialMergeFailure) {
        super("query", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterState, task,
                searchPhaseController.newSearchPhaseResults(executor, task.getProgressListener(),
                    request, shardsIts.size(), onPartialMergeFailure), request.getMaxConcurrentShardRequests(), clusters);
        this.topDocsSize = getTopDocsSize(request);
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.searchPhaseController = searchPhaseController;
        this.progressListener = task.getProgressListener();
        final SearchSourceBuilder sourceBuilder = request.source();
        progressListener.notifyListShards(SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts), clusters, sourceBuilder == null || sourceBuilder.size() != 0);
    }

    protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                       final SearchActionListener<SearchPhaseResult> listener) {
        ShardSearchRequest request = rewriteShardSearchRequest(super.buildShardSearchRequest(shardIt));
        getSearchTransport().sendExecuteQuery(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            request, getTask(), listener);
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

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, final SearchPhaseContext context) {
        return new FetchSearchPhase(results, searchPhaseController, context, clusterState());
    }

    private ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        if (bottomSortCollector == null) {
            return request;
        }

        // disable tracking total hits if we already reached the required estimation.
        if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE
                && bottomSortCollector.getTotalHits() > trackTotalHitsUpTo) {
            request.source(request.source().shallowCopy().trackTotalHits(false));
        }

        // set the current best bottom field doc
        if (bottomSortCollector.getBottomSortValues() != null) {
            request.setBottomSortValues(bottomSortCollector.getBottomSortValues());
        }
        return request;
    }
}
