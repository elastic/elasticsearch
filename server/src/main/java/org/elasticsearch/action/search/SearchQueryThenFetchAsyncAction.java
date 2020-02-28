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
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseController.QueryPhaseResultConsumer;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.search.internal.SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;
    private final int topDocsSize;
    private final SearchProgressListener progressListener;
    private final QueryPhaseResultConsumer resultConsumer;

    SearchQueryThenFetchAsyncAction(final Logger logger, final SearchTransportService searchTransportService,
            final BiFunction<String, String, Transport.Connection> nodeIdToConnection, final Map<String, AliasFilter> aliasFilter,
            final Map<String, Float> concreteIndexBoosts, final Map<String, Set<String>> indexRoutings,
            final SearchPhaseController searchPhaseController, final Executor executor,
            final SearchRequest request, final ActionListener<SearchResponse> listener,
            final GroupShardsIterator<SearchShardIterator> shardsIts, final TransportSearchAction.SearchTimeProvider timeProvider,
            long clusterStateVersion, SearchTask task, SearchResponse.Clusters clusters) {
        super("query", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterStateVersion, task,
                searchPhaseController.newSearchPhaseResults(task.getProgressListener(), request, shardsIts.size()),
                request.getMaxConcurrentShardRequests(), clusters);
        this.topDocsSize = getTopDocsSize(request);
        this.resultConsumer = getResultConsumer(request, topDocsSize, results);
        this.searchPhaseController = searchPhaseController;
        this.progressListener = task.getProgressListener();
        final SearchSourceBuilder sourceBuilder = request.source();
        progressListener.notifyListShards(progressListener.searchShards(this.shardsIts),
            sourceBuilder == null || sourceBuilder.size() != 0);
    }

    protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                       final SearchActionListener<SearchPhaseResult> listener) {
        ShardSearchRequest request = buildShardSearchRequestInternal(shardIt);
        getSearchTransport().sendExecuteQuery(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            request, getTask(), listener);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, exc);
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, final SearchPhaseContext context) {
        return new FetchSearchPhase(results, searchPhaseController, context);
    }

    ShardSearchRequest buildShardSearchRequestInternal(SearchShardIterator shardIt) {
        ShardSearchRequest request = super.buildShardSearchRequest(shardIt);
        if (resultConsumer == null) {
            return request;
        }
        List<TopFieldDocs> topDocsList = resultConsumer.getRemainingTopDocs().stream()
            .filter(obj -> obj instanceof TopFieldDocs)
            .map(obj -> (TopFieldDocs) obj)
            .collect(Collectors.toList());
        if (topDocsList.isEmpty()) {
            return request;
        }
        SearchSourceBuilder source = request.source();
        int trackTotalHits = source.trackTotalHitsUpTo() == null ? DEFAULT_TRACK_TOTAL_HITS_UP_TO : source.trackTotalHitsUpTo();
        long totalHits = -1;
        FieldDoc bestBottom = null;
        SortField primarySort = topDocsList.get(0).fields[0];
        FieldComparator fieldComparator = primarySort.getComparator(1, 0);
        int reverseMul = primarySort.getReverse() ? -1 : 1;
        // we don't want to perform a costly merge to find the best bottom field doc so
        // we just check pick the best bottom document among the available buffer. This
        // means that we don't have the true-best bottom but this avoids running a partial
        // merge too often.
        for (TopFieldDocs topDocs : topDocsList) {
            totalHits += topDocs.totalHits.value;
            if (topDocs.scoreDocs.length == topDocsSize) {
                FieldDoc cand = (FieldDoc) topDocs.scoreDocs[topDocsSize-1];
                if (bestBottom == null ||
                        fieldComparator.compareValues(cand.fields[0], bestBottom.fields[0]) * reverseMul < 0) {
                    bestBottom = cand;
                }
            }
        }
        if (trackTotalHits != SearchContext.TRACK_TOTAL_HITS_ACCURATE && totalHits >= trackTotalHits) {
            request.source(source.shallowCopy().trackTotalHits(false));
        }
        if (bestBottom != null) {
            request.setRawBottomSortValues(bestBottom.fields);
        }
        return request;
    }

    /**
     * Returns a result consumer that exposes the buffer of partially reduced top docs
     * if the primary sort can rewrite to match none. See {@link ShardSearchRequest#getRewriteable()}
     * for more details.
     */
    private static QueryPhaseResultConsumer getResultConsumer(SearchRequest request,
                                                              int topDocsSize,
                                                              SearchPhaseResults<SearchPhaseResult> results) {
        if (results instanceof SearchPhaseController.QueryPhaseResultConsumer == false) {
            return null;
        }
        FieldSortBuilder fieldSort = FieldSortBuilder.getPrimaryFieldSortOrNull(request.source());
        if (topDocsSize == 0
                || fieldSort == null
                || fieldSort.canRewriteToMatchNone() == false) {
            return null;
        }

        return (QueryPhaseResultConsumer) results;
    }
}
