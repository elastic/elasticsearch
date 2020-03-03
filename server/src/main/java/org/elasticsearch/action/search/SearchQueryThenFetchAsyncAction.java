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
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;
    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private final boolean hasPrimaryFieldSort;
    private volatile TopFieldDocs bottomTopDocs;

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
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.hasPrimaryFieldSort = FieldSortBuilder.hasPrimaryFieldSort(request.source());
        this.searchPhaseController = searchPhaseController;
        this.progressListener = task.getProgressListener();
        final SearchSourceBuilder sourceBuilder = request.source();
        progressListener.notifyListShards(progressListener.searchShards(this.shardsIts),
            sourceBuilder == null || sourceBuilder.size() != 0);
    }

    protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                       final SearchActionListener<SearchPhaseResult> listener) {

        ShardSearchRequest request = rewriteShardSearchRequest(super.buildShardSearchRequest(shardIt));
        getSearchTransport().sendExecuteQuery(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            request, getTask(), listener);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, exc);
    }

    @Override
    protected void onShardResult(SearchPhaseResult result, SearchShardIterator shardIt) {
        try {
            QuerySearchResult queryResult = result.queryResult();
            if (queryResult.isNull() == false
                    && hasPrimaryFieldSort
                    && queryResult.topDocs().topDocs instanceof TopFieldDocs) {
                mergeTopDocs((TopFieldDocs) result.queryResult().topDocs().topDocs, result.getShardIndex());
            }
        } catch (Exception exc) {
            getLogger().error("Failed to merge response", exc);
        }
        super.onShardResult(result, shardIt);
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, final SearchPhaseContext context) {
        return new FetchSearchPhase(results, searchPhaseController, context);
    }

    // merge the current best bottom field doc with the new query result
    private synchronized void mergeTopDocs(TopFieldDocs topDocs, int shardIndex) {
        final ScoreDoc[] bottomDocs;
        if (topDocs.scoreDocs.length == topDocsSize) {
            FieldDoc bottom = (FieldDoc) topDocs.scoreDocs[topDocsSize - 1];
            bottomDocs = new FieldDoc[] { new FieldDoc(bottom.doc, bottom.score, bottom.fields, shardIndex) };
        } else {
            bottomDocs = new ScoreDoc[0];
        }
        TopFieldDocs toMerge = new TopFieldDocs(topDocs.totalHits, bottomDocs, topDocs.fields);
        if (bottomTopDocs == null) {
            bottomTopDocs = toMerge;
        } else {
            final Sort sort = new Sort(bottomTopDocs.fields);
            bottomTopDocs = TopFieldDocs.merge(sort, 0, 1, new TopFieldDocs[]{ bottomTopDocs, toMerge }, false);
        }
    }

    ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        final TopFieldDocs current = bottomTopDocs;
        if (current == null) {
            return request;
        }

        // disable tracking total hits if we already reached the required estimation.
        if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE
                && current.totalHits.value >= trackTotalHitsUpTo
                && current.totalHits.relation == GREATER_THAN_OR_EQUAL_TO) {
            assert request.source() != null : "source should contain a primary sort field";
            request.source(request.source().shallowCopy().trackTotalHits(false));
        }
        // set the global best bottom field doc
        FieldDoc bestBottom = current.scoreDocs.length == 1 ? (FieldDoc) current.scoreDocs[0] : null;
        if (bestBottom != null) {
            request.setRawBottomSortValues(bestBottom.fields);
        }
        return request;
    }
}
