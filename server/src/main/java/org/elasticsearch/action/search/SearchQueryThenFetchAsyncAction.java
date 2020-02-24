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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.search.internal.SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;
    private final Supplier<TopDocs> topDocsSupplier;
    private final int topDocsSize;
    private final SearchProgressListener progressListener;

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
        this.topDocsSupplier = getBufferTopDocsSupplier(request, results);
        this.searchPhaseController = searchPhaseController;
        this.progressListener = task.getProgressListener();
        final SearchSourceBuilder sourceBuilder = request.source();
        progressListener.notifyListShards(progressListener.searchShards(this.shardsIts),
            sourceBuilder == null || sourceBuilder.size() != 0);
    }

    protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                       final SearchActionListener<SearchPhaseResult> listener) {
        ShardSearchRequest request = rewriteShardRequest(buildShardSearchRequest(shardIt));
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

    ShardSearchRequest rewriteShardRequest(ShardSearchRequest request) {
        TopDocs topDocs = topDocsSupplier.get();
        if (topDocs != null && topDocs instanceof TopFieldDocs) {
            SearchSourceBuilder source = request.source();
            int trackTotalHits = source.trackTotalHitsUpTo() == null ? DEFAULT_TRACK_TOTAL_HITS_UP_TO :
                source.trackTotalHitsUpTo();
            if (topDocs.totalHits.value >= trackTotalHits) {
                SearchSourceBuilder newSource = source.shallowCopy();
                newSource.trackTotalHits(false);
                if (topDocs.scoreDocs.length >= topDocsSize) {
                    FieldDoc bottomDoc = (FieldDoc) topDocs.scoreDocs[topDocs.scoreDocs.length-1];
                    request.setRawBottomSortValues(bottomDoc.fields);
                }
                request.source(newSource);
            }
        }
        return request;
    }

    private Supplier<TopDocs> getBufferTopDocsSupplier(SearchRequest request,
                                                       SearchPhaseResults<SearchPhaseResult> searchPhaseResults) {
        if (searchPhaseResults instanceof SearchPhaseController.QueryPhaseResultConsumer == false) {
            return () -> null;
        }
        FieldSortBuilder fieldSort = FieldSortBuilder.getPrimaryFieldSortOrNull(request.source());
        if (topDocsSize == 0
                || fieldSort == null
                || fieldSort.canRewriteToMatchNone() == false) {
            return () -> null;
        }
        return ((SearchPhaseController.QueryPhaseResultConsumer) searchPhaseResults)::getBufferTopDocs;
    }
}
