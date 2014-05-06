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

package org.elasticsearch.action.search.type;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportSearchQueryThenFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchQueryThenFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                               SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QuerySearchResult> {

        final AtomicArray<FetchSearchResult> fetchResults;
        final AtomicArray<IntArrayList> docIdsToLoad;

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
            fetchResults = new AtomicArray<>(firstResults.length());
            docIdsToLoad = new AtomicArray<>(firstResults.length());
        }

        @Override
        protected String firstPhaseName() {
            return "query";
        }

        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchRequest request, SearchServiceListener<QuerySearchResult> listener) {
            searchService.sendExecuteQuery(node, request, listener);
        }

        @Override
        protected void moveToSecondPhase() throws Exception {
            boolean useScroll = !useSlowScroll && request.scroll() != null;
            sortedShardList = searchPhaseController.sortDocs(useScroll, firstResults);
            searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);

            if (docIdsToLoad.asList().isEmpty()) {
                finishHim();
                return;
            }

            final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(
                    request, sortedShardList, firstResults.length()
            );
            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());
            for (AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
                QuerySearchResult queryResult = firstResults.get(entry.index);
                DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
                FetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult, entry, lastEmittedDocPerShard);
                executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
            }
        }

        void executeFetch(final int shardIndex, final SearchShardTarget shardTarget, final AtomicInteger counter, final FetchSearchRequest fetchSearchRequest, DiscoveryNode node) {
            searchService.sendExecuteFetch(node, fetchSearchRequest, new SearchServiceListener<FetchSearchResult>() {
                @Override
                public void onResult(FetchSearchResult result) {
                    result.shardTarget(shardTarget);
                    fetchResults.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // the failure might happen without managing to clear the search context..., potentially need to clear its context (for example)
                    docIdsToLoad.set(shardIndex, null);
                    onFetchFailure(t, fetchSearchRequest, shardIndex, shardTarget, counter);
                }
            });
        }

        void onFetchFailure(Throwable t, FetchSearchRequest fetchSearchRequest, int shardIndex, SearchShardTarget shardTarget, AtomicInteger counter) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Failed to execute fetch phase", t, fetchSearchRequest.id());
            }
            this.addShardFailure(shardIndex, shardTarget, t);
            successfulOps.decrementAndGet();
            if (counter.decrementAndGet() == 0) {
                finishHim();
            }
        }

        void finishHim() {
            try {
                innerFinishHim();
            } catch (Throwable e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                listener.onFailure(failure);
            } finally {
                releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
            }
        }

        void innerFinishHim() throws Exception {
            InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults, fetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults, null);
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
        }
    }
}
