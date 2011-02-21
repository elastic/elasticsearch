/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.scan.ScanSearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class TransportSearchScanAction extends TransportSearchTypeAction {

    @Inject public TransportSearchScanAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                             TransportSearchCache transportSearchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, transportSearchCache, searchService, searchPhaseController);
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<ScanSearchResult> {

        private final Map<SearchShardTarget, ScanSearchResult> scanResults = ConcurrentCollections.newConcurrentMap();

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override protected String firstPhaseName() {
            return "init_scan";
        }

        @Override protected void sendExecuteFirstPhase(DiscoveryNode node, InternalSearchRequest request, SearchServiceListener<ScanSearchResult> listener) {
            searchService.sendExecuteScan(node, request, listener);
        }

        @Override protected void processFirstPhaseResult(ShardRouting shard, ScanSearchResult result) {
            scanResults.put(result.shardTarget(), result);
        }

        @Override protected void moveToSecondPhase() throws Exception {
            long totalHits = 0;
            for (ScanSearchResult scanSearchResult : scanResults.values()) {
                totalHits += scanSearchResult.totalHits();
            }
            final InternalSearchResponse internalResponse = new InternalSearchResponse(new InternalSearchHits(InternalSearchHits.EMPTY, totalHits, 0.0f), null, false);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = TransportSearchHelper.buildScrollId(request.searchType(), scanResults.values());
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get(), buildTookInMillis(), buildShardFailures()));
        }
    }
}