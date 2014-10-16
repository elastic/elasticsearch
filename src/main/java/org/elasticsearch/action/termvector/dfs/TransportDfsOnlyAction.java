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

package org.elasticsearch.action.termvector.dfs;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.type.TransportSearchTypeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 *
 */
public class TransportDfsOnlyAction extends TransportSearchTypeAction {

    @Inject
    public TransportDfsOnlyAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController, ActionFilters actionFilters) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<DfsSearchResult> {

        private AggregatedDfs dfs;

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override
        protected String firstPhaseName() {
            return "dfs_only";
        }

        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, SearchServiceListener<DfsSearchResult> listener) {
            searchService.sendExecuteDfs(node, request, listener);
        }

        @Override
        protected void moveToSecondPhase() {
            this.dfs = searchPhaseController.aggregateDfs(firstResults);
            sendFreeContext();
            finishHim();
        }

        private void sendFreeContext() {
            for (AtomicArray.Entry<DfsSearchResult> entry : firstResults.asList()) {
                try {
                    DiscoveryNode node = nodes.get(entry.value.shardTarget().nodeId());
                    if (node != null) { // should not happen (==null) but safeguard anyhow
                        searchService.sendFreeContext(node, entry.value.id(), request);
                    }
                } catch (Throwable t1) {
                    logger.trace("failed to release context", t1);
                }
            }
        }

        private void finishHim() {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable(listener) {
                @Override
                public void doRun() throws IOException {
                    listener.onResponse(new DfsOnlyResponse(dfs, buildTookInMillis(), buildShardFailures()));
                }

                @Override
                public void onFailure(Throwable t) {
                    ElasticsearchException failure = new ElasticsearchException("Exception during dfs phase", t);
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to perform dfs", failure);
                    }
                    super.onFailure(t);
                }
            });
        }
    }

}
