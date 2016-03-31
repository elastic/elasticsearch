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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.search.TransportSearchHelper.parseScrollId;

/**
 */
public class TransportClearScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;

    @Inject
    public TransportClearScrollAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                      ClusterService clusterService, SearchTransportService searchTransportService,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ClearScrollAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, ClearScrollRequest::new);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
    }

    @Override
    protected void doExecute(ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        new Async(request, listener, clusterService.state()).run();
    }

    private class Async {
        final DiscoveryNodes nodes;
        final CountDown expectedOps;
        final List<ScrollIdForNode[]> contexts = new ArrayList<>();
        final ActionListener<ClearScrollResponse> listener;
        final AtomicReference<Throwable> expHolder;
        final AtomicInteger numberOfFreedSearchContexts = new AtomicInteger(0);

        private Async(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener, ClusterState clusterState) {
            int expectedOps = 0;
            this.nodes = clusterState.nodes();
            if (request.getScrollIds().size() == 1 && "_all".equals(request.getScrollIds().get(0))) {
                expectedOps = nodes.getSize();
            } else {
                for (String parsedScrollId : request.getScrollIds()) {
                    ScrollIdForNode[] context = parseScrollId(parsedScrollId).getContext();
                    expectedOps += context.length;
                    this.contexts.add(context);
                }
            }
            this.listener = listener;
            this.expHolder = new AtomicReference<>();
            this.expectedOps = new CountDown(expectedOps);
        }

        public void run() {
            if (expectedOps.isCountedDown()) {
                listener.onResponse(new ClearScrollResponse(true, 0));
                return;
            }

            if (contexts.isEmpty()) {
                for (final DiscoveryNode node : nodes) {
                    searchTransportService.sendClearAllScrollContexts(node, new ActionListener<TransportResponse>() {
                        @Override
                        public void onResponse(TransportResponse response) {
                            onFreedContext(true);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            onFailedFreedContext(e, node);
                        }
                    });
                }
            } else {
                for (ScrollIdForNode[] context : contexts) {
                    for (ScrollIdForNode target : context) {
                        final DiscoveryNode node = nodes.get(target.getNode());
                        if (node == null) {
                            onFreedContext(false);
                            continue;
                        }

                        searchTransportService.sendFreeContext(node, target.getScrollId(), new ActionListener<SearchTransportService.SearchFreeContextResponse>() {
                            @Override
                            public void onResponse(SearchTransportService.SearchFreeContextResponse freed) {
                                onFreedContext(freed.isFreed());
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                onFailedFreedContext(e, node);
                            }
                        });
                    }
                }
            }
        }

        void onFreedContext(boolean freed) {
            if (freed) {
                numberOfFreedSearchContexts.incrementAndGet();
            }
            if (expectedOps.countDown()) {
                boolean succeeded = expHolder.get() == null;
                listener.onResponse(new ClearScrollResponse(succeeded, numberOfFreedSearchContexts.get()));
            }
        }

        void onFailedFreedContext(Throwable e, DiscoveryNode node) {
            logger.warn("Clear SC failed on node[{}]", e, node);
            if (expectedOps.countDown()) {
                listener.onResponse(new ClearScrollResponse(false, numberOfFreedSearchContexts.get()));
            } else {
                expHolder.set(e);
            }
        }

    }

}
