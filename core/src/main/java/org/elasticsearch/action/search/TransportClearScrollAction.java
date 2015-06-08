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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.search.type.TransportSearchHelper.parseScrollId;

/**
 */
public class TransportClearScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {

    private final ClusterService clusterService;
    private final SearchServiceTransportAction searchServiceTransportAction;

    @Inject
    public TransportClearScrollAction(Settings settings, TransportService transportService, ThreadPool threadPool, ClusterService clusterService, SearchServiceTransportAction searchServiceTransportAction, ActionFilters actionFilters) {
        super(settings, ClearScrollAction.NAME, threadPool, transportService, actionFilters, ClearScrollRequest.class);
        this.clusterService = clusterService;
        this.searchServiceTransportAction = searchServiceTransportAction;
    }

    @Override
    protected void doExecute(ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        new Async(request, listener, clusterService.state()).run();
    }

    private class Async {

        final DiscoveryNodes nodes;
        final CountDown expectedOps;
        final ClearScrollRequest request;
        final List<Tuple<String, Long>[]> contexts = new ArrayList<>();
        final ActionListener<ClearScrollResponse> listener;
        final AtomicReference<Throwable> expHolder;
        final AtomicInteger numberOfFreedSearchContexts = new AtomicInteger(0);

        private Async(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener, ClusterState clusterState) {
            int expectedOps = 0;
            this.nodes = clusterState.nodes();
            if (request.getScrollIds().size() == 1 && "_all".equals(request.getScrollIds().get(0))) {
                expectedOps = nodes.size();
            } else {
                for (String parsedScrollId : request.getScrollIds()) {
                    Tuple<String, Long>[] context = parseScrollId(parsedScrollId).getContext();
                    expectedOps += context.length;
                    this.contexts.add(context);
                }
            }

            this.request = request;
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
                    searchServiceTransportAction.sendClearAllScrollContexts(node, request, new ActionListener<TransportResponse>() {
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
                for (Tuple<String, Long>[] context : contexts) {
                    for (Tuple<String, Long> target : context) {
                        final DiscoveryNode node = nodes.get(target.v1());
                        if (node == null) {
                            onFreedContext(false);
                            continue;
                        }

                        searchServiceTransportAction.sendFreeContext(node, target.v2(), request, new ActionListener<SearchServiceTransportAction.SearchFreeContextResponse>() {
                            @Override
                            public void onResponse(SearchServiceTransportAction.SearchFreeContextResponse freed) {
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
