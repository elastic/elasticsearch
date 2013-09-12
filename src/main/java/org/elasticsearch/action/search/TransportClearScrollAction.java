/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.search.type.TransportSearchHelper.parseScrollId;

/**
 */
public class TransportClearScrollAction extends TransportAction<ClearScrollRequest, ClearScrollResponse> {

    private final ClusterService clusterService;
    private final SearchServiceTransportAction searchServiceTransportAction;

    @Inject
    public TransportClearScrollAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, SearchServiceTransportAction searchServiceTransportAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.searchServiceTransportAction = searchServiceTransportAction;
    }

    @Override
    protected void doExecute(ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        new Async(request, listener, clusterService.state()).run();
    }

    private class Async {

        final DiscoveryNodes nodes;
        final AtomicInteger expectedOps;
        final ClearScrollRequest request;
        final List<Tuple<String, Long>[]> contexts = new ArrayList<Tuple<String, Long>[]>();
        final AtomicReference<Throwable> expHolder;
        final ActionListener<ClearScrollResponse> listener;

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
            this.expHolder = new AtomicReference<Throwable>();
            this.expectedOps = new AtomicInteger(expectedOps);
        }

        public void run() {
            if (expectedOps.get() == 0) {
                listener.onResponse(new ClearScrollResponse(true));
                return;
            }

            if (contexts.isEmpty()) {
                for (final DiscoveryNode node : nodes) {
                    searchServiceTransportAction.sendClearAllScrollContexts(node, request, new ActionListener<Boolean>() {
                        @Override
                        public void onResponse(Boolean success) {
                            onFreedContext();
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
                            onFreedContext();
                            continue;
                        }

                        searchServiceTransportAction.sendFreeContext(node, target.v2(), request, new ActionListener<Boolean>() {
                            @Override
                            public void onResponse(Boolean success) {
                                onFreedContext();
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

        void onFreedContext() {
            assert expectedOps.get() > 0;
            if (expectedOps.decrementAndGet() == 0) {
                boolean succeeded = expHolder.get() == null;
                listener.onResponse(new ClearScrollResponse(succeeded));
            }
        }

        void onFailedFreedContext(Throwable e, DiscoveryNode node) {
            logger.warn("Clear SC failed on node[{}]", e, node);
            assert expectedOps.get() > 0;
            if (expectedOps.decrementAndGet() == 0) {
                listener.onResponse(new ClearScrollResponse(false));
            } else {
                expHolder.set(e);
            }
        }

    }

}
