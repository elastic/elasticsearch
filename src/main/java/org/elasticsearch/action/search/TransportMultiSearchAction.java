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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TransportMultiSearchAction extends TransportAction<MultiSearchRequest, MultiSearchResponse> {

    private final ClusterService clusterService;

    private final TransportSearchAction searchAction;

    @Inject
    public TransportMultiSearchAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService, TransportSearchAction searchAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.searchAction = searchAction;

        transportService.registerHandler(MultiSearchAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[request.requests().size()];
        final AtomicInteger counter = new AtomicInteger(responses.length);
        for (int i = 0; i < responses.length; i++) {
            final int index = i;
            searchAction.execute(request.requests().get(i), new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    synchronized (responses) {
                        responses[index] = new MultiSearchResponse.Item(searchResponse, null);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    synchronized (responses) {
                        responses[index] = new MultiSearchResponse.Item(null, ExceptionsHelper.detailedMessage(e));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiSearchResponse(responses));
                }
            });
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<MultiSearchRequest> {

        @Override
        public MultiSearchRequest newInstance() {
            return new MultiSearchRequest();
        }

        @Override
        public void messageReceived(final MultiSearchRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<MultiSearchResponse>() {
                @Override
                public void onResponse(MultiSearchResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [msearch] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
