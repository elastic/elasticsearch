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

package org.elasticsearch.action.termvector;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiTermVectorsAction extends TransportAction<MultiTermVectorsRequest, MultiTermVectorsResponse> {

    private final ClusterService clusterService;

    private final TransportSingleShardMultiTermsVectorAction shardAction;

    @Inject
    public TransportMultiTermVectorsAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                           ClusterService clusterService, TransportSingleShardMultiTermsVectorAction shardAction) {
        super(settings, MultiTermVectorsAction.NAME, threadPool);
        this.clusterService = clusterService;
        this.shardAction = shardAction;

        transportService.registerHandler(MultiTermVectorsAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        ClusterState clusterState = clusterService.state();

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiTermVectorsItemResponse> responses = new AtomicArray<>(request.requests.size());

        Map<ShardId, MultiTermVectorsShardRequest> shardRequests = new HashMap<>();
        for (int i = 0; i < request.requests.size(); i++) {
            TermVectorRequest termVectorRequest = request.requests.get(i);
            termVectorRequest.routing(clusterState.metaData().resolveIndexRouting(termVectorRequest.routing(), termVectorRequest.index()));
            if (!clusterState.metaData().hasConcreteIndex(termVectorRequest.index())) {
                responses.set(i, new MultiTermVectorsItemResponse(null, new MultiTermVectorsResponse.Failure(termVectorRequest.index(),
                        termVectorRequest.type(), termVectorRequest.id(), "[" + termVectorRequest.index() + "] missing")));
                continue;
            }
            termVectorRequest.index(clusterState.metaData().concreteSingleIndex(termVectorRequest.index()));
            if (termVectorRequest.routing() == null && clusterState.getMetaData().routingRequired(termVectorRequest.index(), termVectorRequest.type())) {
                responses.set(i, new MultiTermVectorsItemResponse(null, new MultiTermVectorsResponse.Failure(termVectorRequest.index(), termVectorRequest.type(), termVectorRequest.id(),
                        "routing is required for [" + termVectorRequest.index() + "]/[" + termVectorRequest.type() + "]/[" + termVectorRequest.id() + "]")));
                continue;
            }
            ShardId shardId = clusterService
                    .operationRouting()
                    .getShards(clusterState, termVectorRequest.index(), termVectorRequest.type(), termVectorRequest.id(),
                            termVectorRequest.routing(), null).shardId();
            MultiTermVectorsShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiTermVectorsShardRequest(shardId.index().name(), shardId.id());
                shardRequest.preference(request.preference);

                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, termVectorRequest);
        }
        
        if (shardRequests.size() == 0) {
            // only failures..
            listener.onResponse(new MultiTermVectorsResponse(responses.toArray(new MultiTermVectorsItemResponse[responses.length()])));
        }
        
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());
        for (final MultiTermVectorsShardRequest shardRequest : shardRequests.values()) {
            shardAction.execute(shardRequest, new ActionListener<MultiTermVectorsShardResponse>() {
                @Override
                public void onResponse(MultiTermVectorsShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        responses.set(response.locations.get(i), new MultiTermVectorsItemResponse(response.responses.get(i),
                                response.failures.get(i)));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    // create failures for all relevant requests
                    String message = ExceptionsHelper.detailedMessage(e);
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        TermVectorRequest termVectorRequest = shardRequest.requests.get(i);
                        responses.set(shardRequest.locations.get(i), new MultiTermVectorsItemResponse(null,
                                new MultiTermVectorsResponse.Failure(shardRequest.index(), termVectorRequest.type(),
                                        termVectorRequest.id(), message)));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiTermVectorsResponse(
                            responses.toArray(new MultiTermVectorsItemResponse[responses.length()])));
                }
            });
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<MultiTermVectorsRequest> {

        @Override
        public MultiTermVectorsRequest newInstance() {
            return new MultiTermVectorsRequest();
        }

        @Override
        public void messageReceived(final MultiTermVectorsRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<MultiTermVectorsResponse>() {
                @Override
                public void onResponse(MultiTermVectorsResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable t) {
                        onFailure(t);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Throwable t) {
                        logger.warn("Failed to send error response for action [" + MultiTermVectorsAction.NAME + "] and request ["
                                + request + "]", t);
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
