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

package org.elasticsearch.action.get;

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

public class TransportMultiGetAction extends TransportAction<MultiGetRequest, MultiGetResponse> {

    private final ClusterService clusterService;

    private final TransportShardMultiGetAction shardAction;

    @Inject
    public TransportMultiGetAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService, TransportShardMultiGetAction shardAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.shardAction = shardAction;

        transportService.registerHandler(MultiGetAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        ClusterState clusterState = clusterService.state();

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiGetItemResponse> responses = new AtomicArray<MultiGetItemResponse>(request.items.size());

        Map<ShardId, MultiGetShardRequest> shardRequests = new HashMap<ShardId, MultiGetShardRequest>();
        for (int i = 0; i < request.items.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);
            if (!clusterState.metaData().hasConcreteIndex(item.index())) {
                responses.set(i, new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.type(), item.id(), "[" + item.index() + "] missing")));
                continue;
            }

            item.routing(clusterState.metaData().resolveIndexRouting(item.routing(), item.index()));
            item.index(clusterState.metaData().concreteIndex(item.index()));
            ShardId shardId = clusterService.operationRouting()
                    .getShards(clusterState, item.index(), item.type(), item.id(), item.routing(), null).shardId();
            MultiGetShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiGetShardRequest(shardId.index().name(), shardId.id());
                shardRequest.preference(request.preference);
                shardRequest.realtime(request.realtime);
                shardRequest.refresh(request.refresh);

                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, item.type(), item.id(), item.fields());
        }

        if (shardRequests.size() == 0) {
            // only failures..
            listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
        }

        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiGetShardRequest shardRequest : shardRequests.values()) {
            shardAction.execute(shardRequest, new ActionListener<MultiGetShardResponse>() {
                @Override
                public void onResponse(MultiGetShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        responses.set(response.locations.get(i), new MultiGetItemResponse(response.responses.get(i), response.failures.get(i)));
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
                        responses.set(shardRequest.locations.get(i), new MultiGetItemResponse(null,
                                new MultiGetResponse.Failure(shardRequest.index(), shardRequest.types.get(i), shardRequest.ids.get(i), message)));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
                }
            });
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<MultiGetRequest> {

        @Override
        public MultiGetRequest newInstance() {
            return new MultiGetRequest();
        }

        @Override
        public void messageReceived(final MultiGetRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<MultiGetResponse>() {
                @Override
                public void onResponse(MultiGetResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [" + MultiGetAction.NAME + "] and request [" + request + "]", e1);
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
