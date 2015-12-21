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

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiGetAction extends HandledTransportAction<MultiGetRequest, MultiGetResponse> {

    private final ClusterService clusterService;

    private final TransportShardMultiGetAction shardAction;

    @Inject
    public TransportMultiGetAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                   ClusterService clusterService, TransportShardMultiGetAction shardAction,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, MultiGetAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, MultiGetRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
    }

    @Override
    protected void doExecute(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        ClusterState clusterState = clusterService.state();

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiGetItemResponse> responses = new AtomicArray<>(request.items.size());

        Map<ShardId, MultiGetShardRequest> shardRequests = new HashMap<>();
        for (int i = 0; i < request.items.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);
            if (!clusterState.metaData().hasConcreteIndex(item.index())) {
                responses.set(i, new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.type(), item.id(), new IndexNotFoundException(item.index()))));
                continue;
            }
            item.routing(clusterState.metaData().resolveIndexRouting(item.parent(), item.routing(), item.index()));
            String concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, item);
            if (item.routing() == null && clusterState.getMetaData().routingRequired(concreteSingleIndex, item.type())) {
                responses.set(i, new MultiGetItemResponse(null, new MultiGetResponse.Failure(concreteSingleIndex, item.type(), item.id(),
                        new IllegalArgumentException("routing is required for [" + concreteSingleIndex + "]/[" + item.type() + "]/[" + item.id() + "]"))));
                continue;
            }
            ShardId shardId = clusterService.operationRouting()
                    .getShards(clusterState, concreteSingleIndex, item.type(), item.id(), item.routing(), null).shardId();
            MultiGetShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiGetShardRequest(request, shardId.index().name(), shardId.id());
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, item);
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
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        MultiGetRequest.Item item = shardRequest.items.get(i);
                        responses.set(shardRequest.locations.get(i), new MultiGetItemResponse(null,
                                new MultiGetResponse.Failure(shardRequest.index(), item.type(), item.id(), e)));
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
}
