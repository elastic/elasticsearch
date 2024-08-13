/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiGetAction extends HandledTransportAction<MultiGetRequest, MultiGetResponse> {

    public static final String NAME = "indices:data/read/mget";
    public static final ActionType<MultiGetResponse> TYPE = new ActionType<>(NAME);
    private final ClusterService clusterService;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportMultiGetAction(
        TransportService transportService,
        ClusterService clusterService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver resolver,
        IndicesService indicesService
    ) {
        super(NAME, transportService, actionFilters, MultiGetRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.client = client;
        this.indexNameExpressionResolver = resolver;
        // register the internal TransportGetFromTranslogAction
        new TransportShardMultiGetFomTranslogAction(transportService, indicesService, actionFilters);
    }

    @Override
    protected void doExecute(Task task, final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiGetItemResponse> responses = new AtomicArray<>(request.items.size());
        final Map<ShardId, MultiGetShardRequest> shardRequests = new HashMap<>();
        // single item cache that maps the provided index name to the resolved one
        Tuple<String, String> lastResolvedIndex = Tuple.tuple(null, null);

        for (int i = 0; i < request.items.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);

            ShardId shardId;
            try {
                String concreteSingleIndex;
                if (item.index().equals(lastResolvedIndex.v1())) {
                    concreteSingleIndex = lastResolvedIndex.v2();
                } else {
                    concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, item).getName();
                    lastResolvedIndex = Tuple.tuple(item.index(), concreteSingleIndex);
                }
                item.routing(clusterState.metadata().resolveIndexRouting(item.routing(), item.index()));
                shardId = clusterService.operationRouting().shardId(clusterState, concreteSingleIndex, item.id(), item.routing());
            } catch (RoutingMissingException e) {
                responses.set(i, newItemFailure(e.getIndex().getName(), e.getId(), e));
                continue;
            } catch (Exception e) {
                responses.set(i, newItemFailure(item.index(), item.id(), e));
                continue;
            }

            MultiGetShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiGetShardRequest(request, shardId.getIndexName(), shardId.getId());
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, item);
        }

        if (shardRequests.isEmpty()) {
            // only failures..
            listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
        }

        executeShardAction(listener, responses, shardRequests);
    }

    protected void executeShardAction(
        ActionListener<MultiGetResponse> listener,
        AtomicArray<MultiGetItemResponse> responses,
        Map<ShardId, MultiGetShardRequest> shardRequests
    ) {
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiGetShardRequest shardRequest : shardRequests.values()) {
            client.executeLocally(TransportShardMultiGetAction.TYPE, shardRequest, new DelegatingActionListener<>(listener) {
                @Override
                public void onResponse(MultiGetShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        MultiGetItemResponse itemResponse = new MultiGetItemResponse(response.responses.get(i), response.failures.get(i));
                        responses.set(response.locations.get(i), itemResponse);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // create failures for all relevant requests
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        MultiGetRequest.Item item = shardRequest.items.get(i);
                        responses.set(shardRequest.locations.get(i), newItemFailure(shardRequest.index(), item.id(), e));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    delegate.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
                }
            });
        }
    }

    private static MultiGetItemResponse newItemFailure(String index, String id, Exception exception) {
        return new MultiGetItemResponse(null, new MultiGetResponse.Failure(index, id, exception));
    }
}
