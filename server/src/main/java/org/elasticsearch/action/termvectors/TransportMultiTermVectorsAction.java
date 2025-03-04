/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiTermVectorsAction extends HandledTransportAction<MultiTermVectorsRequest, MultiTermVectorsResponse> {

    private final ClusterService clusterService;
    private final NodeClient client;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportMultiTermVectorsAction(
        TransportService transportService,
        ClusterService clusterService,
        NodeClient client,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            MultiTermVectorsAction.NAME,
            transportService,
            actionFilters,
            MultiTermVectorsRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.clusterService = clusterService;
        this.client = client;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        ClusterState clusterState = clusterService.state();
        ProjectMetadata project = projectResolver.getProjectMetadata(clusterState);
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiTermVectorsItemResponse> responses = new AtomicArray<>(request.requests.size());

        Map<ShardId, MultiTermVectorsShardRequest> shardRequests = new HashMap<>();
        for (int i = 0; i < request.requests.size(); i++) {
            TermVectorsRequest termVectorsRequest = request.requests.get(i);
            ShardId shardId;
            try {
                termVectorsRequest.routing(project.resolveIndexRouting(termVectorsRequest.routing(), termVectorsRequest.index()));
                String concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(project, termVectorsRequest).getName();
                shardId = OperationRouting.shardId(project, concreteSingleIndex, termVectorsRequest.id(), termVectorsRequest.routing());
            } catch (RoutingMissingException e) {
                responses.set(
                    i,
                    new MultiTermVectorsItemResponse(null, new MultiTermVectorsResponse.Failure(e.getIndex().getName(), e.getId(), e))
                );
                continue;
            } catch (Exception e) {
                responses.set(
                    i,
                    new MultiTermVectorsItemResponse(
                        null,
                        new MultiTermVectorsResponse.Failure(termVectorsRequest.index(), termVectorsRequest.id(), e)
                    )
                );
                continue;
            }

            MultiTermVectorsShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiTermVectorsShardRequest(shardId.getIndexName(), shardId.id());
                shardRequest.preference(request.preference);
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, termVectorsRequest);
        }

        if (shardRequests.size() == 0) {
            // only failures..
            listener.onResponse(new MultiTermVectorsResponse(responses.toArray(new MultiTermVectorsItemResponse[responses.length()])));
        }

        executeShardAction(listener, responses, shardRequests);
    }

    protected void executeShardAction(
        ActionListener<MultiTermVectorsResponse> listener,
        AtomicArray<MultiTermVectorsItemResponse> responses,
        Map<ShardId, MultiTermVectorsShardRequest> shardRequests
    ) {
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiTermVectorsShardRequest shardRequest : shardRequests.values()) {
            client.executeLocally(TransportShardMultiTermsVectorAction.TYPE, shardRequest, new ActionListener<>() {
                @Override
                public void onResponse(MultiTermVectorsShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        responses.set(
                            response.locations.get(i),
                            new MultiTermVectorsItemResponse(response.responses.get(i), response.failures.get(i))
                        );
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // create failures for all relevant requests
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        TermVectorsRequest termVectorsRequest = shardRequest.requests.get(i);
                        responses.set(
                            shardRequest.locations.get(i),
                            new MultiTermVectorsItemResponse(
                                null,
                                new MultiTermVectorsResponse.Failure(shardRequest.index(), termVectorsRequest.id(), e)
                            )
                        );
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(
                        new MultiTermVectorsResponse(responses.toArray(new MultiTermVectorsItemResponse[responses.length()]))
                    );
                }
            });
        }
    }
}
