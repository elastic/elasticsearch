/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class TransportGetFieldMappingsAction extends HandledTransportAction<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NodeClient client;

    @Inject
    public TransportGetFieldMappingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client
    ) {
        super(GetFieldMappingsAction.NAME, transportService, actionFilters, GetFieldMappingsRequest::new);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<>(concreteIndices.length);

        if (concreteIndices.length == 0) {
            listener.onResponse(new GetFieldMappingsResponse(emptyMap()));
        } else {
            for (final String index : concreteIndices) {
                GetFieldMappingsIndexRequest shardRequest = new GetFieldMappingsIndexRequest(request, index);

                client.executeLocally(TransportGetFieldMappingsIndexAction.TYPE, shardRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(GetFieldMappingsResponse result) {
                        indexResponses.set(indexCounter.getAndIncrement(), result);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        int index = indexCounter.getAndIncrement();
                        indexResponses.set(index, e);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }
                });
            }
        }
    }

    private static GetFieldMappingsResponse merge(AtomicReferenceArray<Object> indexResponses) {
        Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mergedResponses = new HashMap<>();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof GetFieldMappingsResponse response) {
                mergedResponses.putAll(response.mappings());
            }
        }
        return new GetFieldMappingsResponse(unmodifiableMap(mergedResponses));
    }
}
