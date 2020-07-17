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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
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
    public TransportGetFieldMappingsAction(TransportService transportService, ClusterService clusterService,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                           NodeClient client) {
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

    private GetFieldMappingsResponse merge(AtomicReferenceArray<Object> indexResponses) {
        Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mergedResponses = new HashMap<>();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof GetFieldMappingsResponse) {
                GetFieldMappingsResponse response = (GetFieldMappingsResponse) element;
                mergedResponses.putAll(response.mappings());
            }
        }
        return new GetFieldMappingsResponse(unmodifiableMap(mergedResponses));
    }
}
