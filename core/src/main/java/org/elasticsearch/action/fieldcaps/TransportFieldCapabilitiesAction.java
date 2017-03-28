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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportFieldCapabilitiesAction
    extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    private final ClusterService clusterService;
    private final TransportFieldCapabilitiesIndexAction shardAction;

    @Inject
    public TransportFieldCapabilitiesAction(Settings settings, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            TransportFieldCapabilitiesIndexAction shardAction,
                                            ActionFilters actionFilters,
                                            IndexNameExpressionResolver
                                                    indexNameExpressionResolver) {
        super(settings, FieldCapabilitiesAction.NAME, threadPool, transportService,
            actionFilters, indexNameExpressionResolver, FieldCapabilitiesRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
    }

    @Override
    protected void doExecute(FieldCapabilitiesRequest request,
                             final ActionListener<FieldCapabilitiesResponse> listener) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices =
            indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses =
            new AtomicReferenceArray<>(concreteIndices.length);
        if (concreteIndices.length == 0) {
            listener.onResponse(new FieldCapabilitiesResponse());
        } else {
            for (final String index : concreteIndices) {
                FieldCapabilitiesIndexRequest indexRequest =
                    new FieldCapabilitiesIndexRequest(request, index);
                shardAction.execute(indexRequest,
                    new ActionListener<FieldCapabilitiesIndexResponse> () {
                    @Override
                    public void onResponse(FieldCapabilitiesIndexResponse result) {
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

    private FieldCapabilitiesResponse merge(AtomicReferenceArray<Object> indexResponses) {
        Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<> ();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof FieldCapabilitiesIndexResponse == false) {
                continue;
            }
            FieldCapabilitiesIndexResponse response = (FieldCapabilitiesIndexResponse) element;
            for (String field : response.get().keySet()) {
                Map<String, FieldCapabilities.Builder> typeMap = responseMapBuilder.get(field);
                if (typeMap == null) {
                    typeMap = new HashMap<> ();
                    responseMapBuilder.put(field, typeMap);
                }
                FieldCapabilities fieldCap = response.getField(field);
                FieldCapabilities.Builder builder = typeMap.get(fieldCap.getType());
                if (builder == null) {
                    builder = new FieldCapabilities.Builder(field, fieldCap.getType());
                    typeMap.put(fieldCap.getType(), builder);
                }
                builder.add(response.getIndexName(),
                    fieldCap.isSearchable(), fieldCap.isAggregatable());
            }
        }

        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry :
            responseMapBuilder.entrySet()) {
            Map<String, FieldCapabilities> typeMap = new HashMap<>();
            boolean multi = entry.getValue().size() > 1;
            for (Map.Entry<String, FieldCapabilities.Builder> fieldEntry :
                entry.getValue().entrySet()) {
                typeMap.put(fieldEntry.getKey(), fieldEntry.getValue().build(multi));
            }
            responseMap.put(entry.getKey(), typeMap);
        }

        return new FieldCapabilitiesResponse(responseMap);
    }
}
