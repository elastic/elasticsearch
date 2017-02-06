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

package org.elasticsearch.action.admin.indices.fieldcaps;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    private final ClusterService clusterService;
    private final TransportFieldCapabilitiesIndexAction shardAction;

    @Inject
    public TransportFieldCapabilitiesAction(Settings settings, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            TransportFieldCapabilitiesIndexAction shardAction,
                                            ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, FieldCapabilitiesAction.NAME, threadPool, transportService,
            actionFilters, indexNameExpressionResolver, FieldCapabilitiesRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
    }

    @Override
    protected void doExecute(FieldCapabilitiesRequest request,
                             final ActionListener<FieldCapabilitiesResponse> listener) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<>(concreteIndices.length);
        if (concreteIndices.length == 0) {
            listener.onResponse(new FieldCapabilitiesResponse());
        } else {
            boolean expandPerIndex = "indices".equals(request.level());
            for (final String index : concreteIndices) {
                FieldCapabilitiesIndexRequest indexRequest = new FieldCapabilitiesIndexRequest(request, index);
                shardAction.execute(indexRequest, new ActionListener<FieldCapabilitiesResponse> () {
                    @Override
                    public void onResponse(FieldCapabilitiesResponse result) {
                        indexResponses.set(indexCounter.getAndIncrement(), result);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(expandPerIndex, indexResponses));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        int index = indexCounter.getAndIncrement();
                        indexResponses.set(index, e);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(expandPerIndex, indexResponses));
                        }
                    }
                });
            }
        }
    }

    private FieldCapabilitiesResponse merge(boolean expandPerIndex, AtomicReferenceArray<Object> indexResponses) {
        List<FieldCapabilitiesResponse> validResponses = new ArrayList<> ();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof FieldCapabilitiesResponse) {
                validResponses.add((FieldCapabilitiesResponse) element);
            }
        }
        final Map<String, Map<String, FieldCapabilities>> mergedResponses = new HashMap<>();
        Set<String> fields = validResponses.stream()
            .flatMap((r) -> r.getFieldsCaps().keySet().stream()).collect(Collectors.toSet());
        for (String field : fields) {
            List<Map.Entry<String, FieldCapabilities> > indexFields = validResponses.stream()
                .filter((r) -> r.getFieldsCaps().containsKey(field))
                .flatMap((r) -> r.getFieldsCaps().get(field).entrySet().stream())
                .collect(Collectors.toList());
            if (expandPerIndex) {
                // Adds one entry per index for each field
                final Map<String, FieldCapabilities> fieldCapMap = new HashMap<>();
                indexFields.stream()
                    .forEach((e) -> fieldCapMap.put(e.getKey(), e.getValue()));
                mergedResponses.put(field, fieldCapMap);
            } else {
                // Merge fields from different indices in a single entry named "_all"
                Set<String> types = new HashSet<>();
                boolean searchable = false;
                boolean aggregatable = false;
                for (Map.Entry<String, FieldCapabilities> indexField : indexFields) {
                    assert indexField.getValue().getTypes().length == 1;
                    types.add(indexField.getValue().getTypes()[0]);
                    searchable |= indexField.getValue().isSearchable();
                    aggregatable |= indexField.getValue().isAggregatable();
                }
                String[] typesArr = types.toArray(new String[types.size()]);
                Arrays.sort(typesArr);
                mergedResponses.put(field,
                    Collections.singletonMap("_all", new FieldCapabilities(field, searchable, aggregatable, typesArr)));
            }
        }
        return new FieldCapabilitiesResponse(mergedResponses);
    }
}
