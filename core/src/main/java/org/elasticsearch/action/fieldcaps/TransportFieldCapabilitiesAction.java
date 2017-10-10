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
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    private final ClusterService clusterService;
    private final TransportFieldCapabilitiesIndexAction shardAction;
    private final RemoteClusterService remoteClusterService;
    private final TransportService transportService;

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
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.transportService = transportService;
        this.shardAction = shardAction;
    }

    @Override
    protected void doExecute(FieldCapabilitiesRequest request,
                             final ActionListener<FieldCapabilitiesResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(),
            request.indices(), idx -> indexNameExpressionResolver.hasIndexOrAlias(idx, clusterState));
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (remoteClusterIndices.isEmpty() == false && localIndices.indices().length == 0) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote
            // indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
        }
        final int totalNumRequest = concreteIndices.length + remoteClusterIndices.size();
        final CountDown completionCounter = new CountDown(totalNumRequest);
        final List<FieldCapabilitiesIndexResponse> indexResponses = Collections.synchronizedList(new ArrayList<>());
        final Runnable onResponse = () -> {
            if (completionCounter.countDown()) {
                if (request.isMergeResults()) {
                    listener.onResponse(merge(indexResponses));
                } else {
                    listener.onResponse(new FieldCapabilitiesResponse(indexResponses));
                }
            }
        };
        if (totalNumRequest == 0) {
            listener.onResponse(new FieldCapabilitiesResponse());
        } else {
            ActionListener<FieldCapabilitiesIndexResponse> innerListener = new ActionListener<FieldCapabilitiesIndexResponse>() {
                @Override
                public void onResponse(FieldCapabilitiesIndexResponse result) {
                    indexResponses.add(result);
                    onResponse.run();
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO we should somehow inform the user that we failed
                    onResponse.run();
                }
            };
            for (String index : concreteIndices) {
                shardAction.execute(new FieldCapabilitiesIndexRequest(request.fields(), index), innerListener);
            }

            // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
            // send us back all individual index results.
            for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                String clusterAlias = remoteIndices.getKey();
                OriginalIndices originalIndices = remoteIndices.getValue();
                // if we are connected this is basically a no-op, if we are not we try to connect in parallel in a non-blocking fashion
                remoteClusterService.ensureConnected(clusterAlias, ActionListener.wrap(v -> {
                    Transport.Connection connection = remoteClusterService.getConnection(clusterAlias);
                    FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
                    remoteRequest.setMergeResults(false); // we need to merge on this node
                    remoteRequest.indicesOptions(originalIndices.indicesOptions());
                    remoteRequest.indices(originalIndices.indices());
                    remoteRequest.fields(request.fields());
                    transportService.sendRequest(connection, FieldCapabilitiesAction.NAME, remoteRequest, TransportRequestOptions.EMPTY,
                        new TransportResponseHandler<FieldCapabilitiesResponse>() {

                            @Override
                            public FieldCapabilitiesResponse newInstance() {
                                return new FieldCapabilitiesResponse();
                            }

                            @Override
                            public void handleResponse(FieldCapabilitiesResponse response) {
                                try {
                                    for (FieldCapabilitiesIndexResponse res : response.getIndexResponses()) {
                                        indexResponses.add(new FieldCapabilitiesIndexResponse(RemoteClusterAware.
                                            buildRemoteIndexName(clusterAlias, res.getIndexName()), res.get()));
                                    }
                                } finally {
                                    onResponse.run();
                                }
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onResponse.run();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }
                        });
                }, e -> onResponse.run()));
            }

        }
    }

    private FieldCapabilitiesResponse merge(List<FieldCapabilitiesIndexResponse> indexResponses) {
        Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<> ();
        for (FieldCapabilitiesIndexResponse response : indexResponses) {
            innerMerge(responseMapBuilder, response.getIndexName(), response.get());
        }

        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry :
            responseMapBuilder.entrySet()) {
            Map<String, FieldCapabilities> typeMap = new HashMap<>();
            boolean multiTypes = entry.getValue().size() > 1;
            for (Map.Entry<String, FieldCapabilities.Builder> fieldEntry :
                entry.getValue().entrySet()) {
                typeMap.put(fieldEntry.getKey(), fieldEntry.getValue().build(multiTypes));
            }
            responseMap.put(entry.getKey(), typeMap);
        }

        return new FieldCapabilitiesResponse(responseMap);
    }

    private void innerMerge(Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder, String indexName,
                            Map<String, FieldCapabilities> map) {
        for (Map.Entry<String, FieldCapabilities> entry : map.entrySet()) {
            final String field = entry.getKey();
            final FieldCapabilities fieldCap = entry.getValue();
            Map<String, FieldCapabilities.Builder> typeMap = responseMapBuilder.computeIfAbsent(field, f -> new HashMap<>());
            FieldCapabilities.Builder builder = typeMap.computeIfAbsent(fieldCap.getType(), key -> new FieldCapabilities.Builder(field,
                key));
            builder.add(indexName, fieldCap.isSearchable(), fieldCap.isAggregatable());
        }
    }
}
