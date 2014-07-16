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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public class TransportGetFieldMappingsAction extends TransportAction<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    private final ClusterService clusterService;
    private final TransportGetFieldMappingsIndexAction shardAction;

    @Inject
    public TransportGetFieldMappingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, TransportGetFieldMappingsIndexAction shardAction) {
        super(settings, GetFieldMappingsAction.NAME, threadPool);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
        transportService.registerHandler(actionName, new TransportHandler());
    }

    @Override
    protected void doExecute(GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = clusterState.metaData().concreteIndices(request.indicesOptions(), request.indices());
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<>(concreteIndices.length);

        if (concreteIndices.length == 0) {
            listener.onResponse(new GetFieldMappingsResponse());
        } else {
            boolean probablySingleFieldRequest = concreteIndices.length == 1 && request.types().length == 1 && request.fields().length == 1;
            for (final String index : concreteIndices) {
                GetFieldMappingsIndexRequest shardRequest = new GetFieldMappingsIndexRequest(request, index, probablySingleFieldRequest);
                // no threading needed, all is done on the index replication one
                shardRequest.listenerThreaded(false);
                shardAction.execute(shardRequest, new ActionListener<GetFieldMappingsResponse>() {
                    @Override
                    public void onResponse(GetFieldMappingsResponse result) {
                        indexResponses.set(indexCounter.getAndIncrement(), result);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
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
        MapBuilder<String, ImmutableMap<String, ImmutableMap<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mergedResponses = MapBuilder.newMapBuilder();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof GetFieldMappingsResponse) {
                GetFieldMappingsResponse response = (GetFieldMappingsResponse) element;
                mergedResponses.putAll(response.mappings());
            }
        }
        return new GetFieldMappingsResponse(mergedResponses.immutableMap());
    }

    private class TransportHandler extends BaseTransportRequestHandler<GetFieldMappingsRequest> {

        @Override
        public GetFieldMappingsRequest newInstance() {
            return new GetFieldMappingsRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final GetFieldMappingsRequest request, final TransportChannel channel) throws Exception {
            // no need for a threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<GetFieldMappingsResponse>() {
                @Override
                public void onResponse(GetFieldMappingsResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [" + actionName + "] and request [" + request + "]", e1);
                    }
                }
            });
        }
    }
}
