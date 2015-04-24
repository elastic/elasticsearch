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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public abstract class TransportIndicesReplicationOperationAction<Request extends IndicesReplicationOperationRequest, Response extends ActionResponse, IndexRequest extends IndexReplicationOperationRequest, IndexResponse extends ActionResponse,
        ShardRequest extends ShardReplicationOperationRequest, ShardResponse extends ActionWriteResponse>
        extends HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportIndexReplicationOperationAction<IndexRequest, IndexResponse, ShardRequest, ShardResponse> indexAction;

    protected TransportIndicesReplicationOperationAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                         TransportIndexReplicationOperationAction<IndexRequest, IndexResponse, ShardRequest, ShardResponse> indexAction, ActionFilters actionFilters,
                                                         Class<Request> request) {
        super(settings, actionName, threadPool, transportService, actionFilters, request);
        this.clusterService = clusterService;
        this.indexAction = indexAction;
    }


    protected abstract Map<String, Set<String>> resolveRouting(ClusterState clusterState, Request request) throws ElasticsearchException;

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        ClusterState clusterState = clusterService.state();
        ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
        if (blockException != null) {
            throw blockException;
        }
        // get actual indices
        String[] concreteIndices = clusterState.metaData().concreteIndices(request.indicesOptions(), request.indices());
        blockException = checkRequestBlock(clusterState, request, concreteIndices);
        if (blockException != null) {
            throw blockException;
        }

        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<>(concreteIndices.length);
        final long startTimeInMillis = System.currentTimeMillis();

        Map<String, Set<String>> routingMap = resolveRouting(clusterState, request);
        if (concreteIndices.length == 0) {
            listener.onResponse(newResponseInstance(request, indexResponses));
        } else {
            for (final String index : concreteIndices) {
                Set<String> routing = null;
                if (routingMap != null) {
                    routing = routingMap.get(index);
                }
                IndexRequest indexRequest = newIndexRequestInstance(request, index, routing, startTimeInMillis);
                // no threading needed, all is done on the index replication one
                indexRequest.listenerThreaded(false);
                indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse result) {
                        indexResponses.set(indexCounter.getAndIncrement(), result);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(newResponseInstance(request, indexResponses));
                        }
                    }
    
                    @Override
                    public void onFailure(Throwable e) {
                        int index = indexCounter.getAndIncrement();
                        if (accumulateExceptions()) {
                            indexResponses.set(index, e);
                        }
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(newResponseInstance(request, indexResponses));
                        }
                    }
                });
            }
        }
    }

    protected abstract Response newResponseInstance(Request request, AtomicReferenceArray indexResponses);

    protected abstract IndexRequest newIndexRequestInstance(Request request, String index, Set<String> routing, long startTimeInMillis);

    protected abstract boolean accumulateExceptions();

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);
}
