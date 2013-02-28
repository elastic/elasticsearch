/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.replication.TransportIndicesReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public class TransportDeleteByQueryAction extends TransportIndicesReplicationOperationAction<DeleteByQueryRequest, DeleteByQueryResponse, IndexDeleteByQueryRequest, IndexDeleteByQueryResponse, ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    @Inject
    public TransportDeleteByQueryAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        ThreadPool threadPool, TransportIndexDeleteByQueryAction indexDeleteByQueryAction) {
        super(settings, transportService, clusterService, threadPool, indexDeleteByQueryAction);
    }

    @Override
    protected Map<String, Set<String>> resolveRouting(ClusterState clusterState, DeleteByQueryRequest request) throws ElasticSearchException {
        return clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
    }

    @Override
    protected DeleteByQueryRequest newRequestInstance() {
        return new DeleteByQueryRequest();
    }

    @Override
    protected DeleteByQueryResponse newResponseInstance(DeleteByQueryRequest request, AtomicReferenceArray indexResponses) {
        DeleteByQueryResponse response = new DeleteByQueryResponse();
        for (int i = 0; i < indexResponses.length(); i++) {
            IndexDeleteByQueryResponse indexResponse = (IndexDeleteByQueryResponse) indexResponses.get(i);
            if (indexResponse != null) {
                response.getIndices().put(indexResponse.getIndex(), indexResponse);
            }
        }
        return response;
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected String transportAction() {
        return DeleteByQueryAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DeleteByQueryRequest replicationPingRequest) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DeleteByQueryRequest replicationPingRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, concreteIndices);
    }

    @Override
    protected IndexDeleteByQueryRequest newIndexRequestInstance(DeleteByQueryRequest request, String index, Set<String> routing) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(index, request.indices());
        return new IndexDeleteByQueryRequest(request, index, routing, filteringAliases);
    }
}
