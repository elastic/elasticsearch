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

package org.elasticsearch.legacy.action.deletebyquery;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.DestructiveOperations;
import org.elasticsearch.legacy.action.support.replication.TransportIndicesReplicationOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.node.settings.NodeSettingsService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public class TransportDeleteByQueryAction extends TransportIndicesReplicationOperationAction<DeleteByQueryRequest, DeleteByQueryResponse, IndexDeleteByQueryRequest, IndexDeleteByQueryResponse, ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteByQueryAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        ThreadPool threadPool, TransportIndexDeleteByQueryAction indexDeleteByQueryAction,
                                        NodeSettingsService nodeSettingsService) {
        super(settings, DeleteByQueryAction.NAME, transportService, clusterService, threadPool, indexDeleteByQueryAction);
        this.destructiveOperations = new DestructiveOperations(logger, settings, nodeSettingsService);
    }

    @Override
    protected void doExecute(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(request, listener);
    }

    @Override
    protected Map<String, Set<String>> resolveRouting(ClusterState clusterState, DeleteByQueryRequest request) throws ElasticsearchException {
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
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DeleteByQueryRequest replicationPingRequest) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DeleteByQueryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, concreteIndices);
    }

    @Override
    protected IndexDeleteByQueryRequest newIndexRequestInstance(DeleteByQueryRequest request, String index, Set<String> routing, long startTimeInMillis) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(index, request.indices());
        return new IndexDeleteByQueryRequest(request, index, routing, filteringAliases, startTimeInMillis);
    }
}
