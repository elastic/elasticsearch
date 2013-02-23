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

import org.elasticsearch.action.support.replication.TransportIndexReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportIndexDeleteByQueryAction extends TransportIndexReplicationOperationAction<IndexDeleteByQueryRequest, IndexDeleteByQueryResponse, ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    @Inject
    public TransportIndexDeleteByQueryAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                             ThreadPool threadPool, TransportShardDeleteByQueryAction shardDeleteByQueryAction) {
        super(settings, transportService, clusterService, threadPool, shardDeleteByQueryAction);
    }

    @Override
    protected IndexDeleteByQueryRequest newRequestInstance() {
        return new IndexDeleteByQueryRequest();
    }

    @Override
    protected IndexDeleteByQueryResponse newResponseInstance(IndexDeleteByQueryRequest request, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            if (shardsResponses.get(i) == null) {
                failedShards++;
            } else {
                successfulShards++;
            }
        }
        return new IndexDeleteByQueryResponse(request.index(), successfulShards, failedShards);
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected String transportAction() {
        return DeleteByQueryAction.NAME + "/index";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndexDeleteByQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndexDeleteByQueryRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected GroupShardsIterator shards(IndexDeleteByQueryRequest request) {
        return clusterService.operationRouting().deleteByQueryShards(clusterService.state(), request.index(), request.routing());
    }

    @Override
    protected ShardDeleteByQueryRequest newShardRequestInstance(IndexDeleteByQueryRequest request, int shardId) {
        return new ShardDeleteByQueryRequest(request, shardId);
    }
}
