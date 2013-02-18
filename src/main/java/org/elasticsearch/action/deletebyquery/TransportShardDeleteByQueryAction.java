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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportShardDeleteByQueryAction extends TransportShardReplicationOperationAction<ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    @Inject
    public TransportShardDeleteByQueryAction(Settings settings, TransportService transportService,
                                             ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                             ShardStateAction shardStateAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ShardDeleteByQueryRequest newRequestInstance() {
        return new ShardDeleteByQueryRequest();
    }

    @Override
    protected ShardDeleteByQueryRequest newReplicaRequestInstance() {
        return new ShardDeleteByQueryRequest();
    }

    @Override
    protected ShardDeleteByQueryResponse newResponseInstance() {
        return new ShardDeleteByQueryResponse();
    }

    @Override
    protected String transportAction() {
        return DeleteByQueryAction.NAME + "/shard";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ShardDeleteByQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ShardDeleteByQueryRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.getIndex());
    }

    @Override
    protected PrimaryResponse<ShardDeleteByQueryResponse, ShardDeleteByQueryRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.getIndex()).shardSafe(shardRequest.shardId);
        Engine.DeleteByQuery deleteByQuery = indexShard.prepareDeleteByQuery(request.getQuerySource(), request.getFilteringAliases(), request.getTypes());
        indexShard.deleteByQuery(deleteByQuery);
        return new PrimaryResponse<ShardDeleteByQueryResponse, ShardDeleteByQueryRequest>(shardRequest.request, new ShardDeleteByQueryResponse(), null);
    }


    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.getIndex()).shardSafe(shardRequest.shardId);
        Engine.DeleteByQuery deleteByQuery = indexShard.prepareDeleteByQuery(request.getQuerySource(), request.getFilteringAliases(), request.getTypes());
        indexShard.deleteByQuery(deleteByQuery);
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, ShardDeleteByQueryRequest request) {
        GroupShardsIterator group = clusterService.operationRouting().deleteByQueryShards(clusterService.state(), request.getIndex(), request.getRouting());
        for (ShardIterator shardIt : group) {
            if (shardIt.shardId().id() == request.getShardId()) {
                return shardIt;
            }
        }
        throw new ElasticSearchIllegalStateException("No shards iterator found for shard [" + request.getShardId() + "]");
    }
}
