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

package org.elasticsearch.action.delete.index;

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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportIndexDeleteAction extends TransportIndexReplicationOperationAction<IndexDeleteRequest, IndexDeleteResponse, ShardDeleteRequest, ShardDeleteRequest, ShardDeleteResponse> {

    @Inject
    public TransportIndexDeleteAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      ThreadPool threadPool, TransportShardDeleteAction deleteAction) {
        super(settings, transportService, clusterService, threadPool, deleteAction);
    }

    @Override
    protected IndexDeleteRequest newRequestInstance() {
        return new IndexDeleteRequest();
    }

    @Override
    protected IndexDeleteResponse newResponseInstance(IndexDeleteRequest request, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;
        ArrayList<ShardDeleteResponse> responses = new ArrayList<ShardDeleteResponse>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            if (shardsResponses.get(i) == null) {
                failedShards++;
            } else {
                responses.add((ShardDeleteResponse) shardsResponses.get(i));
                successfulShards++;
            }
        }
        return new IndexDeleteResponse(request.index(), successfulShards, failedShards, responses.toArray(new ShardDeleteResponse[responses.size()]));
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected String transportAction() {
        return "indices/index/delete";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndexDeleteRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndexDeleteRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected GroupShardsIterator shards(IndexDeleteRequest request) {
        return clusterService.operationRouting().broadcastDeleteShards(clusterService.state(), request.index());
    }

    @Override
    protected ShardDeleteRequest newShardRequestInstance(IndexDeleteRequest request, int shardId) {
        return new ShardDeleteRequest(request, shardId);
    }
}
