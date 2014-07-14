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

package org.elasticsearch.action.delete.index;

import org.elasticsearch.action.ShardOperationFailedException;
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

import java.util.List;

/**
 *
 */
public class TransportIndexDeleteAction extends TransportIndexReplicationOperationAction<IndexDeleteRequest, IndexDeleteResponse, ShardDeleteRequest, ShardDeleteRequest, ShardDeleteResponse> {

    private static final String ACTION_NAME = "indices/index/delete";

    @Inject
    public TransportIndexDeleteAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      ThreadPool threadPool, TransportShardDeleteAction deleteAction) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, deleteAction);
    }

    @Override
    protected IndexDeleteRequest newRequestInstance() {
        return new IndexDeleteRequest();
    }

    @Override
    protected IndexDeleteResponse newResponseInstance(IndexDeleteRequest request, List<ShardDeleteResponse> shardDeleteResponses, int failuresCount, List<ShardOperationFailedException> shardFailures) {
        return new IndexDeleteResponse(request.index(), failuresCount, shardDeleteResponses.toArray(new ShardDeleteResponse[shardDeleteResponses.size()]));
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
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
