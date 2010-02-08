/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.admin.indices.refresh;

import com.google.inject.Inject;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportShardRefreshAction extends TransportShardReplicationOperationAction<ShardRefreshRequest, ShardRefreshResponse> {

    @Inject public TransportShardRefreshAction(Settings settings, TransportService transportService,
                                               ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                               ShardStateAction shardStateAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
    }

    @Override protected ShardRefreshRequest newRequestInstance() {
        return new ShardRefreshRequest();
    }

    @Override protected ShardRefreshResponse newResponseInstance() {
        return new ShardRefreshResponse();
    }

    @Override protected String transportAction() {
        return "indices/index/shard/refresh";
    }

    @Override protected ShardRefreshResponse shardOperationOnPrimary(ShardOperationRequest shardRequest) {
        ShardRefreshRequest request = shardRequest.request;
        indexShard(shardRequest).refresh(request.waitForOperations());
        return new ShardRefreshResponse();
    }

    @Override protected void shardOperationOnBackup(ShardOperationRequest shardRequest) {
        ShardRefreshRequest request = shardRequest.request;
        indexShard(shardRequest).refresh(request.waitForOperations());
    }

    @Override protected ShardsIterator shards(ShardRefreshRequest request) {
        return clusterService.state().routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }
}