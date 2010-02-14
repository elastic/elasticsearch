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

package org.elasticsearch.action.admin.indices.flush;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportFlushAction extends TransportBroadcastOperationAction<FlushRequest, FlushResponse, ShardFlushRequest, ShardFlushResponse> {

    @Inject public TransportFlushAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService, indicesService);
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.FLUSH;
    }

    @Override protected String transportShardAction() {
        return "indices/flush/shard";
    }

    @Override protected FlushRequest newRequest() {
        return new FlushRequest();
    }

    @Override protected FlushResponse newResponse(FlushRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            ShardFlushResponse shardCountResponse = (ShardFlushResponse) shardsResponses.get(i);
            if (shardCountResponse == null) {
                failedShards++;
            } else {
                successfulShards++;
            }
        }
        return new FlushResponse(successfulShards, failedShards);
    }

    @Override protected ShardFlushRequest newShardRequest() {
        return new ShardFlushRequest();
    }

    @Override protected ShardFlushRequest newShardRequest(ShardRouting shard, FlushRequest request) {
        return new ShardFlushRequest(shard.index(), shard.id());
    }

    @Override protected ShardFlushResponse newShardResponse() {
        return new ShardFlushResponse();
    }

    @Override protected ShardFlushResponse shardOperation(ShardFlushRequest request) throws ElasticSearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.shardId());
        indexShard.flush();
        return new ShardFlushResponse(request.index(), request.shardId());
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override protected GroupShardsIterator shards(FlushRequest request, ClusterState clusterState) {
        return clusterState.routingTable().allShardsGrouped(request.indices());
    }

    //    @Override protected FlushRequest newRequestInstance() {
//        return new FlushRequest();
//    }
//
//    @Override protected FlushResponse newResponseInstance(FlushRequest request, AtomicReferenceArray indexResponses) {
//        FlushResponse response = new FlushResponse();
//        for (int i = 0; i < indexResponses.length(); i++) {
//            IndexFlushResponse indexFlushResponse = (IndexFlushResponse) indexResponses.get(i);
//            if (indexFlushResponse != null) {
//                response.indices().put(indexFlushResponse.index(), indexFlushResponse);
//            }
//        }
//        return response;
//    }
//
//    @Override protected boolean accumulateExceptions() {
//        return false;
//    }
//
//    @Override protected String transportAction() {
//        return TransportActions.Admin.Indices.FLUSH;
//    }
//
//    @Override protected IndexFlushRequest newIndexRequestInstance(FlushRequest request, String index) {
//        return new IndexFlushRequest(request, index);
//    }
}