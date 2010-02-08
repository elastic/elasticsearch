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

package org.elasticsearch.action.count;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
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
public class TransportCountAction extends TransportBroadcastOperationAction<CountRequest, CountResponse, ShardCountRequest, ShardCountResponse> {

    @Inject public TransportCountAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService, indicesService);
    }

    @Override protected String transportAction() {
        return TransportActions.COUNT;
    }

    @Override protected String transportShardAction() {
        return "indices/count/shard";
    }

    @Override protected CountRequest newRequest() {
        return new CountRequest();
    }

    @Override protected ShardCountRequest newShardRequest() {
        return new ShardCountRequest();
    }

    @Override protected ShardCountRequest newShardRequest(ShardRouting shard, CountRequest request) {
        return new ShardCountRequest(shard.index(), shard.id(), request.querySource(), request.minScore(), request.queryParserName(), request.types());
    }

    @Override protected ShardCountResponse newShardResponse() {
        return new ShardCountResponse();
    }

    @Override protected CountResponse newResponse(CountRequest request, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;
        long count = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            ShardCountResponse shardCountResponse = (ShardCountResponse) shardsResponses.get(i);
            if (shardCountResponse == null) {
                failedShards++;
            } else {
                count += shardCountResponse.count();
                successfulShards++;
            }
        }
        return new CountResponse(count, successfulShards, failedShards);
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected ShardCountResponse shardOperation(ShardCountRequest request) throws ElasticSearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.shardId());
        long count = indexShard.count(request.minScore(), request.querySource(), request.queryParserName(), request.types());
        return new ShardCountResponse(request.index(), request.shardId(), count);
    }
}
