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

package org.elasticsearch.action.deletebyquery;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportShardDeleteByQueryAction extends TransportShardReplicationOperationAction<ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    @Inject public TransportShardDeleteByQueryAction(Settings settings, TransportService transportService,
                                                     ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                                     ShardStateAction shardStateAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
    }

    @Override protected ShardDeleteByQueryRequest newRequestInstance() {
        return new ShardDeleteByQueryRequest();
    }

    @Override protected ShardDeleteByQueryResponse newResponseInstance() {
        return new ShardDeleteByQueryResponse();
    }

    @Override protected String transportAction() {
        return "indices/index/shard/deleteByQuery";
    }

    @Override protected ShardDeleteByQueryResponse shardOperationOnPrimary(ShardOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        indexShard(shardRequest).deleteByQuery(request.querySource(), request.queryParserName(), request.types());
        return new ShardDeleteByQueryResponse();
    }

    @Override protected void shardOperationOnBackup(ShardOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        indexShard(shardRequest).deleteByQuery(request.querySource(), request.queryParserName(), request.types());
    }

    @Override protected ShardsIterator shards(ShardDeleteByQueryRequest request) {
        GroupShardsIterator group = indicesService.indexServiceSafe(request.index()).operationRouting().deleteByQueryShards(clusterService.state());
        for (ShardsIterator shards : group) {
            if (shards.shardId().id() == request.shardId()) {
                return shards;
            }
        }
        throw new ElasticSearchIllegalStateException("No shards iterator found for shard [" + request.shardId() + "]");
    }
}
