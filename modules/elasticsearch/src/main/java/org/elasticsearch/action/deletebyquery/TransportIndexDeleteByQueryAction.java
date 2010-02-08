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
import org.elasticsearch.action.support.replication.TransportIndexReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportIndexDeleteByQueryAction extends TransportIndexReplicationOperationAction<IndexDeleteByQueryRequest, IndexDeleteByQueryResponse, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    @Inject public TransportIndexDeleteByQueryAction(Settings settings, ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                                     ThreadPool threadPool, TransportShardDeleteByQueryAction shardDeleteByQueryAction) {
        super(settings, transportService, threadPool, shardDeleteByQueryAction);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override protected IndexDeleteByQueryRequest newRequestInstance() {
        return new IndexDeleteByQueryRequest();
    }

    @Override protected IndexDeleteByQueryResponse newResponseInstance(IndexDeleteByQueryRequest request, AtomicReferenceArray shardsResponses) {
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

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected String transportAction() {
        return "indices/index/deleteByQuery";
    }

    @Override protected GroupShardsIterator shards(IndexDeleteByQueryRequest request) {
        return indicesService.indexServiceSafe(request.index()).operationRouting().deleteByQueryShards(clusterService.state());
    }

    @Override protected ShardDeleteByQueryRequest newShardRequestInstance(IndexDeleteByQueryRequest request, int shardId) {
        return new ShardDeleteByQueryRequest(request, shardId);
    }
}