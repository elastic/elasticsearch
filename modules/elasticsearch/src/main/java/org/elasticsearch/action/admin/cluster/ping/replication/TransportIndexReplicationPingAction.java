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

package org.elasticsearch.action.admin.cluster.ping.replication;

import com.google.inject.Inject;
import org.elasticsearch.action.support.replication.TransportIndexReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportIndexReplicationPingAction extends TransportIndexReplicationOperationAction<IndexReplicationPingRequest, IndexReplicationPingResponse, ShardReplicationPingRequest, ShardReplicationPingResponse> {

    private final ClusterService clusterService;

    @Inject public TransportIndexReplicationPingAction(Settings settings, ClusterService clusterService,
                                                       TransportService transportService, ThreadPool threadPool,
                                                       TransportShardReplicationPingAction shardReplicationPingAction) {
        super(settings, transportService, threadPool, shardReplicationPingAction);
        this.clusterService = clusterService;
    }

    @Override protected IndexReplicationPingRequest newRequestInstance() {
        return new IndexReplicationPingRequest();
    }

    @Override protected IndexReplicationPingResponse newResponseInstance(IndexReplicationPingRequest request, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            if (shardsResponses.get(i) == null) {
                failedShards++;
            } else {
                successfulShards++;
            }
        }
        return new IndexReplicationPingResponse(request.index(), successfulShards, failedShards);
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected String transportAction() {
        return "ping/replication/index";
    }

    @Override protected GroupShardsIterator shards(IndexReplicationPingRequest indexRequest) {
        IndexRoutingTable indexRouting = clusterService.state().routingTable().index(indexRequest.index());
        if (indexRouting == null) {
            throw new IndexMissingException(new Index(indexRequest.index()));
        }
        return indexRouting.groupByShardsIt();
    }

    @Override protected ShardReplicationPingRequest newShardRequestInstance(IndexReplicationPingRequest indexRequest, int shardId) {
        return new ShardReplicationPingRequest(indexRequest, shardId);
    }
}