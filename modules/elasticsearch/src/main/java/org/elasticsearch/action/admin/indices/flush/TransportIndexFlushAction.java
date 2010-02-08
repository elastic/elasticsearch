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
public class TransportIndexFlushAction extends TransportIndexReplicationOperationAction<IndexFlushRequest, IndexFlushResponse, ShardFlushRequest, ShardFlushResponse> {

    private final ClusterService clusterService;

    @Inject public TransportIndexFlushAction(Settings settings, ClusterService clusterService,
                                             TransportService transportService, ThreadPool threadPool,
                                             TransportShardFlushAction shardFlushAction) {
        super(settings, transportService, threadPool, shardFlushAction);
        this.clusterService = clusterService;
    }

    @Override protected IndexFlushRequest newRequestInstance() {
        return new IndexFlushRequest();
    }

    @Override protected IndexFlushResponse newResponseInstance(IndexFlushRequest indexFlushReqest, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            if (shardsResponses.get(i) == null) {
                failedShards++;
            } else {
                successfulShards++;
            }
        }
        return new IndexFlushResponse(indexFlushReqest.index(), successfulShards, failedShards);
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected String transportAction() {
        return "indices/index/flush";
    }

    @Override protected GroupShardsIterator shards(IndexFlushRequest indexFlushRequest) {
        IndexRoutingTable indexRouting = clusterService.state().routingTable().index(indexFlushRequest.index());
        if (indexRouting == null) {
            throw new IndexMissingException(new Index(indexFlushRequest.index()));
        }
        return indexRouting.groupByShardsIt();
    }

    @Override protected ShardFlushRequest newShardRequestInstance(IndexFlushRequest indexFlushRequest, int shardId) {
        return new ShardFlushRequest(indexFlushRequest, shardId);
    }
}