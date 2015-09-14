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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Flush Action.
 */
public class TransportFlushAction extends TransportBroadcastReplicationAction<FlushRequest, FlushResponse, ShardFlushRequest, ActionWriteResponse> {

    @Inject
    public TransportFlushAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                TransportService transportService, ActionFilters actionFilters,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                TransportShardFlushAction replicatedFlushAction) {
        super(FlushAction.NAME, FlushRequest::new, settings, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, replicatedFlushAction);
    }

    @Override
    protected ActionWriteResponse newShardResponse() {
        return new ActionWriteResponse();
    }

    @Override
    protected ShardFlushRequest newShardRequest(FlushRequest request, ShardId shardId) {
        return new ShardFlushRequest(request).setShardId(shardId);
    }

    @Override
    protected FlushResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List<ShardOperationFailedException> shardFailures) {
        return new FlushResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
