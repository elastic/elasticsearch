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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replicatedbroadcast.ReplicatedBroadcastResponse;
import org.elasticsearch.action.support.replicatedbroadcast.ReplicatedBroadcastShardRequest;
import org.elasticsearch.action.support.replicatedbroadcast.ReplicatedBroadcastShardResponse;
import org.elasticsearch.action.support.replicatedbroadcast.TransportReplicatedBroadcastAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Refresh action.
 */
public class TransportRefreshAction extends TransportReplicatedBroadcastAction<RefreshRequest, RefreshResponse, ReplicatedBroadcastShardRequest, ReplicatedBroadcastShardResponse> {

    @Inject
    public TransportRefreshAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  TransportShardRefreshAction shardRefreshAction) {
        super(RefreshAction.NAME, RefreshRequest.class, settings, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, shardRefreshAction);
    }

    @Override
    protected ReplicatedBroadcastShardResponse newShardResponse(int totalNumCopies, ShardId shardId) {
        return new ReplicatedBroadcastShardResponse(shardId, totalNumCopies);
    }

    @Override
    protected ReplicatedBroadcastShardRequest newShardRequest(ShardId shardId, RefreshRequest request) {
        return new ReplicatedBroadcastShardRequest(shardId);
    }

    @Override
    protected ReplicatedBroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List<ShardOperationFailedException> shardFailures) {
        return new RefreshResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
