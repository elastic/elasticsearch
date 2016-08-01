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

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportShardRefreshAction
        extends TransportReplicationAction<BasicReplicationRequest, BasicReplicationRequest, ReplicationResponse> {

    public static final String NAME = RefreshAction.NAME + "[s]";

    @Inject
    public TransportShardRefreshAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, BasicReplicationRequest::new, BasicReplicationRequest::new, ThreadPool.Names.REFRESH);
    }

    @Override
    protected ReplicationResponse newResponseInstance() {
        return new ReplicationResponse();
    }

    @Override
    protected PrimaryResult shardOperationOnPrimary(BasicReplicationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId().getIndex()).getShard(shardRequest.shardId().id());
        indexShard.refresh("api");
        logger.trace("{} refresh request executed on primary", indexShard.shardId());
        return new PrimaryResult(shardRequest, new ReplicationResponse());
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(BasicReplicationRequest request) {
        final ShardId shardId = request.shardId();
        IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        indexShard.refresh("api");
        logger.trace("{} refresh request executed on replica", indexShard.shardId());
        return new ReplicaResult();
    }

    @Override
    protected boolean checkWriteConsistency() {
        return false;
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.METADATA_WRITE;
    }

    @Override
    protected ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.METADATA_WRITE;
    }

    @Override
    protected boolean shouldExecuteReplication(Settings settings) {
        return true;
    }
}
