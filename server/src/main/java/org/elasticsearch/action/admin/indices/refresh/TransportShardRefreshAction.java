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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;


public class TransportShardRefreshAction
        extends TransportReplicationAction<BasicReplicationRequest, BasicReplicationRequest, ReplicationResponse> {

    public static final String NAME = RefreshAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);

    @Inject
    public TransportShardRefreshAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                       ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                BasicReplicationRequest::new, BasicReplicationRequest::new, ThreadPool.Names.REFRESH);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(BasicReplicationRequest shardRequest, IndexShard primary,
            ActionListener<PrimaryResult<BasicReplicationRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            primary.refresh("api");
            logger.trace("{} refresh request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(BasicReplicationRequest request, IndexShard replica) {
        replica.refresh("api");
        logger.trace("{} refresh request executed on replica", replica.shardId());
        return new ReplicaResult();
    }
}
