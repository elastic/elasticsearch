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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 */
public abstract class TransportReplicatedMutationAction<
            Request extends ReplicatedMutationRequest<Request>,
            ReplicaRequest extends ReplicatedMutationRequest<ReplicaRequest>,
            Response extends ReplicationResponse
        > extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected TransportReplicatedMutationAction(Settings settings, String actionName, TransportService transportService,
            ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
            Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, request, replicaRequest, executor);
    }

    /**
     * Called on each replica node with a reference to the shard to modify.
     *
     * @return the translog location after the modification
     */
    protected abstract Translog.Location onReplicaShard(ReplicaRequest request, IndexShard indexShard);

    @Override
    protected final void shardOperationOnReplica(ReplicaRequest request) {
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Translog.Location location = onReplicaShard(request, indexShard);
        processAfterWrite(request.refresh(), indexShard, location);
    }

    protected final void processAfterWrite(boolean refresh, IndexShard indexShard, Translog.Location location) {
        if (refresh) {
            try {
                indexShard.refresh("refresh_flag_index");
            } catch (Throwable e) {
                // ignore
            }
        }
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null) {
            indexShard.sync(location);
        }
        indexShard.maybeFlush();
    }
}
