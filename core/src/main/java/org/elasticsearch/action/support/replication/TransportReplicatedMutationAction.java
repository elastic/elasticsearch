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

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 */
public abstract class TransportReplicatedMutationAction<
            Request extends ReplicatedMutationRequest<Request>,
            Response extends ReplicatedMutationResponse
        > extends TransportReplicationAction<Request, Request, Response> {

    protected TransportReplicatedMutationAction(Settings settings, String actionName, TransportService transportService,
            ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
            String executor) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, request, request, executor);
    }

    /**
     * Called with a reference to the primary shard.
     *
     * @return the result of the write - basically just the response to send back and the translog location of the {@linkplain IndexShard}
     *         after the write was completed
     */
    protected abstract WriteResult<Response> onPrimaryShard(IndexService indexService, IndexShard indexShard, Request request)
            throws Exception;

    /**
     * Called once per replica with a reference to the {@linkplain IndexShard} to modify.
     *
     * @return the translog location of the {@linkplain IndexShard} after the write was completed
     */
    protected abstract Translog.Location onReplicaShard(Request request, IndexShard indexShard);

    @Override
    protected Request shardOperationOnPrimary(Request request, ActionListener<Response> listener) throws Exception {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().id());
        WriteResult<Response> result = onPrimaryShard(indexService, indexShard, request);
        processAfterWrite(request.isRefresh(), indexShard, result.location);
        if (request.isRefresh()) {
            // Only setForcedRefresh if it is true because this can touch every item in a bulk request
            result.response.setForcedRefresh(true);
        }
        if (request.shouldBlockUntilRefresh() && false == request.isRefresh()) {
            indexShard.addRefreshListener(result.location, forcedRefresh -> {
                if (forcedRefresh) {
                    logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                    result.response.setForcedRefresh(true);
                }
                listener.onResponse(result.response);
            });
        } else {
            listener.onResponse(result.response);
        }
        return request;
    }

    @Override
    protected final void shardOperationOnReplica(Request request, ActionListener<TransportResponse.Empty> listener) {
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Translog.Location location = onReplicaShard(request, indexShard);
        processAfterWrite(request.isRefresh(), indexShard, location);
        if (request.shouldBlockUntilRefresh() && false == request.isRefresh() && location != null) {
            indexShard.addRefreshListener(location, forcedRefresh -> {
                logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                // TODO mark the response?!?
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            });
        } else {
            listener.onResponse(TransportResponse.Empty.INSTANCE);
        }
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

    protected static class WriteResult<T extends ReplicatedMutationResponse> {
        public final T response;
        public final Translog.Location location;

        public WriteResult(T response, Translog.Location location) {
            this.response = response;
            this.location = location;
        }

        public T response() {
            // this sets total, pending and failed to 0 and this is ok, because we will embed this into the replica
            // request and not use it
            response.setShardInfo(new ReplicationResponse.ShardInfo());
            return response;
        }
    }
}
