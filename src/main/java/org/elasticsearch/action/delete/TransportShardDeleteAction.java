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

package org.elasticsearch.action.delete;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportShardDeleteAction extends TransportShardReplicationOperationAction<ShardDeleteRequest, ShardDeleteRequest, ShardDeleteResponse> {

    private static final String ACTION_NAME = DeleteAction.NAME + "[s]";

    @Inject
    public TransportShardDeleteAction(Settings settings, TransportService transportService,
                                      ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                      ShardStateAction shardStateAction, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ShardDeleteRequest newRequestInstance() {
        return new ShardDeleteRequest();
    }

    @Override
    protected ShardDeleteRequest newReplicaRequestInstance() {
        return new ShardDeleteRequest();
    }

    @Override
    protected ShardDeleteResponse newResponseInstance() {
        return new ShardDeleteResponse();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected PrimaryResponse<ShardDeleteResponse, ShardDeleteRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        ShardDeleteRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId.getIndex()).shardSafe(shardRequest.shardId.id());
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version(), VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the version to happen on the replicas
        request.version(delete.version());

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_delete").force(false));
            } catch (Exception e) {
                // ignore
            }
        }


        ShardDeleteResponse response = new ShardDeleteResponse(delete.version(), delete.found());
        return new PrimaryResponse<>(shardRequest.request, response, null);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        ShardDeleteRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId.getIndex()).shardSafe(shardRequest.shardId.id());
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version(), VersionType.INTERNAL, Engine.Operation.Origin.REPLICA);

        // IndexDeleteAction doesn't support version type at the moment. Hard coded for the INTERNAL version
        delete = new Engine.Delete(delete, VersionType.INTERNAL.versionTypeForReplicationAndRecovery());

        assert delete.versionType().validateVersionForWrites(delete.version());

        indexShard.delete(delete);

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_delete").force(false));
            } catch (Exception e) {
                // ignore
            }
        }

    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        GroupShardsIterator group = clusterService.operationRouting().broadcastDeleteShards(clusterService.state(), request.concreteIndex());
        for (ShardIterator shardIt : group) {
            if (shardIt.shardId().id() == request.request().shardId()) {
                return shardIt;
            }
        }
        throw new ElasticsearchIllegalStateException("No shards iterator found for shard [" + request.request().shardId() + "]");
    }
}
