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

package org.elasticsearch.indices.syncedflush;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 */
public class TransportSyncedFlushAction extends TransportShardReplicationOperationAction<SyncedFlushRequest, SyncedFlushRequest, SyncedFlushResponse, SyncedFlushReplicaResponse> {

    public static final String NAME = "indices:admin/syncedflush";

    @Inject
    public TransportSyncedFlushAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                      ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected SyncedFlushRequest newRequestInstance() {
        return new SyncedFlushRequest();
    }

    @Override
    protected SyncedFlushRequest newReplicaRequestInstance() {
        return new SyncedFlushRequest();
    }

    @Override
    protected SyncedFlushResponse newResponseInstance() {
        return new SyncedFlushResponse();
    }

    @Override
    protected SyncedFlushReplicaResponse newReplicaResponseInstance() {
        return new SyncedFlushReplicaResponse();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.FLUSH;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        // get all shards for id
        return clusterService.state().routingTable().index(request.concreteIndex()).shard(request.request().shardId().id()).shardsIt();
    }

    @Override
    protected Tuple<SyncedFlushResponse, SyncedFlushRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
        byte[] commitId = shardRequest.request.commitIds().get(clusterService.localNode().getId());
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());
        SyncedFlushResponse syncedFlushResponse = new SyncedFlushResponse(indexShard.syncFlushIfNoPendingChanges(shardRequest.request.syncId(), commitId), shardRequest.request.syncId());
        if (syncedFlushResponse.success() == false) {
            throw new ElasticsearchIllegalStateException("could not sync commit on primary");
        }
        return new Tuple<>(syncedFlushResponse, shardRequest.request);
    }

    @Override
    protected SyncedFlushReplicaResponse shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        byte[] commitId = shardRequest.request.commitIds().get(clusterService.localNode().getId());
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());
        SyncedFlushReplicaResponse syncedFlushReplicaResponse = new SyncedFlushReplicaResponse();
        boolean success = indexShard.syncFlushIfNoPendingChanges(shardRequest.request.syncId(), commitId);
        String message = success ? "synced flush succeeded" : "synced flush failed";
        syncedFlushReplicaResponse.setResult(success, shardRequest.request.index(), shardRequest.shardId.id(), shardRequest.getNodeId(), message);
        return syncedFlushReplicaResponse;
    }


    protected SyncedFlushResponse onAllReplicasResponded(SyncedFlushResponse finalResponse, CopyOnWriteArrayList<SyncedFlushReplicaResponse> replicaResponses) {
        List<ActionWriteResponse.ShardInfo.Failure> additionalFailures = new ArrayList<>();
        for (SyncedFlushReplicaResponse replicaResponse : replicaResponses) {
            if (replicaResponse.succeeded == false) {
                additionalFailures.add(new ActionWriteResponse.ShardInfo.Failure(replicaResponse.getIndex(), replicaResponse.getShardId(), replicaResponse.getNodeId(), replicaResponse.getReason(), RestStatus.CONFLICT, false));
            }
        }
        additionalFailures.addAll(Arrays.asList(finalResponse.getShardInfo().getFailures()));
        finalResponse.setShardInfo(new ActionWriteResponse.ShardInfo(finalResponse.getShardInfo().getTotal(), finalResponse.getShardInfo().getTotal() - additionalFailures.size(), additionalFailures.toArray(new ActionWriteResponse.ShardInfo.Failure[additionalFailures.size()])));
        return finalResponse;
    }
}
