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

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 */
public abstract class TransportBroadcastReplicationAction<Request extends BroadcastRequest, Response extends BroadcastResponse, ShardRequest extends ReplicationRequest, ShardResponse extends ReplicationResponse> extends HandledTransportAction<Request, Response> {

    private final TransportReplicationAction replicatedBroadcastShardAction;
    private final ClusterService clusterService;

    public TransportBroadcastReplicationAction(String name, Supplier<Request> request, Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                               TransportService transportService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, TransportReplicationAction replicatedBroadcastShardAction) {
        super(settings, name, threadPool, transportService, actionFilters, indexNameExpressionResolver, request);
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        List<ShardId> shards = shards(request, clusterState);
        final CopyOnWriteArrayList<ShardResponse> shardsResponses = new CopyOnWriteArrayList();
        if (shards.size() == 0) {
            finishAndNotifyListener(listener, shardsResponses);
        }
        final CountDown responsesCountDown = new CountDown(shards.size());
        for (final ShardId shardId : shards) {
            ActionListener<ShardResponse> shardActionListener = new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    shardsResponses.add(shardResponse);
                    logger.trace("{}: got response from {}", actionName, shardId);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.trace("{}: got failure from {}", actionName, shardId);
                    int totalNumCopies = clusterState.getMetaData().index(shardId.index().getName()).getNumberOfReplicas() + 1;
                    ShardResponse shardResponse = newShardResponse();
                    ReplicationResponse.ShardInfo.Failure[] failures;
                    if (TransportActions.isShardNotAvailableException(e)) {
                        failures = new ReplicationResponse.ShardInfo.Failure[0];
                    } else {
                        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(shardId.index().name(), shardId.id(), null, e, ExceptionsHelper.status(e), true);
                        failures = new ReplicationResponse.ShardInfo.Failure[totalNumCopies];
                        Arrays.fill(failures, failure);
                    }
                    shardResponse.setShardInfo(new ReplicationResponse.ShardInfo(totalNumCopies, 0, failures));
                    shardsResponses.add(shardResponse);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }
            };
            shardExecute(request, shardId, shardActionListener);
        }
    }

    protected void shardExecute(Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        replicatedBroadcastShardAction.execute(newShardRequest(request, shardId), shardActionListener);
    }

    /**
     * @return all shard ids the request should run on
     */
    protected List<ShardId> shards(Request request, ClusterState clusterState) {
        List<ShardId> shardIds = new ArrayList<>();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, request);
        for (String index : concreteIndices) {
            IndexMetaData indexMetaData = clusterState.metaData().getIndices().get(index);
            if (indexMetaData != null) {
                for (IntObjectCursor<IndexShardRoutingTable> shardRouting : clusterState.getRoutingTable().indicesRouting().get(index).getShards()) {
                    shardIds.add(shardRouting.value.shardId());
                }
            }
        }
        return shardIds;
    }

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    private void finishAndNotifyListener(ActionListener listener, CopyOnWriteArrayList<ShardResponse> shardsResponses) {
        logger.trace("{}: got all shard responses", actionName);
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.size(); i++) {
            ReplicationResponse shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // non active shard, ignore
            } else {
                failedShards += shardResponse.getShardInfo().getFailed();
                successfulShards += shardResponse.getShardInfo().getSuccessful();
                totalNumCopies += shardResponse.getShardInfo().getTotal();
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                for (ReplicationResponse.ShardInfo.Failure failure : shardResponse.getShardInfo().getFailures()) {
                    shardFailures.add(new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(new ShardId(failure.index(), failure.shardId()), failure.getCause())));
                }
            }
        }
        listener.onResponse(newResponse(successfulShards, failedShards, totalNumCopies, shardFailures));
    }

    protected abstract BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List<ShardOperationFailedException> shardFailures);
}
