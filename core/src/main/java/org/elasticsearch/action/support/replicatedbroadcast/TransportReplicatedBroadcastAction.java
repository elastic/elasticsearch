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

package org.elasticsearch.action.support.replicatedbroadcast;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Refresh action.
 */
public abstract class TransportReplicatedBroadcastAction<Request extends ReplicatedBroadcastRequest, Response extends ReplicatedBroadcastResponse, ShardRequest extends ReplicatedBroadcastShardRequest, ShardResponse extends ReplicatedBroadcastShardResponse> extends HandledTransportAction<Request, Response> {

    private final TransportReplicatedBroadcastShardAction replicatedBroadcastShardAction;
    private final ClusterService clusterService;

    public TransportReplicatedBroadcastAction(String name, Class<Request> request, Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                              TransportService transportService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, TransportReplicatedBroadcastShardAction replicatedBroadcastShardAction) {
        super(settings, name, threadPool, transportService, actionFilters, indexNameExpressionResolver,request);
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        GroupShardsIterator groupShardsIterator = clusterService.operationRouting().searchShards(clusterService.state(), indexNameExpressionResolver.concreteIndices(clusterService.state(), request), null, null);
        final CopyOnWriteArrayList<ShardResponse> shardsResponses = new CopyOnWriteArrayList();
        final CountDown responsesCountDown = new CountDown(groupShardsIterator.size());
        if (responsesCountDown.isCountedDown() == false) {
            for (final ShardsIterator shardsIterator : groupShardsIterator) {
                final ShardRouting shardRouting = shardsIterator.nextOrNull();
                if (shardRouting == null) {
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                } else {
                    final ShardId shardId = shardRouting.shardId();
                    replicatedBroadcastShardAction.execute(newShardRequest(shardId, request), new ActionListener<ShardResponse>() {
                        @Override
                        public void onResponse(ShardResponse shardResponse) {
                            shardsResponses.add(shardResponse);
                            if (responsesCountDown.countDown()) {
                                logger.trace("replicated broadcast: got response from {}", shardResponse.getShardId());
                                finishAndNotifyListener(listener, shardsResponses);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.trace("replicated broadcast: got failure from {}", shardId);
                            int totalNumCopies = clusterService.state().getMetaData().index(shardId.index().getName()).getNumberOfReplicas() + 1;
                            ShardResponse shardResponse = newShardResponse(totalNumCopies, shardId);
                            ActionWriteResponse.ShardInfo.Failure failure = new ActionWriteResponse.ShardInfo.Failure(shardId.index().name(), shardId.id(), null, e, ExceptionsHelper.status(e), true);
                            ActionWriteResponse.ShardInfo.Failure[] failures = new ActionWriteResponse.ShardInfo.Failure[totalNumCopies];
                            Arrays.fill(failures, failure);
                            shardResponse.setShardInfo(new ActionWriteResponse.ShardInfo(totalNumCopies, 0, failures));
                            shardsResponses.add(shardResponse);
                            if (responsesCountDown.countDown()) {
                                finishAndNotifyListener(listener, shardsResponses);
                            }
                        }
                    });
                }
            }
        } else {
            finishAndNotifyListener(listener, shardsResponses);
        }
    }

    protected abstract ShardResponse newShardResponse(int totalNumCopies, ShardId shardId);

    protected abstract ShardRequest newShardRequest(ShardId shardId, Request request);

    private void finishAndNotifyListener(ActionListener listener, CopyOnWriteArrayList<ShardResponse> shardsResponses) {
        logger.trace("replicated broadcast: got all shard responses");
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.size(); i++) {
            ReplicatedBroadcastShardResponse shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // non active shard, ignore
            } else {
                failedShards += shardResponse.getShardInfo().getFailed();
                successfulShards += shardResponse.getShardInfo().getSuccessful();
                totalNumCopies += shardResponse.getTotalNumCopies();
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                for (ActionWriteResponse.ShardInfo.Failure failure : shardResponse.getShardInfo().getFailures()) {
                    shardFailures.add(new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(new ShardId(failure.index(), failure.shardId()), failure.getCause())));
                }
            }
        }
        listener.onResponse(newResponse(successfulShards, failedShards, totalNumCopies, shardFailures));
    }

    protected abstract ReplicatedBroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List<ShardOperationFailedException> shardFailures);
}
