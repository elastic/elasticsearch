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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.replicatedrefresh.ReplicatedRefreshRequest;
import org.elasticsearch.action.replicatedrefresh.ReplicatedRefreshResponse;
import org.elasticsearch.action.replicatedrefresh.TransportReplicatedRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Refresh action.
 */
public class TransportRefreshAction extends HandledTransportAction<RefreshRequest, RefreshResponse> {

    private final TransportReplicatedRefreshAction replicatedRefreshAction;
    ClusterService clusterService;

    @Inject
    public TransportRefreshAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService,
                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, TransportReplicatedRefreshAction replicatedRefreshAction) {
        super(settings, RefreshAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, RefreshRequest.class);
        this.replicatedRefreshAction = replicatedRefreshAction;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        logger.trace("execute refresh");
        GroupShardsIterator groupShardsIterator = clusterService.operationRouting().searchShards(clusterService.state(), indexNameExpressionResolver.concreteIndices(clusterService.state(), request), null, null);
        final AtomicReferenceArray<ReplicatedRefreshResponse> shardsResponses = new AtomicReferenceArray(groupShardsIterator.size());
        final AtomicInteger numPrimariesToProcess = new AtomicInteger(groupShardsIterator.size());
        if (numPrimariesToProcess.get() != 0) {
            for (final ShardsIterator shardsIterator : groupShardsIterator) {
                final ShardRouting shardRouting = shardsIterator.nextOrNull();
                if (shardRouting == null) {
                    if (numPrimariesToProcess.decrementAndGet() == 0) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                } else {
                    final ShardId shardId = shardRouting.shardId();
                    replicatedRefreshAction.execute(new ReplicatedRefreshRequest(shardId), new ActionListener<ReplicatedRefreshResponse>() {
                        @Override
                        public void onResponse(ReplicatedRefreshResponse replicatedRefreshResponse) {
                            int responseNumber = numPrimariesToProcess.decrementAndGet();
                            shardsResponses.getAndSet(responseNumber, replicatedRefreshResponse);
                            if (responseNumber == 0) {
                                logger.trace("refresh: got response from {}", replicatedRefreshResponse.getShardId());
                                finishAndNotifyListener(listener, shardsResponses);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.trace("refresh: got failure from {}", shardId);
                            int totalNumCopies = clusterService.state().getMetaData().index(shardId.index().getName()).getNumberOfReplicas() + 1;
                            ReplicatedRefreshResponse replicatedRefreshResponse = new ReplicatedRefreshResponse(shardId, totalNumCopies);
                            ActionWriteResponse.ShardInfo.Failure failure = new ActionWriteResponse.ShardInfo.Failure(shardId.index().name(), shardId.id(), null, e, ExceptionsHelper.status(e), true);
                            ActionWriteResponse.ShardInfo.Failure[] failures = new ActionWriteResponse.ShardInfo.Failure[totalNumCopies];
                            Arrays.fill(failures, failure);
                            replicatedRefreshResponse.setShardInfo(new ActionWriteResponse.ShardInfo(totalNumCopies, 0, failures));
                            int responseNumber = numPrimariesToProcess.decrementAndGet();
                            shardsResponses.getAndSet(responseNumber, replicatedRefreshResponse);
                            if (responseNumber == 0) {
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

    private void finishAndNotifyListener(ActionListener listener, AtomicReferenceArray<ReplicatedRefreshResponse> shardsResponses) {
        logger.trace("refresh: got all shard responses");
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            ReplicatedRefreshResponse shardResponse = shardsResponses.get(i);
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
        listener.onResponse(new RefreshResponse(totalNumCopies, successfulShards, failedShards, shardFailures));
    }
}
