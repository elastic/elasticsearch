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
package org.elasticsearch.legacy.action.percolate;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.ShardOperationFailedException;
import org.elasticsearch.legacy.action.get.GetResponse;
import org.elasticsearch.legacy.action.get.TransportGetAction;
import org.elasticsearch.legacy.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.routing.GroupShardsIterator;
import org.elasticsearch.legacy.cluster.routing.ShardRouting;
import org.elasticsearch.legacy.common.bytes.BytesReference;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.engine.DocumentMissingException;
import org.elasticsearch.legacy.index.shard.ShardId;
import org.elasticsearch.legacy.percolator.PercolateException;
import org.elasticsearch.legacy.percolator.PercolatorService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportPercolateAction extends TransportBroadcastOperationAction<PercolateRequest, PercolateResponse, PercolateShardRequest, PercolateShardResponse> {

    private final PercolatorService percolatorService;
    private final TransportGetAction getAction;

    @Inject
    public TransportPercolateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                    TransportService transportService, PercolatorService percolatorService,
                                    TransportGetAction getAction) {
        super(settings, PercolateAction.NAME, threadPool, clusterService, transportService);
        this.percolatorService = percolatorService;
        this.getAction = getAction;
    }

    @Override
    protected void doExecute(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        request.startTime = System.currentTimeMillis();
        if (request.getRequest() != null) {
            getAction.execute(request.getRequest(), new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getResponse) {
                    if (!getResponse.isExists()) {
                        onFailure(new DocumentMissingException(null, request.getRequest().type(), request.getRequest().id()));
                        return;
                    }

                    BytesReference docSource = getResponse.getSourceAsBytesRef();
                    TransportPercolateAction.super.doExecute(new PercolateRequest(request, docSource), listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            super.doExecute(request, listener);
        }
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.PERCOLATE;
    }

    @Override
    protected PercolateRequest newRequest() {
        return new PercolateRequest();
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PercolateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PercolateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected PercolateResponse newResponse(PercolateRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        return reduce(request, shardsResponses, percolatorService);
    }

    public static PercolateResponse reduce(PercolateRequest request, AtomicReferenceArray shardsResponses, PercolatorService percolatorService) {
        int successfulShards = 0;
        int failedShards = 0;

        List<PercolateShardResponse> shardResults = null;
        List<ShardOperationFailedException> shardFailures = null;

        byte percolatorTypeId = 0x00;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                PercolateShardResponse percolateShardResponse = (PercolateShardResponse) shardResponse;
                successfulShards++;
                if (!percolateShardResponse.isEmpty()) {
                    if (shardResults == null) {
                        percolatorTypeId = percolateShardResponse.percolatorTypeId();
                        shardResults = newArrayList();
                    }
                    shardResults.add(percolateShardResponse);
                }
            }
        }

        if (shardResults == null) {
            long tookInMillis = System.currentTimeMillis() - request.startTime;
            PercolateResponse.Match[] matches = request.onlyCount() ? null : PercolateResponse.EMPTY;
            return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, tookInMillis, matches);
        } else {
            PercolatorService.ReduceResult result = percolatorService.reduce(percolatorTypeId, shardResults);
            long tookInMillis = System.currentTimeMillis() - request.startTime;
            return new PercolateResponse(
                    shardsResponses.length(), successfulShards, failedShards, shardFailures,
                    result.matches(), result.count(), tookInMillis, result.reducedFacets(), result.reducedAggregations()
            );
        }
    }

    @Override
    protected PercolateShardRequest newShardRequest() {
        return new PercolateShardRequest();
    }

    @Override
    protected PercolateShardRequest newShardRequest(int numShards, ShardRouting shard, PercolateRequest request) {
        return new PercolateShardRequest(shard.index(), shard.id(), numShards, request);
    }

    @Override
    protected PercolateShardResponse newShardResponse() {
        return new PercolateShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, PercolateRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected PercolateShardResponse shardOperation(PercolateShardRequest request) throws ElasticsearchException {
        try {
            return percolatorService.percolate(request);
        } catch (Throwable e) {
            logger.trace("[{}][{}] failed to percolate", e, request.index(), request.shardId());
            ShardId shardId = new ShardId(request.index(), request.shardId());
            throw new PercolateException(shardId, "failed to percolate", e);
        }
    }

}
