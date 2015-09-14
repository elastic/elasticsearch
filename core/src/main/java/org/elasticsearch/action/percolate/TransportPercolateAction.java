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
package org.elasticsearch.action.percolate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.percolator.PercolateException;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportPercolateAction extends TransportBroadcastAction<PercolateRequest, PercolateResponse, PercolateShardRequest, PercolateShardResponse> {

    private final PercolatorService percolatorService;
    private final TransportGetAction getAction;

    @Inject
    public TransportPercolateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                    TransportService transportService, PercolatorService percolatorService,
                                    TransportGetAction getAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PercolateAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, PercolateRequest::new, PercolateShardRequest::new, ThreadPool.Names.PERCOLATE);
        this.percolatorService = percolatorService;
        this.getAction = getAction;
    }

    @Override
    protected void doExecute(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        request.startTime = System.currentTimeMillis();
        if (request.getRequest() != null) {
            //create a new get request to make sure it has the same headers and context as the original percolate request
            GetRequest getRequest = new GetRequest(request.getRequest(), request);
            getAction.execute(getRequest, new ActionListener<GetResponse>() {
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
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                PercolateShardResponse percolateShardResponse = (PercolateShardResponse) shardResponse;
                successfulShards++;
                if (!percolateShardResponse.isEmpty()) {
                    if (shardResults == null) {
                        percolatorTypeId = percolateShardResponse.percolatorTypeId();
                        shardResults = new ArrayList<>();
                    }
                    shardResults.add(percolateShardResponse);
                }
            }
        }

        if (shardResults == null) {
            long tookInMillis = Math.max(1, System.currentTimeMillis() - request.startTime);
            PercolateResponse.Match[] matches = request.onlyCount() ? null : PercolateResponse.EMPTY;
            return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, tookInMillis, matches);
        } else {
            PercolatorService.ReduceResult result = percolatorService.reduce(percolatorTypeId, shardResults, request);
            long tookInMillis =  Math.max(1, System.currentTimeMillis() - request.startTime);
            return new PercolateResponse(
                    shardsResponses.length(), successfulShards, failedShards, shardFailures,
                    result.matches(), result.count(), tookInMillis, result.reducedAggregations()
            );
        }
    }

    @Override
    protected PercolateShardRequest newShardRequest(int numShards, ShardRouting shard, PercolateRequest request) {
        return new PercolateShardRequest(shard.shardId(), numShards, request);
    }

    @Override
    protected PercolateShardResponse newShardResponse() {
        return new PercolateShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, PercolateRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, request.preference());
    }

    @Override
    protected PercolateShardResponse shardOperation(PercolateShardRequest request) {
        try {
            return percolatorService.percolate(request);
        } catch (Throwable e) {
            logger.trace("{} failed to percolate", e, request.shardId());
            throw new PercolateException(request.shardId(), "failed to percolate", e);
        }
    }

}
