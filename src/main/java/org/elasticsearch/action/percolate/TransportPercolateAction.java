/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.percolator.PercolateException;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
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
        super(settings, threadPool, clusterService, transportService);
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
    protected String transportAction() {
        return PercolateAction.NAME;
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
        int successfulShards = 0;
        int failedShards = 0;

        List<PercolateShardResponse> shardResults = null;
        List<ShardOperationFailedException> shardFailures = null;

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
                if (shardResults == null) {
                    shardResults = newArrayList();
                }
                shardResults.add(percolateShardResponse);
                successfulShards++;
            }
        }

        long tookInMillis = System.currentTimeMillis() - request.startTime;
        if (shardResults == null) {
            return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, tookInMillis);
        }

        if (request.onlyCount()) {
            long finalCount = 0;
            for (PercolateShardResponse shardResponse : shardResults) {
                finalCount += shardResponse.count();
            }

            return new PercolateResponse(
                    shardsResponses.length(), successfulShards, failedShards, shardFailures, finalCount, tookInMillis
            );
        } else {
            long finalCount = 0;
            for (PercolateShardResponse response : shardResults) {
                finalCount += response.count();
            }

            int requestedSize = shardResults.get(0).requestedSize();
            boolean limit = shardResults.get(0).limit();
            if (limit) {
                requestedSize = (int) Math.min(requestedSize, finalCount);
            } else {
                // Serializing more than Integer.MAX_VALUE seems insane to me...
                requestedSize = (int) finalCount;
            }

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<PercolateResponse.Match>(requestedSize);
            outer: for (PercolateShardResponse response : shardResults) {
                Text index = new StringText(response.getIndex());
                for (Text id : response.matches()) {
                    finalMatches.add(new PercolateResponse.Match(id, index));
                    if (requestedSize != 0 && finalMatches.size() == requestedSize) {
                        break outer;
                    }
                }
            }

            return new PercolateResponse(
                    shardsResponses.length(), successfulShards, failedShards, shardFailures, finalMatches.toArray(new PercolateResponse.Match[requestedSize]), finalCount, tookInMillis
            );
        }
    }

    @Override
    protected PercolateShardRequest newShardRequest() {
        return new PercolateShardRequest();
    }

    @Override
    protected PercolateShardRequest newShardRequest(ShardRouting shard, PercolateRequest request) {
        return new PercolateShardRequest(shard.index(), shard.id(), request);
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
    protected PercolateShardResponse shardOperation(PercolateShardRequest request) throws ElasticSearchException {
        try {
            if (request.onlyCount()) {
                return percolatorService.countPercolate(request);
            } else {
                return percolatorService.matchPercolate(request);
            }
        } catch (Throwable t) {
            logger.trace("[{}][{}] failed to percolate", t, request.index(), request.shardId());
            ShardId shardId = new ShardId(request.index(), request.shardId());
            throw new PercolateException(shardId, "failed to percolate", t);
        }
    }

}
