/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * Indices clear cache action.
 *
 * @author kimchy (shay.banon)
 */
public class TransportClearIndicesCacheAction extends TransportBroadcastOperationAction<ClearIndicesCacheRequest, ClearIndicesCacheResponse, ShardClearIndicesCacheRequest, ShardClearIndicesCacheResponse> {

    private final IndicesService indicesService;

    @Inject public TransportClearIndicesCacheAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                    TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.Cache.CLEAR;
    }

    @Override protected String transportShardAction() {
        return "indices/cache/clear/shard";
    }


    @Override protected ClearIndicesCacheRequest newRequest() {
        return new ClearIndicesCacheRequest();
    }

    @Override protected boolean ignoreNonActiveExceptions() {
        return true;
    }

    @Override protected ClearIndicesCacheResponse newResponse(ClearIndicesCacheRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
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
                successfulShards++;
            }
        }
        return new ClearIndicesCacheResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override protected ShardClearIndicesCacheRequest newShardRequest() {
        return new ShardClearIndicesCacheRequest();
    }

    @Override protected ShardClearIndicesCacheRequest newShardRequest(ShardRouting shard, ClearIndicesCacheRequest request) {
        return new ShardClearIndicesCacheRequest(shard.index(), shard.id(), request);
    }

    @Override protected ShardClearIndicesCacheResponse newShardResponse() {
        return new ShardClearIndicesCacheResponse();
    }

    @Override protected ShardClearIndicesCacheResponse shardOperation(ShardClearIndicesCacheRequest request) throws ElasticSearchException {
        IndexService service = indicesService.indexService(request.index());
        if (service != null) {
            // we always clear the query cache
            service.cache().queryParserCache().clear();
            boolean clearedAtLeastOne = false;
            if (request.filterCache()) {
                clearedAtLeastOne = true;
                service.cache().filter().clear();
            }
            if (request.fieldDataCache()) {
                clearedAtLeastOne = true;
                service.cache().fieldData().clear();
            }
            if (request.idCache()) {
                clearedAtLeastOne = true;
                service.cache().idCache().clear();
            }
            if (request.bloomCache()) {
                clearedAtLeastOne = true;
                service.cache().bloomCache().clear();
            }
            if (!clearedAtLeastOne) {
                service.cache().clear();
            }
            service.cache().invalidateCache();
        }
        return new ShardClearIndicesCacheResponse(request.index(), request.shardId());
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override protected GroupShardsIterator shards(ClearIndicesCacheRequest request, String[] concreteIndices, ClusterState clusterState) {
        return clusterState.routingTable().allActiveShardsGrouped(concreteIndices, true);
    }
}