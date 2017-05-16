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

package org.elasticsearch.legacy.action.admin.indices.cache.clear;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ShardOperationFailedException;
import org.elasticsearch.legacy.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.legacy.cache.recycler.CacheRecycler;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.routing.GroupShardsIterator;
import org.elasticsearch.legacy.cluster.routing.ShardRouting;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.legacy.index.service.IndexService;
import org.elasticsearch.legacy.indices.IndicesService;
import org.elasticsearch.legacy.indices.cache.filter.terms.IndicesTermsFilterCache;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Indices clear cache action.
 */
public class TransportClearIndicesCacheAction extends TransportBroadcastOperationAction<ClearIndicesCacheRequest, ClearIndicesCacheResponse, ShardClearIndicesCacheRequest, ShardClearIndicesCacheResponse> {

    private final IndicesService indicesService;
    private final IndicesTermsFilterCache termsFilterCache;
    private final CacheRecycler cacheRecycler;

    @Inject
    public TransportClearIndicesCacheAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                            TransportService transportService, IndicesService indicesService, IndicesTermsFilterCache termsFilterCache,
                                            CacheRecycler cacheRecycler) {
        super(settings, ClearIndicesCacheAction.NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.termsFilterCache = termsFilterCache;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClearIndicesCacheRequest newRequest() {
        return new ClearIndicesCacheRequest();
    }

    @Override
    protected ClearIndicesCacheResponse newResponse(ClearIndicesCacheRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
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

    @Override
    protected ShardClearIndicesCacheRequest newShardRequest() {
        return new ShardClearIndicesCacheRequest();
    }

    @Override
    protected ShardClearIndicesCacheRequest newShardRequest(int numShards, ShardRouting shard, ClearIndicesCacheRequest request) {
        return new ShardClearIndicesCacheRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardClearIndicesCacheResponse newShardResponse() {
        return new ShardClearIndicesCacheResponse();
    }

    @Override
    protected ShardClearIndicesCacheResponse shardOperation(ShardClearIndicesCacheRequest request) throws ElasticsearchException {
        IndexService service = indicesService.indexService(request.index());
        if (service != null) {
            // we always clear the query cache
            service.cache().queryParserCache().clear();
            boolean clearedAtLeastOne = false;
            if (request.filterCache()) {
                clearedAtLeastOne = true;
                service.cache().filter().clear("api");
                termsFilterCache.clear("api");
            }
            if (request.filterKeys() != null && request.filterKeys().length > 0) {
                clearedAtLeastOne = true;
                service.cache().filter().clear("api", request.filterKeys());
                termsFilterCache.clear("api", request.filterKeys());
            }
            if (request.fieldDataCache()) {
                clearedAtLeastOne = true;
                if (request.fields() == null || request.fields().length == 0) {
                    service.fieldData().clear();
                } else {
                    for (String field : request.fields()) {
                        service.fieldData().clearField(field);
                    }
                }
            }
            if (request.recycler()) {
                logger.debug("Clear CacheRecycler on index [{}]", service.index());
                clearedAtLeastOne = true;
                // cacheRecycler.clear();
            }
            if (request.idCache()) {
                clearedAtLeastOne = true;
                service.fieldData().clearField(ParentFieldMapper.NAME);
            }
            if (!clearedAtLeastOne) {
                if (request.fields() != null && request.fields().length > 0) {
                    // only clear caches relating to the specified fields
                    for (String field : request.fields()) {
                        service.fieldData().clearField(field);
                    }
                } else {
                    service.cache().clear("api");
                    service.fieldData().clear();
                    termsFilterCache.clear("api");
                }
            }
        }
        return new ShardClearIndicesCacheResponse(request.index(), request.shardId());
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, ClearIndicesCacheRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allActiveShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ClearIndicesCacheRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ClearIndicesCacheRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

}
