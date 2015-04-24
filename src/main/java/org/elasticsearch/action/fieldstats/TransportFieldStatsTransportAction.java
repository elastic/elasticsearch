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

package org.elasticsearch.action.fieldstats;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportFieldStatsTransportAction extends TransportBroadcastOperationAction<FieldStatsRequest, FieldStatsResponse, FieldStatsShardRequest, FieldStatsShardResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFieldStatsTransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, IndicesService indicesService) {
        super(settings, FieldStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, FieldStatsRequest.class, FieldStatsShardRequest.class, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    protected FieldStatsResponse newResponse(FieldStatsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        Map<String, Map<String, FieldStats>> indicesMergedFieldStats = new HashMap<>();
        List<ShardOperationFailedException> shardFailures = new ArrayList<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardValue = shardsResponses.get(i);
            if (shardValue == null) {
                // simply ignore non active shards
            } else if (shardValue instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardValue));
            } else {
                successfulShards++;
                FieldStatsShardResponse shardResponse = (FieldStatsShardResponse) shardValue;

                final String indexName;
                if ("cluster".equals(request.level())) {
                    indexName = "_all";
                } else if ("indices".equals(request.level())) {
                    indexName = shardResponse.getIndex();
                } else {
                    // should already have been catched by the FieldStatsRequest#validate(...)
                    throw new ElasticsearchIllegalArgumentException("Illegal level option [" + request.level() + "]");
                }

                Map<String, FieldStats> indexMergedFieldStats = indicesMergedFieldStats.get(indexName);
                if (indexMergedFieldStats == null) {
                    indicesMergedFieldStats.put(indexName, indexMergedFieldStats = new HashMap<>());
                }

                Map<String, FieldStats> fieldStats = shardResponse.getFieldStats();
                for (Map.Entry<String, FieldStats> entry : fieldStats.entrySet()) {
                    FieldStats existing = indexMergedFieldStats.get(entry.getKey());
                    if (existing != null) {
                        if (existing.getType() != entry.getValue().getType()) {
                            throw new ElasticsearchIllegalStateException(
                                    "trying to merge the field stats of field [" + entry.getKey() + "] from index [" + shardResponse.getIndex() + "] but the field type is incompatible, try to set the 'level' option to 'indices'"
                            );
                        }

                        existing.append(entry.getValue());
                    } else {
                        indexMergedFieldStats.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return new FieldStatsResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, indicesMergedFieldStats);
    }

    @Override
    protected FieldStatsShardRequest newShardRequest(int numShards, ShardRouting shard, FieldStatsRequest request) {
        return new FieldStatsShardRequest(shard.shardId(), request);
    }

    @Override
    protected FieldStatsShardResponse newShardResponse() {
        return new FieldStatsShardResponse();
    }

    @Override
    protected FieldStatsShardResponse shardOperation(FieldStatsShardRequest request) throws ElasticsearchException {
        ShardId shardId = request.shardId();
        Map<String, FieldStats> fieldStats = new HashMap<>();
        IndexService indexServices = indicesService.indexServiceSafe(shardId.getIndex());
        MapperService mapperService = indexServices.mapperService();
        IndexShard shard = indexServices.shardSafe(shardId.id());
        shard.readAllowed();
        try (Engine.Searcher searcher = shard.acquireSearcher("fieldstats")) {
            for (String field : request.getFields()) {
                FieldMappers fieldMappers = mapperService.fullName(field);
                if (fieldMappers != null) {
                    IndexReader reader = searcher.reader();
                    Terms terms = MultiFields.getTerms(reader, field);
                    if (terms != null) {
                        fieldStats.put(field, fieldMappers.mapper().stats(terms, reader.maxDoc()));
                    }
                } else {
                    throw new IllegalArgumentException("field [" + field + "] doesn't exist");
                }
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        return new FieldStatsShardResponse(shardId, fieldStats);
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, FieldStatsRequest request, String[] concreteIndices) {
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, null, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FieldStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FieldStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }
}
