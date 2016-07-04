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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportFieldStatsAction extends
    TransportBroadcastAction<FieldStatsRequest, FieldStatsResponse, FieldStatsShardRequest, FieldStatsShardResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFieldStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                              TransportService transportService, ActionFilters actionFilters,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              IndicesService indicesService) {
        super(settings, FieldStatsAction.NAME, threadPool, clusterService, transportService,
            actionFilters, indexNameExpressionResolver, FieldStatsRequest::new,
            FieldStatsShardRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    protected FieldStatsResponse newResponse(FieldStatsRequest request, AtomicReferenceArray shardsResponses,
                                             ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        Map<String, String> conflicts = new HashMap<>();
        Map<String, Map<String, FieldStats>> indicesMergedFieldStats = new HashMap<>();
        List<ShardOperationFailedException> shardFailures = new ArrayList<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardValue = shardsResponses.get(i);
            if (shardValue == null) {
                // simply ignore non active shards
            } else if (shardValue instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                shardFailures.add(
                    new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardValue)
                );
            } else {
                successfulShards++;
                FieldStatsShardResponse shardResponse = (FieldStatsShardResponse) shardValue;

                final String indexName;
                if ("cluster".equals(request.level())) {
                    indexName = "_all";
                } else if ("indices".equals(request.level())) {
                    indexName = shardResponse.getIndex();
                } else {
                    // should already have been caught by the FieldStatsRequest#validate(...)
                    throw new IllegalArgumentException("Illegal level option [" + request.level() + "]");
                }

                Map<String, FieldStats> indexMergedFieldStats = indicesMergedFieldStats.get(indexName);
                if (indexMergedFieldStats == null) {
                    indicesMergedFieldStats.put(indexName, indexMergedFieldStats = new HashMap<>());
                }

                Map<String, FieldStats<?>> fieldStats = shardResponse.getFieldStats();
                for (Map.Entry<String, FieldStats<?>> entry : fieldStats.entrySet()) {
                    FieldStats<?> existing = indexMergedFieldStats.get(entry.getKey());
                    if (existing != null) {
                        if (existing.getType() != entry.getValue().getType()) {
                            if (conflicts.containsKey(entry.getKey()) == false) {
                                FieldStats[] fields = new FieldStats[] {entry.getValue(), existing};
                                Arrays.sort(fields, (o1, o2) -> Byte.compare(o1.getType(), o2.getType()));
                                conflicts.put(entry.getKey(),
                                    "Field [" + entry.getKey() + "] of type [" +
                                        FieldStats.typeName(fields[0].getType()) +
                                        "] conflicts with existing field of type [" +
                                        FieldStats.typeName(fields[1].getType()) +
                                        "] in other index.");
                            }
                        } else {
                            existing.accumulate(entry.getValue());
                        }
                    } else {
                        indexMergedFieldStats.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            // Check the field with conflicts and remove them.
            for (String conflictKey : conflicts.keySet()) {
                Iterator<Map.Entry<String, Map<String, FieldStats>>> iterator =
                    indicesMergedFieldStats.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Map<String, FieldStats>> entry = iterator.next();
                    if (entry.getValue().containsKey(conflictKey)) {
                        entry.getValue().remove(conflictKey);
                    }
                }
            }

        }

        if (request.getIndexConstraints().length != 0) {
            Set<String> fieldStatFields = new HashSet<>(Arrays.asList(request.getFields()));
            for (IndexConstraint indexConstraint : request.getIndexConstraints()) {
                Iterator<Map.Entry<String, Map<String, FieldStats>>> iterator =
                    indicesMergedFieldStats.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Map<String, FieldStats>> entry = iterator.next();
                    FieldStats indexConstraintFieldStats = entry.getValue().get(indexConstraint.getField());
                    if (indexConstraintFieldStats != null && indexConstraintFieldStats.match(indexConstraint)) {
                        // If the field stats didn't occur in the list of fields in the original request
                        // we need to remove the field stats, because it was never requested and was only needed to
                        // validate the index constraint.
                        if (fieldStatFields.contains(indexConstraint.getField()) == false) {
                            entry.getValue().remove(indexConstraint.getField());
                        }
                    } else {
                        // The index constraint didn't match or was empty,
                        // so we remove all the field stats of the index we're checking.
                        iterator.remove();
                    }
                }
            }
        }

        return new FieldStatsResponse(shardsResponses.length(), successfulShards, failedShards,
            shardFailures, indicesMergedFieldStats, conflicts);
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
    protected FieldStatsShardResponse shardOperation(FieldStatsShardRequest request) {
        ShardId shardId = request.shardId();
        Map<String, FieldStats<?>> fieldStats = new HashMap<>();
        IndexService indexServices = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard shard = indexServices.getShard(shardId.id());
        try (Engine.Searcher searcher = shard.acquireSearcher("fieldstats")) {
            // Resolve patterns and deduplicate
            Set<String> fieldNames = new HashSet<>();
            for (String field : request.getFields()) {
                fieldNames.addAll(shard.mapperService().simpleMatchToIndexNames(field));
            }
            for (String field : fieldNames) {
                FieldStats<?> stats = indicesService.getFieldStats(shard, searcher, field, request.shouldUseCache());
                if (stats != null) {
                    fieldStats.put(field, stats);
                }
            }
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        return new FieldStatsShardResponse(shardId, fieldStats);
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, FieldStatsRequest request,
                                         String[] concreteIndices) {
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, null, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FieldStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FieldStatsRequest request,
                                                      String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }
}
