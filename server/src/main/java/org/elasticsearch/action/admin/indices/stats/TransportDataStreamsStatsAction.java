/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;

public class TransportDataStreamsStatsAction extends TransportBroadcastByNodeAction<DataStreamsStatsRequest, DataStreamsStatsResponse,
    DataStreamShardStats> {

    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportDataStreamsStatsAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(DataStreamStatsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            DataStreamsStatsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected DataStreamsStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new DataStreamsStatsRequest(in);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DataStreamsStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DataStreamsStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    private static List<String> resolveIndexAbstractions(String[] indices, IndicesOptions indicesOptions, Metadata metadata,
                                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();
        Set<String> availableIndexAbstractions = metadata.getIndicesLookup().keySet();
        List<String> finalIndices = new ArrayList<>();
        boolean wildcardSeen = false;
        for (String index : indices) {
            String indexAbstraction;
            boolean minus = false;
            if (index.charAt(0) == '-' && wildcardSeen) {
                indexAbstraction = index.substring(1);
                minus = true;
            } else {
                indexAbstraction = index;
            }

            // we always need to check for date math expressions
            final String dateMathName = indexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);
            if (dateMathName != indexAbstraction) {
                assert dateMathName.equals(indexAbstraction) == false;
                if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                    // continue
                    indexAbstraction = dateMathName;
                } else if (availableIndexAbstractions.contains(dateMathName) &&
                    isIndexVisible(indexAbstraction, dateMathName, indicesOptions, metadata, true)) {
                    if (minus) {
                        finalIndices.remove(dateMathName);
                    } else {
                        finalIndices.add(dateMathName);
                    }
                } else {
                    if (indicesOptions.ignoreUnavailable() == false) {
                        throw new IndexNotFoundException(dateMathName);
                    }
                }
            }

            if (replaceWildcards && Regex.isSimpleMatchPattern(indexAbstraction)) {
                wildcardSeen = true;
                Set<String> resolvedIndices = new HashSet<>();
                for (String authorizedIndex : availableIndexAbstractions) {
                    if (Regex.simpleMatch(indexAbstraction, authorizedIndex) &&
                        isIndexVisible(indexAbstraction, authorizedIndex, indicesOptions, metadata)) {
                        resolvedIndices.add(authorizedIndex);
                    }
                }
                if (resolvedIndices.isEmpty()) {
                    //es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                    if (indicesOptions.allowNoIndices() == false) {
                        throw new IndexNotFoundException(indexAbstraction);
                    }
                } else {
                    if (minus) {
                        finalIndices.removeAll(resolvedIndices);
                    } else {
                        finalIndices.addAll(resolvedIndices);
                    }
                }
            } else if (dateMathName.equals(indexAbstraction)) {
                if (minus) {
                    finalIndices.remove(indexAbstraction);
                } else {
                    finalIndices.add(indexAbstraction);
                }
            }
        }
        return finalIndices;
    }

    private static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata) {
        return isIndexVisible(expression, index, indicesOptions, metadata, false);
    }

    private static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata,
                                          boolean dateMathExpression) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
        final boolean isHidden = indexAbstraction.isHidden();
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            //it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            //complicated to support those options with aliases pointing to multiple indices...
            if (indicesOptions.ignoreAliases()) {
                return false;
            } else if (isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index)) {
                return true;
            } else {
                return false;
            }
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
            // If indicesOptions.includeDataStreams() returns false then we fail later in IndexNameExpressionResolver.
            if (isHidden == false || indicesOptions.expandWildcardsHidden()) {
                return true;
            } else {
                return false;
            }
        }
        assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
        IndexMetadata indexMetadata = indexAbstraction.getIndices().get(0);
        if (isHidden && indicesOptions.expandWildcardsHidden() == false && isVisibleDueToImplicitHidden(expression, index) == false) {
            return false;
        }

        // the index is not hidden and since it is a date math expression, we consider it visible regardless of open/closed
        if (dateMathExpression) {
            assert IndexMetadata.State.values().length == 2 : "a new IndexMetadata.State value may need to be handled!";
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
            return true;
        }
        return false;
    }

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, DataStreamsStatsRequest request, String[] concreteIndices) {
        // Resolve the request pattern to concrete indices, but an index pattern might pull in indices that are not part of datastreams
        // Filter to only include those concrete indices that are backing datastreams
        // TODO: Hmmm we probably shouldn't be filtering on an index pattern at all. These patterns should be applied to data streams only.
        String[] requestIndices = request.indices();
        if (requestIndices == null || requestIndices.length == 0) {
            requestIndices = new String[]{"*"};
        }
        List<String> abstractionNames = resolveIndexAbstractions(requestIndices, request.indicesOptions(), clusterState.getMetadata(), indexNameExpressionResolver);
        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();
        logger.info("Lookup Contents: {}", indicesLookup);

        logger.info("Abstractions: {}", abstractionNames);
        String[] concreteDatastreamIndices = abstractionNames.stream().flatMap(abstractionName -> {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName);
            logger.info("Checking {}", abstractionName);
            assert indexAbstraction != null;
            logger.info("{}: Datastream? {}", abstractionName, indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM);
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) indexAbstraction;
                List<IndexMetadata> indices = dataStream.getIndices();
                return indices.stream().map(idx -> idx.getIndex().getName());
            } else {
                return Stream.empty();
            }
        }).toArray(String[]::new);
        logger.info("Final indices: {}", Arrays.toString(concreteDatastreamIndices));
        return clusterState.getRoutingTable().allShards(concreteDatastreamIndices);
    }

    @Override
    protected DataStreamShardStats shardOperation(DataStreamsStatsRequest request, ShardRouting shardRouting) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        // if we don't have the routing entry yet, we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
        StoreStats storeStats = indexShard.storeStats();
        return new DataStreamShardStats(
            indexShard.routingEntry(),
            storeStats,
            0L //TODO
        );
    }

    @Override
    protected DataStreamShardStats readShardResult(StreamInput in) throws IOException {
        return new DataStreamShardStats(in);
    }

    private static class Stats {
        Set<String> backingIndices = new HashSet<>();
        long storageBytes = 0L;
        long maxTimestamp = 0L;
    }

    @Override
    protected DataStreamsStatsResponse newResponse(DataStreamsStatsRequest request, int totalShards, int successfulShards, int failedShards,
                                                   List<DataStreamShardStats> dataStreamShardStats,
                                                   List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        Map<String, Stats> dataStreamsStats = new HashMap<>();
        Set<String> allBackingIndices = new HashSet<>();
        long totalStoreSizeBytes = 0L;

        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();
        for (DataStreamShardStats shardStat : dataStreamShardStats) {
            String indexName = shardStat.getShardRouting().getIndexName();
            IndexAbstraction indexAbstraction = indicesLookup.get(indexName);
            IndexAbstraction.DataStream dataStream = indexAbstraction.getParentDataStream();
            assert dataStream != null;

            // Aggregate global stats
            totalStoreSizeBytes += shardStat.getStoreStats().sizeInBytes();
            allBackingIndices.add(indexName);

            // Aggregate data stream stats
            Stats stats = dataStreamsStats.computeIfAbsent(dataStream.getName(), s -> new Stats());
            stats.storageBytes += shardStat.getStoreStats().sizeInBytes();
            stats.maxTimestamp = Math.max(stats.maxTimestamp, shardStat.getMaxTimestamp());
            stats.backingIndices.add(indexName);
        }

        DataStreamStats[] dataStreamStats = dataStreamsStats.entrySet().stream()
            .map(entry -> new DataStreamStats(
                entry.getKey(),
                entry.getValue().backingIndices.size(),
                new ByteSizeValue(entry.getValue().storageBytes),
                entry.getValue().maxTimestamp))
            .toArray(DataStreamStats[]::new);

        return new DataStreamsStatsResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardFailures,
            dataStreamsStats.size(),
            allBackingIndices.size(),
            new ByteSizeValue(totalStoreSizeBytes),
            dataStreamStats
        );
    }
}
