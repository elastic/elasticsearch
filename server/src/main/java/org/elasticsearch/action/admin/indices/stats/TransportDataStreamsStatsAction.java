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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;

public class TransportDataStreamsStatsAction extends TransportBroadcastByNodeAction<DataStreamsStatsRequest, DataStreamsStatsResponse,
    DataStreamShardStats> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final IndexAbstractionResolver indexAbstractionResolver;

    @Inject
    public TransportDataStreamsStatsAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(DataStreamStatsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            DataStreamsStatsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
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

    @Override
    protected ShardsIterator shards(ClusterState clusterState, DataStreamsStatsRequest request, String[] concreteIndices) {
        String[] requestIndices = request.indices();
        if (requestIndices == null || requestIndices.length == 0) {
            requestIndices = new String[]{"*"};
        }
        List<String> abstractionNames = indexAbstractionResolver.resolveIndexAbstractions(requestIndices, request.indicesOptions(),
            clusterState.getMetadata());
        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();

        String[] concreteDatastreamIndices = abstractionNames.stream().flatMap(abstractionName -> {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName);
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) indexAbstraction;
                List<IndexMetadata> indices = dataStream.getIndices();
                return indices.stream().map(idx -> idx.getIndex().getName());
            } else {
                return Stream.empty();
            }
        }).toArray(String[]::new);
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
        IndexAbstraction indexAbstraction = clusterService.state().getMetadata().getIndicesLookup().get(shardRouting.getIndexName());
        assert indexAbstraction != null;
        IndexAbstraction.DataStream dataStream = indexAbstraction.getParentDataStream();
        assert dataStream != null;
        long maxTimestamp = 0L;
        try (Engine.Searcher searcher = indexShard.acquireSearcher("data_stream_stats")) {
            IndexReader indexReader = searcher.getIndexReader();
            String fieldName = dataStream.getDataStream().getTimeStampField().getName();
            byte[] maxPackedValue = PointValues.getMaxPackedValue(indexReader, fieldName);
            if (maxPackedValue != null) {
                maxTimestamp = LongPoint.decodeDimension(maxPackedValue, 0);
            }
        }
        return new DataStreamShardStats(
            indexShard.routingEntry(),
            storeStats,
            maxTimestamp
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
