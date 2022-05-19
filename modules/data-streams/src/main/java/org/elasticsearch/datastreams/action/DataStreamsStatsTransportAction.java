/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
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

public class DataStreamsStatsTransportAction extends TransportBroadcastByNodeAction<
    DataStreamsStatsAction.Request,
    DataStreamsStatsAction.Response,
    DataStreamsStatsAction.DataStreamShardStats> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public DataStreamsStatsTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DataStreamsStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            DataStreamsStatsAction.Request::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, DataStreamsStatsAction.Request request, ActionListener<DataStreamsStatsAction.Response> listener) {
        request.indicesOptions(DataStreamsActionUtil.updateIndicesOptions(request.indicesOptions()));
        super.doExecute(task, request, listener);
    }

    @Override
    protected DataStreamsStatsAction.Request readRequestFrom(StreamInput in) throws IOException {
        return new DataStreamsStatsAction.Request(in);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DataStreamsStatsAction.Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState state,
        DataStreamsStatsAction.Request request,
        String[] concreteIndices
    ) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected String[] resolveConcreteIndexNames(ClusterState clusterState, DataStreamsStatsAction.Request request) {
        List<String> abstractionNames = indexNameExpressionResolver.dataStreamNames(
            clusterState,
            request.indicesOptions(),
            request.indices()
        );
        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();

        String[] concreteDatastreamIndices = abstractionNames.stream().flatMap(abstractionName -> {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName);
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) indexAbstraction;
                List<Index> indices = dataStream.getIndices();
                return indices.stream().map(Index::getName);
            } else {
                return Stream.empty();
            }
        }).toArray(String[]::new);
        return concreteDatastreamIndices;
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, DataStreamsStatsAction.Request request, String[] concreteIndices) {
        return clusterState.getRoutingTable().allShards(concreteIndices);
    }

    @Override
    protected void shardOperation(
        DataStreamsStatsAction.Request request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<DataStreamsStatsAction.DataStreamShardStats> listener
    ) {
        ActionListener.completeWith(listener, () -> {
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
            return new DataStreamsStatsAction.DataStreamShardStats(indexShard.routingEntry(), storeStats, maxTimestamp);
        });
    }

    @Override
    protected DataStreamsStatsAction.DataStreamShardStats readShardResult(StreamInput in) throws IOException {
        return new DataStreamsStatsAction.DataStreamShardStats(in);
    }

    @Override
    protected DataStreamsStatsAction.Response newResponse(
        DataStreamsStatsAction.Request request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DataStreamsStatsAction.DataStreamShardStats> dataStreamShardStats,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        Map<String, AggregatedStats> aggregatedDataStreamsStats = new HashMap<>();
        Set<String> allBackingIndices = new HashSet<>();
        long totalStoreSizeBytes = 0L;
        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();

        // Collect the number of backing indices from the cluster state. If every shard operation for an index fails,
        // or if a backing index simply has no shards allocated, it would be excluded from the counts if we only used
        // shard results to calculate.
        List<String> abstractionNames = indexNameExpressionResolver.dataStreamNames(
            clusterState,
            request.indicesOptions(),
            request.indices()
        );
        for (String abstractionName : abstractionNames) {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName);
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) indexAbstraction;
                AggregatedStats stats = aggregatedDataStreamsStats.computeIfAbsent(dataStream.getName(), s -> new AggregatedStats());
                dataStream.getIndices().stream().map(Index::getName).forEach(index -> {
                    stats.backingIndices.add(index);
                    allBackingIndices.add(index);
                });
            }
        }

        for (DataStreamsStatsAction.DataStreamShardStats shardStat : dataStreamShardStats) {
            String indexName = shardStat.getShardRouting().getIndexName();
            IndexAbstraction indexAbstraction = indicesLookup.get(indexName);
            IndexAbstraction.DataStream dataStream = indexAbstraction.getParentDataStream();
            assert dataStream != null;

            // Aggregate global stats
            totalStoreSizeBytes += shardStat.getStoreStats().sizeInBytes();

            // Aggregate data stream stats
            AggregatedStats stats = aggregatedDataStreamsStats.computeIfAbsent(dataStream.getName(), s -> new AggregatedStats());
            stats.storageBytes += shardStat.getStoreStats().sizeInBytes();
            stats.maxTimestamp = Math.max(stats.maxTimestamp, shardStat.getMaxTimestamp());
        }

        DataStreamsStatsAction.DataStreamStats[] dataStreamStats = aggregatedDataStreamsStats.entrySet()
            .stream()
            .map(
                entry -> new DataStreamsStatsAction.DataStreamStats(
                    entry.getKey(),
                    entry.getValue().backingIndices.size(),
                    new ByteSizeValue(entry.getValue().storageBytes),
                    entry.getValue().maxTimestamp
                )
            )
            .toArray(DataStreamsStatsAction.DataStreamStats[]::new);

        return new DataStreamsStatsAction.Response(
            totalShards,
            successfulShards,
            failedShards,
            shardFailures,
            aggregatedDataStreamsStats.size(),
            allBackingIndices.size(),
            new ByteSizeValue(totalStoreSizeBytes),
            dataStreamStats
        );
    }

    private static class AggregatedStats {
        Set<String> backingIndices = new HashSet<>();
        long storageBytes = 0L;
        long maxTimestamp = 0L;
    }
}
