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

package org.elasticsearch.action.admin.indices.datastream;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
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

public class DataStreamsStatsAction extends ActionType<DataStreamsStatsAction.Response> {

    public static final DataStreamsStatsAction INSTANCE = new DataStreamsStatsAction();
    public static final String NAME = "indices:monitor/data_stream/stats";

    public DataStreamsStatsAction() {
        super(NAME, DataStreamsStatsAction.Response::new);
    }

    public static class Request extends BroadcastRequest<Request> {
        public Request() {
            super((String[]) null);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class Response extends BroadcastResponse {
        private final int streams;
        private final int backingIndices;
        private final ByteSizeValue totalStoreSize;
        private final DataStreamStats[] dataStreamStats;

        public Response(int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> shardFailures,
                        int streams, int backingIndices, ByteSizeValue totalStoreSize, DataStreamStats[] dataStreamStats) {
            super(totalShards, successfulShards, failedShards, shardFailures);
            this.streams = streams;
            this.backingIndices = backingIndices;
            this.totalStoreSize = totalStoreSize;
            this.dataStreamStats = dataStreamStats;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.streams = in.readVInt();
            this.backingIndices = in.readVInt();
            this.totalStoreSize = new ByteSizeValue(in);
            this.dataStreamStats = in.readArray(DataStreamStats::new, DataStreamStats[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(streams);
            out.writeVInt(backingIndices);
            totalStoreSize.writeTo(out);
            out.writeArray(dataStreamStats);
        }

        @Override
        protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
            builder.field("data_stream_count", streams);
            builder.field("backing_indices", backingIndices);
            builder.humanReadableField("total_store_size_bytes", "total_store_size", totalStoreSize);
            builder.array("data_streams", (Object[]) dataStreamStats);
        }

        public int getStreams() {
            return streams;
        }

        public int getBackingIndices() {
            return backingIndices;
        }

        public ByteSizeValue getTotalStoreSize() {
            return totalStoreSize;
        }

        public DataStreamStats[] getDataStreamStats() {
            return dataStreamStats;
        }
    }

    public static class DataStreamStats implements ToXContentObject, Writeable {
        private final String dataStreamName;
        private final int backingIndices;
        private final ByteSizeValue storeSize;
        private final long maxTimestamp;

        public DataStreamStats(String dataStreamName, int backingIndices, ByteSizeValue storeSize, long maxTimestamp) {
            this.dataStreamName = dataStreamName;
            this.backingIndices = backingIndices;
            this.storeSize = storeSize;
            this.maxTimestamp = maxTimestamp;
        }

        public DataStreamStats(StreamInput in) throws IOException {
            this.dataStreamName = in.readString();
            this.backingIndices = in.readVInt();
            this.storeSize = new ByteSizeValue(in);
            this.maxTimestamp = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStreamName);
            out.writeVInt(backingIndices);
            storeSize.writeTo(out);
            out.writeVLong(maxTimestamp);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("data_stream", dataStreamName);
            builder.field("backing_indices", backingIndices);
            builder.humanReadableField("store_size_bytes", "store_size", storeSize);
            builder.field("maximum_timestamp", maxTimestamp);
            builder.endObject();
            return builder;
        }

        public String getDataStreamName() {
            return dataStreamName;
        }

        public int getBackingIndices() {
            return backingIndices;
        }

        public ByteSizeValue getStoreSize() {
            return storeSize;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }
    }

    public static class DataStreamShardStats implements Writeable {
        private final ShardRouting shardRouting;
        private final StoreStats storeStats;
        private final long maxTimestamp;

        public DataStreamShardStats(ShardRouting shardRouting, StoreStats storeStats, long maxTimestamp) {
            this.shardRouting = shardRouting;
            this.storeStats = storeStats;
            this.maxTimestamp = maxTimestamp;
        }

        public DataStreamShardStats(StreamInput in) throws IOException {
            this.shardRouting = new ShardRouting(in);
            this.storeStats = new StoreStats(in);
            this.maxTimestamp = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardRouting.writeTo(out);
            storeStats.writeTo(out);
            out.writeVLong(maxTimestamp);
        }

        public ShardRouting getShardRouting() {
            return shardRouting;
        }

        public StoreStats getStoreStats() {
            return storeStats;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }
    }

    private static class AggregatedStats {
        Set<String> backingIndices = new HashSet<>();
        long storageBytes = 0L;
        long maxTimestamp = 0L;
    }

    public static class TransportAction extends TransportBroadcastByNodeAction<Request, Response, DataStreamShardStats> {

        private final ClusterService clusterService;
        private final IndicesService indicesService;
        private final IndexAbstractionResolver indexAbstractionResolver;

        @Inject
        public TransportAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(DataStreamsStatsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                Request::new, ThreadPool.Names.MANAGEMENT);
            this.clusterService = clusterService;
            this.indicesService = indicesService;
            this.indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        }

        @Override
        protected Request readRequestFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
        }

        @Override
        protected ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices) {
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
        protected DataStreamShardStats shardOperation(Request request, ShardRouting shardRouting) throws IOException {
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
                    logger.info("shard[{}:{}] has max value {}", shardRouting.getIndexName(), shardRouting.getId(), maxTimestamp);
                } else {
                    logger.info("shard[{}:{}] has null max value", shardRouting.getIndexName(), shardRouting.getId());
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

        @Override
        protected Response newResponse(Request request, int totalShards, int successfulShards,
                                                       int failedShards, List<DataStreamShardStats> dataStreamShardStats,
                                                       List<DefaultShardOperationFailedException> shardFailures,
                                                       ClusterState clusterState) {
            Map<String, AggregatedStats> dataStreamsStats = new HashMap<>();
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
                AggregatedStats stats = dataStreamsStats.computeIfAbsent(dataStream.getName(), s -> new AggregatedStats());
                stats.storageBytes += shardStat.getStoreStats().sizeInBytes();
                logger.info("shard[{}:{}] max stamp [{}]", shardStat.getShardRouting().getIndexName(), shardStat.getShardRouting().getId(),
                    shardStat.getMaxTimestamp());
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

            return new Response(
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
}
