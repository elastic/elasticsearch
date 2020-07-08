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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        private final int dataStreamCount;
        private final int backingIndices;
        private final ByteSizeValue totalStoreSize;
        private final DataStreamStats[] dataStreams;

        public Response(int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> shardFailures,
                        int dataStreamCount, int backingIndices, ByteSizeValue totalStoreSize, DataStreamStats[] dataStreams) {
            super(totalShards, successfulShards, failedShards, shardFailures);
            this.dataStreamCount = dataStreamCount;
            this.backingIndices = backingIndices;
            this.totalStoreSize = totalStoreSize;
            this.dataStreams = dataStreams;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.dataStreamCount = in.readVInt();
            this.backingIndices = in.readVInt();
            this.totalStoreSize = new ByteSizeValue(in);
            this.dataStreams = in.readArray(DataStreamStats::new, DataStreamStats[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(dataStreamCount);
            out.writeVInt(backingIndices);
            totalStoreSize.writeTo(out);
            out.writeArray(dataStreams);
        }

        @Override
        protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
            builder.field("data_stream_count", dataStreamCount);
            builder.field("backing_indices", backingIndices);
            builder.humanReadableField("total_store_size_bytes", "total_store_size", totalStoreSize);
            builder.array("data_streams", (Object[]) dataStreams);
        }

        public int getDataStreamCount() {
            return dataStreamCount;
        }

        public int getBackingIndices() {
            return backingIndices;
        }

        public ByteSizeValue getTotalStoreSize() {
            return totalStoreSize;
        }

        public DataStreamStats[] getDataStreams() {
            return dataStreams;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response response = (Response) obj;
            return dataStreamCount == response.dataStreamCount &&
                backingIndices == response.backingIndices &&
                Objects.equals(totalStoreSize, response.totalStoreSize) &&
                Arrays.equals(dataStreams, response.dataStreams);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataStreamCount, backingIndices, totalStoreSize);
            result = 31 * result + Arrays.hashCode(dataStreams);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                "dataStreamCount=" + dataStreamCount +
                ", backingIndices=" + backingIndices +
                ", totalStoreSize=" + totalStoreSize +
                ", dataStreams=" + Arrays.toString(dataStreams) +
                '}';
        }
    }

    public static class DataStreamStats implements ToXContentObject, Writeable {
        private final String dataStream;
        private final int backingIndices;
        private final ByteSizeValue storeSize;
        private final long maximumTimestamp;

        public DataStreamStats(String dataStream, int backingIndices, ByteSizeValue storeSize, long maximumTimestamp) {
            this.dataStream = dataStream;
            this.backingIndices = backingIndices;
            this.storeSize = storeSize;
            this.maximumTimestamp = maximumTimestamp;
        }

        public DataStreamStats(StreamInput in) throws IOException {
            this.dataStream = in.readString();
            this.backingIndices = in.readVInt();
            this.storeSize = new ByteSizeValue(in);
            this.maximumTimestamp = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStream);
            out.writeVInt(backingIndices);
            storeSize.writeTo(out);
            out.writeVLong(maximumTimestamp);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("data_stream", dataStream);
            builder.field("backing_indices", backingIndices);
            builder.humanReadableField("store_size_bytes", "store_size", storeSize);
            builder.field("maximum_timestamp", maximumTimestamp);
            builder.endObject();
            return builder;
        }

        public String getDataStream() {
            return dataStream;
        }

        public int getBackingIndices() {
            return backingIndices;
        }

        public ByteSizeValue getStoreSize() {
            return storeSize;
        }

        public long getMaximumTimestamp() {
            return maximumTimestamp;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            DataStreamStats that = (DataStreamStats) obj;
            return backingIndices == that.backingIndices &&
                maximumTimestamp == that.maximumTimestamp &&
                Objects.equals(dataStream, that.dataStream) &&
                Objects.equals(storeSize, that.storeSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStream, backingIndices, storeSize, maximumTimestamp);
        }

        @Override
        public String toString() {
            return "DataStreamStats{" +
                "dataStream='" + dataStream + '\'' +
                ", backingIndices=" + backingIndices +
                ", storeSize=" + storeSize +
                ", maximumTimestamp=" + maximumTimestamp +
                '}';
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
            Map<String, AggregatedStats> aggregatedDataStreamsStats = new HashMap<>();
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
                AggregatedStats stats = aggregatedDataStreamsStats.computeIfAbsent(dataStream.getName(), s -> new AggregatedStats());
                stats.storageBytes += shardStat.getStoreStats().sizeInBytes();
                stats.maxTimestamp = Math.max(stats.maxTimestamp, shardStat.getMaxTimestamp());
                stats.backingIndices.add(indexName);
            }

            DataStreamStats[] dataStreamStats = aggregatedDataStreamsStats.entrySet().stream()
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
                aggregatedDataStreamsStats.size(),
                allBackingIndices.size(),
                new ByteSizeValue(totalStoreSizeBytes),
                dataStreamStats
            );
        }
    }
}
