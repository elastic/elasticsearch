/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class DataStreamsStatsAction extends ActionType<DataStreamsStatsAction.Response> {

    public static final DataStreamsStatsAction INSTANCE = new DataStreamsStatsAction();
    public static final String NAME = "indices:monitor/data_stream/stats";

    public DataStreamsStatsAction() {
        super(NAME);
    }

    public static class Request extends BroadcastRequest<Request> {
        public Request() {
            // this doesn't really matter since data stream name resolution isn't affected by IndicesOptions and
            // a data stream's backing indices are retrieved from its metadata
            super(
                null,
                IndicesOptions.builder()
                    .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
                    .wildcardOptions(
                        IndicesOptions.WildcardOptions.builder()
                            .matchOpen(true)
                            .matchClosed(true)
                            .includeHidden(false)
                            .resolveAliases(false)
                            .allowEmptyExpressions(true)
                            .build()
                    )
                    .gatekeeperOptions(
                        IndicesOptions.GatekeeperOptions.builder()
                            .allowAliasToMultipleIndices(true)
                            .allowClosedIndices(true)
                            .ignoreThrottled(false)
                            .allowFailureIndices(true)
                            .build()
                    )
                    .selectorOptions(IndicesOptions.SelectorOptions.ALL_APPLICABLE)
                    .build()
            );
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

        public Response(
            int totalShards,
            int successfulShards,
            int failedShards,
            List<DefaultShardOperationFailedException> shardFailures,
            int dataStreamCount,
            int backingIndices,
            ByteSizeValue totalStoreSize,
            DataStreamStats[] dataStreams
        ) {
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
            this.totalStoreSize = ByteSizeValue.readFrom(in);
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
            builder.xContentList("data_streams", dataStreams);
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
            return dataStreamCount == response.dataStreamCount
                && backingIndices == response.backingIndices
                && Objects.equals(totalStoreSize, response.totalStoreSize)
                && Arrays.equals(dataStreams, response.dataStreams);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataStreamCount, backingIndices, totalStoreSize);
            result = 31 * result + Arrays.hashCode(dataStreams);
            return result;
        }

        @Override
        public String toString() {
            return "Response{"
                + "dataStreamCount="
                + dataStreamCount
                + ", backingIndices="
                + backingIndices
                + ", totalStoreSize="
                + totalStoreSize
                + ", dataStreams="
                + Arrays.toString(dataStreams)
                + '}';
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
            this.storeSize = ByteSizeValue.readFrom(in);
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
            return backingIndices == that.backingIndices
                && maximumTimestamp == that.maximumTimestamp
                && Objects.equals(dataStream, that.dataStream)
                && Objects.equals(storeSize, that.storeSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStream, backingIndices, storeSize, maximumTimestamp);
        }

        @Override
        public String toString() {
            return "DataStreamStats{"
                + "dataStream='"
                + dataStream
                + '\''
                + ", backingIndices="
                + backingIndices
                + ", storeSize="
                + storeSize
                + ", maximumTimestamp="
                + maximumTimestamp
                + '}';
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

}
