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

package org.elasticsearch.client.indices;

import org.elasticsearch.client.core.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class DataStreamsStatsResponse extends BroadcastResponse {

    private final int dataStreamCount;
    private final int backingIndices;
    private final ByteSizeValue totalStoreSize;
    private final Map<String, DataStreamStats> dataStreams;

    protected DataStreamsStatsResponse(Shards shards, int dataStreamCount, int backingIndices, ByteSizeValue totalStoreSize,
                                       Map<String, DataStreamStats> dataStreams) {
        super(shards);
        this.dataStreamCount = dataStreamCount;
        this.backingIndices = backingIndices;
        this.totalStoreSize = totalStoreSize;
        this.dataStreams = dataStreams;
    }

    private static final ParseField DATA_STREAM_COUNT = new ParseField("data_stream_count");
    private static final ParseField BACKING_INDICES = new ParseField("backing_indices");
    private static final ParseField TOTAL_STORE_SIZE_BYTES = new ParseField("total_store_size_bytes");
    private static final ParseField DATA_STREAMS = new ParseField("data_streams");
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField STORE_SIZE_BYTES = new ParseField("store_size_bytes");
    private static final ParseField MAXIMUM_TIMESTAMP = new ParseField("maximum_timestamp");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamsStatsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "data_streams_stats", true, arg -> {
        Shards shards = (Shards) arg[0];
        Integer dataStreamCount = ((Integer) arg[1]);
        Integer backingIndices = ((Integer) arg[2]);
        ByteSizeValue totalStoreSize = ((ByteSizeValue) arg[3]);
        Map<String, DataStreamStats> dataStreams = new HashMap<>();
        for (DataStreamStats dataStreamStats : ((List<DataStreamStats>) arg[4])) {
            dataStreams.put(dataStreamStats.dataStream, dataStreamStats);
        }
        return new DataStreamsStatsResponse(shards, dataStreamCount, backingIndices, totalStoreSize, dataStreams);
    });

    private static final ConstructingObjectParser<DataStreamStats, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
        "data_streams_stats.entry", true, arg -> {
        String dataStream = ((String) arg[0]);
        Integer backingIndices = ((Integer) arg[1]);
        ByteSizeValue storeSize = ((ByteSizeValue) arg[2]);
        Long maximumTimestamp = ((Long) arg[3]);
        return new DataStreamStats(dataStream, backingIndices, storeSize, maximumTimestamp);
    });

    static {
        declareShardsField(PARSER);
        PARSER.declareInt(constructorArg(), DATA_STREAM_COUNT);
        PARSER.declareInt(constructorArg(), BACKING_INDICES);
        PARSER.declareField(constructorArg(), (p, c) -> new ByteSizeValue(p.longValue()), TOTAL_STORE_SIZE_BYTES,
            ObjectParser.ValueType.VALUE);
        PARSER.declareObjectArray(constructorArg(), ENTRY_PARSER, DATA_STREAMS);
        ENTRY_PARSER.declareString(constructorArg(), DATA_STREAM);
        ENTRY_PARSER.declareInt(constructorArg(), BACKING_INDICES);
        ENTRY_PARSER.declareField(constructorArg(), (p, c) -> new ByteSizeValue(p.longValue()), STORE_SIZE_BYTES,
            ObjectParser.ValueType.VALUE);
        ENTRY_PARSER.declareLong(constructorArg(), MAXIMUM_TIMESTAMP);
    }

    public static DataStreamsStatsResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
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

    public Map<String, DataStreamStats> getDataStreams() {
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
        DataStreamsStatsResponse that = (DataStreamsStatsResponse) obj;
        return dataStreamCount == that.dataStreamCount &&
            backingIndices == that.backingIndices &&
            Objects.equals(totalStoreSize, that.totalStoreSize) &&
            Objects.equals(dataStreams, that.dataStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataStreamCount, backingIndices, totalStoreSize, dataStreams);
    }

    @Override
    public String toString() {
        return "DataStreamsStatsResponse{" +
            "dataStreamCount=" + dataStreamCount +
            ", backingIndices=" + backingIndices +
            ", totalStoreSize=" + totalStoreSize +
            ", dataStreams=" + dataStreams +
            '}';
    }

    public static class DataStreamStats {

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
}
