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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexingStats implements Writeable, ToXContentFragment {

    public static class Stats implements Writeable, ToXContentFragment {

        private long indexCount;
        private long indexTimeInMillis;
        private long indexCurrent;
        private long indexFailedCount;
        private long deleteCount;
        private long deleteTimeInMillis;
        private long deleteCurrent;
        private long noopUpdateCount;
        private long throttleTimeInMillis;
        private boolean isThrottled;

        Stats() {}

        public Stats(StreamInput in) throws IOException {
            indexCount = in.readVLong();
            indexTimeInMillis = in.readVLong();
            indexCurrent = in.readVLong();
            indexFailedCount = in.readVLong();
            deleteCount = in.readVLong();
            deleteTimeInMillis = in.readVLong();
            deleteCurrent = in.readVLong();
            noopUpdateCount = in.readVLong();
            isThrottled = in.readBoolean();
            throttleTimeInMillis = in.readLong();
        }

        public Stats(long indexCount, long indexTimeInMillis, long indexCurrent, long indexFailedCount, long deleteCount,
                        long deleteTimeInMillis, long deleteCurrent, long noopUpdateCount, boolean isThrottled, long throttleTimeInMillis) {
            this.indexCount = indexCount;
            this.indexTimeInMillis = indexTimeInMillis;
            this.indexCurrent = indexCurrent;
            this.indexFailedCount = indexFailedCount;
            this.deleteCount = deleteCount;
            this.deleteTimeInMillis = deleteTimeInMillis;
            this.deleteCurrent = deleteCurrent;
            this.noopUpdateCount = noopUpdateCount;
            this.isThrottled = isThrottled;
            this.throttleTimeInMillis = throttleTimeInMillis;
        }

        public void add(Stats stats) {
            indexCount += stats.indexCount;
            indexTimeInMillis += stats.indexTimeInMillis;
            indexCurrent += stats.indexCurrent;
            indexFailedCount += stats.indexFailedCount;

            deleteCount += stats.deleteCount;
            deleteTimeInMillis += stats.deleteTimeInMillis;
            deleteCurrent += stats.deleteCurrent;

            noopUpdateCount += stats.noopUpdateCount;
            throttleTimeInMillis += stats.throttleTimeInMillis;
            if (isThrottled != stats.isThrottled) {
                isThrottled = true; //When combining if one is throttled set result to throttled.
            }
        }

        /**
         * The total number of indexing operations
         */
        public long getIndexCount() { return indexCount; }

        /**
         * The number of failed indexing operations
         */
        public long getIndexFailedCount() { return indexFailedCount; }

        /**
         * The total amount of time spend on executing index operations.
         */
        public TimeValue getIndexTime() { return new TimeValue(indexTimeInMillis); }

        /**
         * Returns the currently in-flight indexing operations.
         */
        public long getIndexCurrent() { return indexCurrent;}

        /**
         * Returns the number of delete operation executed
         */
        public long getDeleteCount() {
            return deleteCount;
        }

        /**
         * Returns if the index is under merge throttling control
         */
        public boolean isThrottled() { return isThrottled; }

        /**
         * Gets the amount of time in a TimeValue that the index has been under merge throttling control
         */
        public TimeValue getThrottleTime() { return new TimeValue(throttleTimeInMillis); }

        /**
         * The total amount of time spend on executing delete operations.
         */
        public TimeValue getDeleteTime() { return new TimeValue(deleteTimeInMillis); }

        /**
         * Returns the currently in-flight delete operations
         */
        public long getDeleteCurrent() {
            return deleteCurrent;
        }

        public long getNoopUpdateCount() {
            return noopUpdateCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(indexCount);
            out.writeVLong(indexTimeInMillis);
            out.writeVLong(indexCurrent);
            out.writeVLong(indexFailedCount);
            out.writeVLong(deleteCount);
            out.writeVLong(deleteTimeInMillis);
            out.writeVLong(deleteCurrent);
            out.writeVLong(noopUpdateCount);
            out.writeBoolean(isThrottled);
            out.writeLong(throttleTimeInMillis);

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.INDEX_TOTAL, indexCount);
            builder.humanReadableField(Fields.INDEX_TIME_IN_MILLIS, Fields.INDEX_TIME, getIndexTime());
            builder.field(Fields.INDEX_CURRENT, indexCurrent);
            builder.field(Fields.INDEX_FAILED, indexFailedCount);

            builder.field(Fields.DELETE_TOTAL, deleteCount);
            builder.humanReadableField(Fields.DELETE_TIME_IN_MILLIS, Fields.DELETE_TIME, getDeleteTime());
            builder.field(Fields.DELETE_CURRENT, deleteCurrent);

            builder.field(Fields.NOOP_UPDATE_TOTAL, noopUpdateCount);

            builder.field(Fields.IS_THROTTLED, isThrottled);
            builder.humanReadableField(Fields.THROTTLED_TIME_IN_MILLIS, Fields.THROTTLED_TIME, getThrottleTime());
            return builder;
        }
    }

    private final Stats totalStats;

    @Nullable
    private Map<String, Stats> typeStats;

    public IndexingStats() {
        totalStats = new Stats();
    }

    public IndexingStats(StreamInput in) throws IOException {
        totalStats = new Stats(in);
        if (in.readBoolean()) {
            typeStats = in.readMap(StreamInput::readString, Stats::new);
        }
    }

    public IndexingStats(Stats totalStats, @Nullable Map<String, Stats> typeStats) {
        this.totalStats = totalStats;
        this.typeStats = typeStats;
    }

    public void add(IndexingStats indexingStats) {
        add(indexingStats, true);
    }

    public void add(IndexingStats indexingStats, boolean includeTypes) {
        if (indexingStats == null) {
            return;
        }
        addTotals(indexingStats);
        if (includeTypes && indexingStats.typeStats != null && !indexingStats.typeStats.isEmpty()) {
            if (typeStats == null) {
                typeStats = new HashMap<>(indexingStats.typeStats.size());
            }
            for (Map.Entry<String, Stats> entry : indexingStats.typeStats.entrySet()) {
                Stats stats = typeStats.get(entry.getKey());
                if (stats == null) {
                    typeStats.put(entry.getKey(), entry.getValue());
                } else {
                    stats.add(entry.getValue());
                }
            }
        }
    }

    public void addTotals(IndexingStats indexingStats) {
        if (indexingStats == null) {
            return;
        }
        totalStats.add(indexingStats.totalStats);
    }

    public Stats getTotal() {
        return this.totalStats;
    }

    @Nullable
    public Map<String, Stats> getTypeStats() {
        return this.typeStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEXING);
        totalStats.toXContent(builder, params);
        if (typeStats != null && !typeStats.isEmpty()) {
            builder.startObject(Fields.TYPES);
            for (Map.Entry<String, Stats> entry : typeStats.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String INDEXING = "indexing";
        static final String TYPES = "types";
        static final String INDEX_TOTAL = "index_total";
        static final String INDEX_TIME = "index_time";
        static final String INDEX_TIME_IN_MILLIS = "index_time_in_millis";
        static final String INDEX_CURRENT = "index_current";
        static final String INDEX_FAILED = "index_failed";
        static final String DELETE_TOTAL = "delete_total";
        static final String DELETE_TIME = "delete_time";
        static final String DELETE_TIME_IN_MILLIS = "delete_time_in_millis";
        static final String DELETE_CURRENT = "delete_current";
        static final String NOOP_UPDATE_TOTAL = "noop_update_total";
        static final String IS_THROTTLED = "is_throttled";
        static final String THROTTLED_TIME_IN_MILLIS = "throttle_time_in_millis";
        static final String THROTTLED_TIME = "throttle_time";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        if (typeStats == null || typeStats.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(typeStats, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        }
    }
}
