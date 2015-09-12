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

package org.elasticsearch.index.indexing;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class IndexingStats implements Streamable, ToXContent {

    public static class Stats implements Streamable, ToXContent {

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

        Stats() {

        }

        public Stats(long indexCount, long indexTimeInMillis, long indexCurrent, long indexFailedCount, long deleteCount, long deleteTimeInMillis, long deleteCurrent, long noopUpdateCount, boolean isThrottled, long throttleTimeInMillis) {
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

        public long getIndexCount() {
            return indexCount;
        }

        public long getIndexFailedCount() {
            return indexFailedCount;
        }

        public TimeValue getIndexTime() {
            return new TimeValue(indexTimeInMillis);
        }

        public long getIndexTimeInMillis() {
            return indexTimeInMillis;
        }

        public long getIndexCurrent() {
            return indexCurrent;
        }

        public long getDeleteCount() {
            return deleteCount;
        }

        /**
         * Returns if the index is under merge throttling control
         * @return
         */
        public boolean isThrottled() {
            return isThrottled;
        }

        /**
         * Gets the amount of time in milliseconds that the index has been under merge throttling control
         * @return
         */
        public long getThrottleTimeInMillis() {
            return throttleTimeInMillis;
        }

        /**
         * Gets the amount of time in a TimeValue that the index has been under merge throttling control
         * @return
         */
        public TimeValue getThrottleTime() {
            return new TimeValue(throttleTimeInMillis);
        }

        public TimeValue getDeleteTime() {
            return new TimeValue(deleteTimeInMillis);
        }

        public long getDeleteTimeInMillis() {
            return deleteTimeInMillis;
        }

        public long getDeleteCurrent() {
            return deleteCurrent;
        }

        public long getNoopUpdateCount() {
            return noopUpdateCount;
        }

        public static Stats readStats(StreamInput in) throws IOException {
            Stats stats = new Stats();
            stats.readFrom(in);
            return stats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            indexCount = in.readVLong();
            indexTimeInMillis = in.readVLong();
            indexCurrent = in.readVLong();

            if(in.getVersion().onOrAfter(Version.V_2_1_0)){
                indexFailedCount = in.readVLong();
            }

            deleteCount = in.readVLong();
            deleteTimeInMillis = in.readVLong();
            deleteCurrent = in.readVLong();
            noopUpdateCount = in.readVLong();
            isThrottled = in.readBoolean();
            throttleTimeInMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(indexCount);
            out.writeVLong(indexTimeInMillis);
            out.writeVLong(indexCurrent);

            if(out.getVersion().onOrAfter(Version.V_2_1_0)) {
                out.writeVLong(indexFailedCount);
            }

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
            builder.timeValueField(Fields.INDEX_TIME_IN_MILLIS, Fields.INDEX_TIME, indexTimeInMillis);
            builder.field(Fields.INDEX_CURRENT, indexCurrent);
            builder.field(Fields.INDEX_FAILED, indexFailedCount);

            builder.field(Fields.DELETE_TOTAL, deleteCount);
            builder.timeValueField(Fields.DELETE_TIME_IN_MILLIS, Fields.DELETE_TIME, deleteTimeInMillis);
            builder.field(Fields.DELETE_CURRENT, deleteCurrent);

            builder.field(Fields.NOOP_UPDATE_TOTAL, noopUpdateCount);

            builder.field(Fields.IS_THROTTLED, isThrottled);
            builder.timeValueField(Fields.THROTTLED_TIME_IN_MILLIS, Fields.THROTTLED_TIME, throttleTimeInMillis);
            return builder;
        }
    }

    private Stats totalStats;

    @Nullable
    private Map<String, Stats> typeStats;

    public IndexingStats() {
        totalStats = new Stats();
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
        totalStats.add(indexingStats.totalStats);
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
                builder.startObject(entry.getKey(), XContentBuilder.FieldCaseConversion.NONE);
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDEXING = new XContentBuilderString("indexing");
        static final XContentBuilderString TYPES = new XContentBuilderString("types");
        static final XContentBuilderString INDEX_TOTAL = new XContentBuilderString("index_total");
        static final XContentBuilderString INDEX_TIME = new XContentBuilderString("index_time");
        static final XContentBuilderString INDEX_TIME_IN_MILLIS = new XContentBuilderString("index_time_in_millis");
        static final XContentBuilderString INDEX_CURRENT = new XContentBuilderString("index_current");
        static final XContentBuilderString INDEX_FAILED = new XContentBuilderString("index_failed");
        static final XContentBuilderString DELETE_TOTAL = new XContentBuilderString("delete_total");
        static final XContentBuilderString DELETE_TIME = new XContentBuilderString("delete_time");
        static final XContentBuilderString DELETE_TIME_IN_MILLIS = new XContentBuilderString("delete_time_in_millis");
        static final XContentBuilderString DELETE_CURRENT = new XContentBuilderString("delete_current");
        static final XContentBuilderString NOOP_UPDATE_TOTAL = new XContentBuilderString("noop_update_total");
        static final XContentBuilderString IS_THROTTLED = new XContentBuilderString("is_throttled");
        static final XContentBuilderString THROTTLED_TIME_IN_MILLIS = new XContentBuilderString("throttle_time_in_millis");
        static final XContentBuilderString THROTTLED_TIME = new XContentBuilderString("throttle_time");
    }

    public static IndexingStats readIndexingStats(StreamInput in) throws IOException {
        IndexingStats indexingStats = new IndexingStats();
        indexingStats.readFrom(in);
        return indexingStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalStats = Stats.readStats(in);
        if (in.readBoolean()) {
            int size = in.readVInt();
            typeStats = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                typeStats.put(in.readString(), Stats.readStats(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        if (typeStats == null || typeStats.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(typeStats.size());
            for (Map.Entry<String, Stats> entry : typeStats.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }
}
