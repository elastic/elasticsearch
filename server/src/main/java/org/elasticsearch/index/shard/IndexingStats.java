/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class IndexingStats implements Writeable, ToXContentFragment {

    public static class Stats implements Writeable, ToXContentFragment {
        private static final TransportVersion WRITE_LOAD_AVG_SUPPORTED_VERSION = TransportVersion.V_8_6_0;

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
        private long totalIndexingTimeSinceShardStartedInNanos;
        private long totalActiveTimeInNanos;

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
            if (in.getTransportVersion().onOrAfter(WRITE_LOAD_AVG_SUPPORTED_VERSION)) {
                totalIndexingTimeSinceShardStartedInNanos = in.readLong();
                totalActiveTimeInNanos = in.readLong();
            }
        }

        public Stats(
            long indexCount,
            long indexTimeInMillis,
            long indexCurrent,
            long indexFailedCount,
            long deleteCount,
            long deleteTimeInMillis,
            long deleteCurrent,
            long noopUpdateCount,
            boolean isThrottled,
            long throttleTimeInMillis,
            long totalIndexingTimeSinceShardStartedInNanos,
            long totalActiveTimeInNanos
        ) {
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
            // We store the raw write-load values in order to avoid losing precision when we combine the shard stats
            this.totalIndexingTimeSinceShardStartedInNanos = totalIndexingTimeSinceShardStartedInNanos;
            this.totalActiveTimeInNanos = totalActiveTimeInNanos;
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
                isThrottled = true; // When combining if one is throttled set result to throttled.
            }
            totalIndexingTimeSinceShardStartedInNanos += stats.totalIndexingTimeSinceShardStartedInNanos;
            totalActiveTimeInNanos += stats.totalActiveTimeInNanos;
        }

        /**
         * The total number of indexing operations
         */
        public long getIndexCount() {
            return indexCount;
        }

        /**
         * The number of failed indexing operations
         */
        public long getIndexFailedCount() {
            return indexFailedCount;
        }

        /**
         * The total amount of time spend on executing index operations.
         */
        public TimeValue getIndexTime() {
            return new TimeValue(indexTimeInMillis);
        }

        /**
         * Returns the currently in-flight indexing operations.
         */
        public long getIndexCurrent() {
            return indexCurrent;
        }

        /**
         * Returns the number of delete operation executed
         */
        public long getDeleteCount() {
            return deleteCount;
        }

        /**
         * Returns if the index is under merge throttling control
         */
        public boolean isThrottled() {
            return isThrottled;
        }

        /**
         * Gets the amount of time in a TimeValue that the index has been under merge throttling control
         */
        public TimeValue getThrottleTime() {
            return new TimeValue(throttleTimeInMillis);
        }

        /**
         * The total amount of time spend on executing delete operations.
         */
        public TimeValue getDeleteTime() {
            return new TimeValue(deleteTimeInMillis);
        }

        /**
         * Returns the currently in-flight delete operations
         */
        public long getDeleteCurrent() {
            return deleteCurrent;
        }

        public long getNoopUpdateCount() {
            return noopUpdateCount;
        }

        public double getWriteLoad() {
            return totalActiveTimeInNanos > 0 ? (double) totalIndexingTimeSinceShardStartedInNanos / totalActiveTimeInNanos : 0;
        }

        public long getTotalActiveTimeInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(totalActiveTimeInNanos);
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
            if (out.getTransportVersion().onOrAfter(WRITE_LOAD_AVG_SUPPORTED_VERSION)) {
                out.writeLong(totalIndexingTimeSinceShardStartedInNanos);
                out.writeLong(totalActiveTimeInNanos);
            }
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

            builder.field(Fields.WRITE_LOAD, getWriteLoad());
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats that = (Stats) o;
            return indexCount == that.indexCount
                && indexTimeInMillis == that.indexTimeInMillis
                && indexCurrent == that.indexCurrent
                && indexFailedCount == that.indexFailedCount
                && deleteCount == that.deleteCount
                && deleteTimeInMillis == that.deleteTimeInMillis
                && deleteCurrent == that.deleteCurrent
                && noopUpdateCount == that.noopUpdateCount
                && isThrottled == that.isThrottled
                && throttleTimeInMillis == that.throttleTimeInMillis
                && totalIndexingTimeSinceShardStartedInNanos == that.totalIndexingTimeSinceShardStartedInNanos
                && totalActiveTimeInNanos == that.totalActiveTimeInNanos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                indexCount,
                indexTimeInMillis,
                indexCurrent,
                indexFailedCount,
                deleteCount,
                deleteTimeInMillis,
                deleteCurrent,
                noopUpdateCount,
                isThrottled,
                throttleTimeInMillis,
                totalIndexingTimeSinceShardStartedInNanos,
                totalActiveTimeInNanos
            );
        }
    }

    private final Stats totalStats;

    public IndexingStats() {
        totalStats = new Stats();
    }

    public IndexingStats(StreamInput in) throws IOException {
        totalStats = new Stats(in);
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            if (in.readBoolean()) {
                Map<String, Stats> typeStats = in.readMap(Stats::new);
                assert typeStats.size() == 1;
                assert typeStats.containsKey(MapperService.SINGLE_MAPPING_NAME);
            }
        }
    }

    public IndexingStats(Stats totalStats) {
        this.totalStats = totalStats;
    }

    public void add(IndexingStats indexingStats) {
        if (indexingStats == null) {
            return;
        }
        addTotals(indexingStats);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEXING);
        totalStats.toXContent(builder, params);
        if (builder.getRestApiVersion() == RestApiVersion.V_7 && params.param("types") != null) {
            builder.startObject(Fields.TYPES);
            builder.startObject(MapperService.SINGLE_MAPPING_NAME);
            totalStats.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexingStats that = (IndexingStats) o;
        return Objects.equals(totalStats, that.totalStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalStats);
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
        static final String WRITE_LOAD = "write_load";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            out.writeBoolean(false);
        }
    }
}
