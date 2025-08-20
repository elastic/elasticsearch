/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.TransportVersions.INDEXING_STATS_INCLUDES_RECENT_WRITE_LOAD;
import static org.elasticsearch.TransportVersions.INDEX_STATS_AND_METADATA_INCLUDE_PEAK_WRITE_LOAD;
import static org.elasticsearch.TransportVersions.WRITE_LOAD_INCLUDES_BUFFER_WRITES;

public class IndexingStats implements Writeable, ToXContentFragment {

    public static class Stats implements Writeable, ToXContentFragment {
        private static final TransportVersion WRITE_LOAD_AVG_SUPPORTED_VERSION = TransportVersions.V_8_6_0;

        private long indexCount;
        private long indexTimeInMillis;
        private long indexCurrent;
        private long indexFailedCount;
        private long indexFailedDueToVersionConflictCount;
        private long deleteCount;
        private long deleteTimeInMillis;
        private long deleteCurrent;
        private long noopUpdateCount;
        private long throttleTimeInMillis;
        private boolean isThrottled;
        private long totalIndexingTimeSinceShardStartedInNanos;
        // This is different from totalIndexingTimeSinceShardStartedInNanos, as it also includes the time taken to write indexing buffers
        // to disk on the same thread as the indexing thread. This happens when we are running low on memory and want to push
        // back on indexing, see IndexingMemoryController#writePendingIndexingBuffers()
        private long totalIndexingExecutionTimeSinceShardStartedInNanos;
        private long totalActiveTimeInNanos;
        private double recentIndexingLoad;
        private double peakIndexingLoad;

        Stats() {}

        public Stats(StreamInput in) throws IOException {
            indexCount = in.readVLong();
            indexTimeInMillis = in.readVLong();
            indexCurrent = in.readVLong();
            indexFailedCount = in.readVLong();
            if (in.getTransportVersion().onOrAfter(TransportVersions.TRACK_INDEX_FAILED_DUE_TO_VERSION_CONFLICT_METRIC)) {
                indexFailedDueToVersionConflictCount = in.readVLong();
            }
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
            if (in.getTransportVersion().onOrAfter(INDEXING_STATS_INCLUDES_RECENT_WRITE_LOAD)) {
                recentIndexingLoad = in.readDouble();
            } else {
                // When getting stats from an older version which doesn't have the recent indexing load, better to fall back to the
                // unweighted write load, rather that assuming zero load:
                recentIndexingLoad = totalActiveTimeInNanos > 0
                    ? (double) totalIndexingTimeSinceShardStartedInNanos / totalActiveTimeInNanos
                    : 0;
            }
            if (in.getTransportVersion().onOrAfter(INDEX_STATS_AND_METADATA_INCLUDE_PEAK_WRITE_LOAD)) {
                peakIndexingLoad = in.readDouble();
            } else {
                // When getting stats from an older version which doesn't have the recent indexing load, better to fall back to the
                // unweighted write load, rather that assuming zero load:
                peakIndexingLoad = totalActiveTimeInNanos > 0
                    ? (double) totalIndexingTimeSinceShardStartedInNanos / totalActiveTimeInNanos
                    : 0;
            }
            if (in.getTransportVersion().onOrAfter(WRITE_LOAD_INCLUDES_BUFFER_WRITES)) {
                totalIndexingExecutionTimeSinceShardStartedInNanos = in.readLong();
            } else {
                // When getting stats from an older version which doesn't have the more accurate indexing execution time,
                // better to fall back to the indexing time, rather that assuming zero load:
                totalIndexingExecutionTimeSinceShardStartedInNanos = totalActiveTimeInNanos > 0
                    ? totalIndexingTimeSinceShardStartedInNanos
                    : 0;
            }
        }

        public Stats(
            long indexCount,
            long indexTimeInMillis,
            long indexCurrent,
            long indexFailedCount,
            long indexFailedDueToVersionConflictCount,
            long deleteCount,
            long deleteTimeInMillis,
            long deleteCurrent,
            long noopUpdateCount,
            boolean isThrottled,
            long throttleTimeInMillis,
            long totalIndexingTimeSinceShardStartedInNanos,
            long totalIndexingExecutionTimeSinceShardStartedInNanos,
            long totalActiveTimeInNanos,
            double recentIndexingLoad,
            double peakIndexingLoad
        ) {
            this.indexCount = indexCount;
            this.indexTimeInMillis = indexTimeInMillis;
            this.indexCurrent = indexCurrent;
            this.indexFailedCount = indexFailedCount;
            this.indexFailedDueToVersionConflictCount = indexFailedDueToVersionConflictCount;
            this.deleteCount = deleteCount;
            this.deleteTimeInMillis = deleteTimeInMillis;
            this.deleteCurrent = deleteCurrent;
            this.noopUpdateCount = noopUpdateCount;
            this.isThrottled = isThrottled;
            this.throttleTimeInMillis = throttleTimeInMillis;
            // We store the raw unweighted write load values in order to avoid losing precision when we combine the shard stats
            this.totalIndexingTimeSinceShardStartedInNanos = totalIndexingTimeSinceShardStartedInNanos;
            this.totalIndexingExecutionTimeSinceShardStartedInNanos = totalIndexingExecutionTimeSinceShardStartedInNanos;
            this.totalActiveTimeInNanos = totalActiveTimeInNanos;
            // We store the weighted write load as a double because the calculation is inherently floating point
            this.recentIndexingLoad = recentIndexingLoad;
            this.peakIndexingLoad = peakIndexingLoad;
        }

        public void add(Stats stats) {
            indexCount += stats.indexCount;
            indexTimeInMillis += stats.indexTimeInMillis;
            indexCurrent += stats.indexCurrent;
            indexFailedCount += stats.indexFailedCount;
            indexFailedDueToVersionConflictCount += stats.indexFailedDueToVersionConflictCount;

            deleteCount += stats.deleteCount;
            deleteTimeInMillis += stats.deleteTimeInMillis;
            deleteCurrent += stats.deleteCurrent;

            noopUpdateCount += stats.noopUpdateCount;
            throttleTimeInMillis += stats.throttleTimeInMillis;
            if (isThrottled != stats.isThrottled) {
                isThrottled = true; // When combining if one is throttled set result to throttled.
            }
            totalIndexingTimeSinceShardStartedInNanos += stats.totalIndexingTimeSinceShardStartedInNanos;
            // N.B. getWriteLoad() returns the ratio of these sums, which is the average of the ratios weighted by active time:
            totalIndexingExecutionTimeSinceShardStartedInNanos += stats.totalIndexingExecutionTimeSinceShardStartedInNanos;
            totalActiveTimeInNanos += stats.totalActiveTimeInNanos;
            // We want getRecentWriteLoad() and getPeakWriteLoad() for the aggregated stats to also be the average weighted by active time,
            // so we use the updating formula for a weighted mean:
            if (totalActiveTimeInNanos > 0) {
                recentIndexingLoad += (stats.recentIndexingLoad - recentIndexingLoad) * stats.totalActiveTimeInNanos
                    / totalActiveTimeInNanos;
                peakIndexingLoad += (stats.peakIndexingLoad - peakIndexingLoad) * stats.totalActiveTimeInNanos / totalActiveTimeInNanos;
            }
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
         * The number of indexing operations that failed because of a version conflict (a subset of all index failed operations)
         */
        public long getIndexFailedDueToVersionConflictCount() {
            return indexFailedDueToVersionConflictCount;
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

        /**
         * Returns a measurement of the write load.
         *
         * <p>If this {@link Stats} instance represents a single shard, this is ratio of the sum of the time taken by every index operations
         * since the shard started to the elapsed time since the shard started.
         *
         * <p>If this {@link Stats} instance represents multiple shards, this is the average of that ratio for each shard, weighted by
         * the elapsed time for each shard.
         */
        public double getWriteLoad() {
            return totalActiveTimeInNanos > 0 ? (double) totalIndexingExecutionTimeSinceShardStartedInNanos / totalActiveTimeInNanos : 0;
        }

        /**
         * Returns a measurement of the write load which favours more recent load.
         *
         * <p>If this {@link Stats} instance represents a single shard, this is an Exponentially Weighted Moving Rate based on the time
         * taken by indexing operations in this shard since the shard started.
         *
         * <p>If this {@link Stats} instance represents multiple shards, this is the average of that ratio for each shard, weighted by
         * the elapsed time for each shard.
         */
        public double getRecentWriteLoad() {
            return recentIndexingLoad;
        }

        /**
         * Returns a measurement of the peak write load.
         *
         * <p>If this {@link Stats} instance represents a single shard, this is the highest value that {@link #getRecentWriteLoad()} would
         * return for any of the instances created for this shard since it started (i.e. the highest value seen by any call to
         * {@link InternalIndexingStats#stats}).
         *
         * <p>If this {@link Stats} instance represents multiple shards, this is the average of that value for each shard, weighted by
         * the elapsed time for each shard. (N.B. This is the average of the peak values, <i>not</i> the peak of the average value.)
         */
        public double getPeakWriteLoad() {
            return peakIndexingLoad;
        }

        public long getTotalActiveTimeInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(totalActiveTimeInNanos);
        }

        /**
         * The total amount of time spend on indexing plus writing indexing buffers.
         */
        public long getTotalIndexingExecutionTimeInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(totalIndexingExecutionTimeSinceShardStartedInNanos);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(indexCount);
            out.writeVLong(indexTimeInMillis);
            out.writeVLong(indexCurrent);
            out.writeVLong(indexFailedCount);
            if (out.getTransportVersion().onOrAfter(TransportVersions.TRACK_INDEX_FAILED_DUE_TO_VERSION_CONFLICT_METRIC)) {
                out.writeVLong(indexFailedDueToVersionConflictCount);
            }
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
            if (out.getTransportVersion().onOrAfter(INDEXING_STATS_INCLUDES_RECENT_WRITE_LOAD)) {
                out.writeDouble(recentIndexingLoad);
            }
            if (out.getTransportVersion().onOrAfter(INDEX_STATS_AND_METADATA_INCLUDE_PEAK_WRITE_LOAD)) {
                out.writeDouble(peakIndexingLoad);
            }
            if (out.getTransportVersion().onOrAfter(WRITE_LOAD_INCLUDES_BUFFER_WRITES)) {
                out.writeLong(totalIndexingExecutionTimeSinceShardStartedInNanos);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.INDEX_TOTAL, indexCount);
            builder.humanReadableField(Fields.INDEX_TIME_IN_MILLIS, Fields.INDEX_TIME, getIndexTime());
            builder.field(Fields.INDEX_CURRENT, indexCurrent);
            builder.field(Fields.INDEX_FAILED, indexFailedCount);
            builder.field(Fields.INDEX_FAILED_DUE_TO_VERSION_CONFLICT, indexFailedDueToVersionConflictCount);

            builder.field(Fields.DELETE_TOTAL, deleteCount);
            builder.humanReadableField(Fields.DELETE_TIME_IN_MILLIS, Fields.DELETE_TIME, getDeleteTime());
            builder.field(Fields.DELETE_CURRENT, deleteCurrent);

            builder.field(Fields.NOOP_UPDATE_TOTAL, noopUpdateCount);

            builder.field(Fields.IS_THROTTLED, isThrottled);
            builder.humanReadableField(Fields.THROTTLED_TIME_IN_MILLIS, Fields.THROTTLED_TIME, getThrottleTime());

            builder.field(Fields.WRITE_LOAD, getWriteLoad());
            builder.field(Fields.RECENT_WRITE_LOAD, getRecentWriteLoad());
            builder.field(Fields.PEAK_WRITE_LOAD, getPeakWriteLoad());
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
                && indexFailedDueToVersionConflictCount == that.indexFailedDueToVersionConflictCount
                && deleteCount == that.deleteCount
                && deleteTimeInMillis == that.deleteTimeInMillis
                && deleteCurrent == that.deleteCurrent
                && noopUpdateCount == that.noopUpdateCount
                && isThrottled == that.isThrottled
                && throttleTimeInMillis == that.throttleTimeInMillis
                && totalIndexingTimeSinceShardStartedInNanos == that.totalIndexingTimeSinceShardStartedInNanos
                && totalIndexingExecutionTimeSinceShardStartedInNanos == that.totalIndexingExecutionTimeSinceShardStartedInNanos
                && totalActiveTimeInNanos == that.totalActiveTimeInNanos
                && recentIndexingLoad == that.recentIndexingLoad
                && peakIndexingLoad == that.peakIndexingLoad;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                indexCount,
                indexTimeInMillis,
                indexCurrent,
                indexFailedCount,
                indexFailedDueToVersionConflictCount,
                deleteCount,
                deleteTimeInMillis,
                deleteCurrent,
                noopUpdateCount,
                isThrottled,
                throttleTimeInMillis,
                totalIndexingTimeSinceShardStartedInNanos,
                totalIndexingExecutionTimeSinceShardStartedInNanos,
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
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
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
        static final String INDEX_TOTAL = "index_total";
        static final String INDEX_TIME = "index_time";
        static final String INDEX_TIME_IN_MILLIS = "index_time_in_millis";
        static final String INDEX_CURRENT = "index_current";
        static final String INDEX_FAILED = "index_failed";
        static final String INDEX_FAILED_DUE_TO_VERSION_CONFLICT = "index_failed_due_to_version_conflict";
        static final String DELETE_TOTAL = "delete_total";
        static final String DELETE_TIME = "delete_time";
        static final String DELETE_TIME_IN_MILLIS = "delete_time_in_millis";
        static final String DELETE_CURRENT = "delete_current";
        static final String NOOP_UPDATE_TOTAL = "noop_update_total";
        static final String IS_THROTTLED = "is_throttled";
        static final String THROTTLED_TIME_IN_MILLIS = "throttle_time_in_millis";
        static final String THROTTLED_TIME = "throttle_time";
        static final String WRITE_LOAD = "write_load";
        static final String RECENT_WRITE_LOAD = "recent_write_load";
        static final String PEAK_WRITE_LOAD = "peak_write_load";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeBoolean(false);
        }
    }
}
