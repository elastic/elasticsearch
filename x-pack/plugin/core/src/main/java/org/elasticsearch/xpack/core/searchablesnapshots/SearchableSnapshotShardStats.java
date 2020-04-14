/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class SearchableSnapshotShardStats implements Writeable, ToXContentObject {

    private final List<CacheIndexInputStats> inputStats;
    private final ShardRouting shardRouting;
    private final SnapshotId snapshotId;
    private final IndexId indexId;

    public SearchableSnapshotShardStats(ShardRouting shardRouting, SnapshotId snapshotId, IndexId indexId,
                                        List<CacheIndexInputStats> stats) {
        this.shardRouting = Objects.requireNonNull(shardRouting);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.inputStats = unmodifiableList(Objects.requireNonNull(stats));
    }

    public SearchableSnapshotShardStats(StreamInput in) throws IOException {
        this.shardRouting = new ShardRouting(in);
        this.snapshotId = new SnapshotId(in);
        this.indexId = new IndexId(in);
        this.inputStats = in.readList(CacheIndexInputStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        snapshotId.writeTo(out);
        indexId.writeTo(out);
        out.writeList(inputStats);
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public SnapshotId getSnapshotId() {
        return snapshotId;
    }

    public IndexId getIndexId() {
        return indexId;
    }

    public List<CacheIndexInputStats> getStats() {
        return inputStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("snapshot_uuid", getSnapshotId().getUUID());
            builder.field("index_uuid", getIndexId().getId());
            builder.startObject("shard");
            {
                builder.field("id", shardRouting.shardId());
                builder.field("state", shardRouting.state());
                builder.field("primary", shardRouting.primary());
                builder.field("node", shardRouting.currentNodeId());
                if (shardRouting.relocatingNodeId() != null) {
                    builder.field("relocating_node", shardRouting.relocatingNodeId());
                }
            }
            builder.endObject();
            builder.startArray("files");
            {
                List<CacheIndexInputStats> stats = inputStats.stream()
                    .sorted(Comparator.comparing(CacheIndexInputStats::getFileName)).collect(toList());
                for (CacheIndexInputStats stat : stats) {
                    stat.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SearchableSnapshotShardStats that = (SearchableSnapshotShardStats) other;
        return Objects.equals(shardRouting, that.shardRouting)
            && Objects.equals(snapshotId, that.snapshotId)
            && Objects.equals(indexId, that.indexId)
            && Objects.equals(inputStats, that.inputStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardRouting, snapshotId, indexId, inputStats);
    }

    public static class CacheIndexInputStats implements Writeable, ToXContentObject {

        private final String fileName;
        private final long fileLength;

        private final long openCount;
        private final long closeCount;

        private final Counter forwardSmallSeeks;
        private final Counter backwardSmallSeeks;
        private final Counter forwardLargeSeeks;
        private final Counter backwardLargeSeeks;
        private final Counter contiguousReads;
        private final Counter nonContiguousReads;
        private final Counter cachedBytesRead;
        private final TimedCounter cachedBytesWritten;
        private final TimedCounter directBytesRead;
        private final TimedCounter optimizedBytesRead;

        public CacheIndexInputStats(String fileName, long fileLength, long openCount, long closeCount,
                                    Counter forwardSmallSeeks, Counter backwardSmallSeeks,
                                    Counter forwardLargeSeeks, Counter backwardLargeSeeks,
                                    Counter contiguousReads, Counter nonContiguousReads,
                                    Counter cachedBytesRead, TimedCounter cachedBytesWritten,
                                    TimedCounter directBytesRead, TimedCounter optimizedBytesRead) {
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.openCount = openCount;
            this.closeCount = closeCount;
            this.forwardSmallSeeks = forwardSmallSeeks;
            this.backwardSmallSeeks = backwardSmallSeeks;
            this.forwardLargeSeeks = forwardLargeSeeks;
            this.backwardLargeSeeks = backwardLargeSeeks;
            this.contiguousReads = contiguousReads;
            this.nonContiguousReads = nonContiguousReads;
            this.cachedBytesRead = cachedBytesRead;
            this.cachedBytesWritten = cachedBytesWritten;
            this.directBytesRead = directBytesRead;
            this.optimizedBytesRead = optimizedBytesRead;
        }

        CacheIndexInputStats(final StreamInput in) throws IOException {
            this.fileName = in.readString();
            this.fileLength = in.readVLong();
            this.openCount = in.readVLong();
            this.closeCount = in.readVLong();
            this.forwardSmallSeeks = new Counter(in);
            this.backwardSmallSeeks = new Counter(in);
            this.forwardLargeSeeks = new Counter(in);
            this.backwardLargeSeeks = new Counter(in);
            this.contiguousReads = new Counter(in);
            this.nonContiguousReads = new Counter(in);
            this.cachedBytesRead = new Counter(in);
            this.cachedBytesWritten = new TimedCounter(in);
            this.directBytesRead = new TimedCounter(in);
            this.optimizedBytesRead = new TimedCounter(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fileName);
            out.writeVLong(fileLength);
            out.writeVLong(openCount);
            out.writeVLong(closeCount);

            forwardSmallSeeks.writeTo(out);
            backwardSmallSeeks.writeTo(out);
            forwardLargeSeeks.writeTo(out);
            backwardLargeSeeks.writeTo(out);
            contiguousReads.writeTo(out);
            nonContiguousReads.writeTo(out);
            cachedBytesRead.writeTo(out);
            cachedBytesWritten.writeTo(out);
            directBytesRead.writeTo(out);
            optimizedBytesRead.writeTo(out);
        }

        public String getFileName() {
            return fileName;
        }

        public long getFileLength() {
            return fileLength;
        }

        public long getOpenCount() {
            return openCount;
        }

        public long getCloseCount() {
            return closeCount;
        }

        public Counter getForwardSmallSeeks() {
            return forwardSmallSeeks;
        }

        public Counter getBackwardSmallSeeks() {
            return backwardSmallSeeks;
        }

        public Counter getForwardLargeSeeks() {
            return forwardLargeSeeks;
        }

        public Counter getBackwardLargeSeeks() {
            return backwardLargeSeeks;
        }

        public Counter getContiguousReads() {
            return contiguousReads;
        }

        public Counter getNonContiguousReads() {
            return nonContiguousReads;
        }

        public Counter getCachedBytesRead() {
            return cachedBytesRead;
        }

        public TimedCounter getCachedBytesWritten() {
            return cachedBytesWritten;
        }

        public TimedCounter getDirectBytesRead() {
            return directBytesRead;
        }

        public TimedCounter getOptimizedBytesRead() {
            return optimizedBytesRead;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", getFileName());
                builder.field("length", getFileLength());
                builder.field("open_count", getOpenCount());
                builder.field("close_count", getCloseCount());
                builder.field("contiguous_bytes_read", getContiguousReads());
                builder.field("non_contiguous_bytes_read", getNonContiguousReads());
                builder.field("cached_bytes_read", getCachedBytesRead());
                builder.field("cached_bytes_written", getCachedBytesWritten());
                builder.field("direct_bytes_read", getDirectBytesRead());
                builder.field("optimized_bytes_read", getOptimizedBytesRead());
                {
                    builder.startObject("forward_seeks");
                    builder.field("small", getForwardSmallSeeks());
                    builder.field("large", getForwardLargeSeeks());
                    builder.endObject();
                }
                {
                    builder.startObject("backward_seeks");
                    builder.field("small", getBackwardSmallSeeks());
                    builder.field("large", getBackwardLargeSeeks());
                    builder.endObject();
                }
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            CacheIndexInputStats stats = (CacheIndexInputStats) other;
            return fileLength == stats.fileLength
                && openCount == stats.openCount
                && closeCount == stats.closeCount
                && Objects.equals(fileName, stats.fileName)
                && Objects.equals(forwardSmallSeeks, stats.forwardSmallSeeks)
                && Objects.equals(backwardSmallSeeks, stats.backwardSmallSeeks)
                && Objects.equals(forwardLargeSeeks, stats.forwardLargeSeeks)
                && Objects.equals(backwardLargeSeeks, stats.backwardLargeSeeks)
                && Objects.equals(contiguousReads, stats.contiguousReads)
                && Objects.equals(nonContiguousReads, stats.nonContiguousReads)
                && Objects.equals(cachedBytesRead, stats.cachedBytesRead)
                && Objects.equals(cachedBytesWritten, stats.cachedBytesWritten)
                && Objects.equals(directBytesRead, stats.directBytesRead)
                && Objects.equals(optimizedBytesRead, stats.optimizedBytesRead);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, fileLength, openCount, closeCount,
                forwardSmallSeeks, backwardSmallSeeks,
                forwardLargeSeeks, backwardLargeSeeks,
                contiguousReads, nonContiguousReads,
                cachedBytesRead, cachedBytesWritten,
                directBytesRead, optimizedBytesRead);
        }
    }

    public static class Counter implements Writeable, ToXContentObject {

        private final long count;
        private final long total;
        private final long min;
        private final long max;

        public Counter(final long count, final long total, final long min, final long max) {
            this.count = count;
            this.total = total;
            this.min = min;
            this.max = max;
        }

        Counter(final StreamInput in) throws IOException {
            this.count = in.readZLong();
            this.total = in.readZLong();
            this.min = in.readZLong();
            this.max = in.readZLong();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeZLong(count);
            out.writeZLong(total);
            out.writeZLong(min);
            out.writeZLong(max);
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("count", count);
                builder.field("sum", total);
                builder.field("min", min);
                builder.field("max", max);
                innerToXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        }

        public long getCount() {
            return count;
        }

        public long getTotal() {
            return total;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Counter that = (Counter) other;
            return count == that.count
                && total == that.total
                && min == that.min
                && max == that.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, total, min, max);
        }
    }

    public static class TimedCounter extends Counter {

        private final long totalNanoseconds;

        public TimedCounter(long count, long total, long min, long max, long totalNanoseconds) {
            super(count, total, min, max);
            this.totalNanoseconds = totalNanoseconds;
        }

        TimedCounter(StreamInput in) throws IOException {
            super(in);
            totalNanoseconds = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(totalNanoseconds);
        }

        @Override
        void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            if (builder.humanReadable()) {
                builder.field("time", TimeValue.timeValueNanos(totalNanoseconds).toString());
            }
            builder.field("time_in_nanos", totalNanoseconds);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            if (super.equals(other) == false) {
                return false;
            }
            TimedCounter that = (TimedCounter) other;
            return totalNanoseconds == that.totalNanoseconds;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), totalNanoseconds);
        }
    }

}
