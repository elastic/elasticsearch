/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class SearchableSnapshotStats implements Writeable, ToXContentObject {

    private final List<CacheDirectoryStats> directoryStats;

    public SearchableSnapshotStats(final List<CacheDirectoryStats> directoryStats) {
        this.directoryStats = unmodifiableList(Objects.requireNonNull(directoryStats));
    }

    public SearchableSnapshotStats(final StreamInput in) throws IOException {
        this.directoryStats = unmodifiableList(in.readList(CacheDirectoryStats::new));
    }

    public List<CacheDirectoryStats> getStats() {
        return directoryStats;
    }

    public boolean isEmpty() {
        if (directoryStats.isEmpty()) {
            return true;
        }
        return directoryStats.stream().map(CacheDirectoryStats::getStats).allMatch(List::isEmpty);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeList(directoryStats);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final List<Index> indices = getStats().stream()
            .filter(stats -> stats.getStats().isEmpty() == false)
            .map(CacheDirectoryStats::getShardId)
            .map(ShardId::getIndex)
            .sorted(Comparator.comparing(Index::getName))
            .collect(toList());
        builder.startObject();
        {
            builder.startObject("indices");
            for (Index index : indices) {
                builder.startObject(index.getName());
                {
                    builder.startArray("shards");
                    {
                        List<CacheDirectoryStats> listOfDirectoryStats = getStats().stream()
                            .filter(dirStats -> dirStats.getShardId().getIndex().equals(index))
                            .sorted(Comparator.comparingInt(dir -> dir.getShardId().id()))
                            .collect(Collectors.toList());
                        for (CacheDirectoryStats stats : listOfDirectoryStats) {
                            builder.value(stats);
                        }
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SearchableSnapshotStats that = (SearchableSnapshotStats) other;
        return Objects.equals(directoryStats, that.directoryStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directoryStats);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class CacheDirectoryStats implements Writeable, ToXContentObject {

        private final List<CacheIndexInputStats> inputStats;
        private final SnapshotId snapshotId;
        private final IndexId indexId;
        private final ShardId shardId;

        public CacheDirectoryStats(SnapshotId snapshotId, IndexId indexId, ShardId shardId, List<CacheIndexInputStats> inputStats) {
            this.snapshotId = Objects.requireNonNull(snapshotId);
            this.indexId = Objects.requireNonNull(indexId);
            this.shardId = Objects.requireNonNull(shardId);
            this.inputStats = unmodifiableList(Objects.requireNonNull(inputStats));
        }

        CacheDirectoryStats(final StreamInput in) throws IOException {
            this.snapshotId = new SnapshotId(in);
            this.indexId = new IndexId(in);
            this.shardId = new ShardId(in);
            this.inputStats = in.readList(CacheIndexInputStats::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            snapshotId.writeTo(out);
            indexId.writeTo(out);
            shardId.writeTo(out);
            out.writeList(inputStats);
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public IndexId getIndexId() {
            return indexId;
        }

        public ShardId getShardId() {
            return shardId;
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
                builder.field("shard", getShardId().getId());
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
            CacheDirectoryStats that = (CacheDirectoryStats) other;
            return Objects.equals(inputStats, that.inputStats)
                && Objects.equals(snapshotId, that.snapshotId)
                && Objects.equals(indexId, that.indexId)
                && Objects.equals(shardId, that.shardId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inputStats, snapshotId, indexId, shardId);
        }
    }

    public static class CacheIndexInputStats implements Writeable, ToXContentObject {

        private final String fileName;
        private final long fileLength;

        private final long openCount;
        private final long innerCount;
        private final long closeCount;

        private final Counter forwardSmallSeeks;
        private final Counter backwardSmallSeeks;
        private final Counter forwardLargeSeeks;
        private final Counter backwardLargeSeeks;
        private final Counter contiguousReads;
        private final Counter nonContiguousReads;
        private final Counter cachedBytesRead;
        private final Counter cachedBytesWritten;
        private final Counter directBytesRead;

        public CacheIndexInputStats(String fileName, long fileLength, long openCount, long innerCount, long closeCount,
                                    Counter forwardSmallSeeks, Counter backwardSmallSeeks,
                                    Counter forwardLargeSeeks, Counter backwardLargeSeeks,
                                    Counter contiguousReads, Counter nonContiguousReads,
                                    Counter cachedBytesRead, Counter cachedBytesWritten,
                                    Counter directBytesRead) {
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.openCount = openCount;
            this.innerCount = innerCount;
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
        }

        CacheIndexInputStats(final StreamInput in) throws IOException {
            this.fileName = in.readString();
            this.fileLength = in.readVLong();
            this.openCount = in.readVLong();
            this.innerCount = in.readVLong();
            this.closeCount = in.readVLong();
            this.forwardSmallSeeks = new Counter(in);
            this.backwardSmallSeeks = new Counter(in);
            this.forwardLargeSeeks = new Counter(in);
            this.backwardLargeSeeks = new Counter(in);
            this.contiguousReads = new Counter(in);
            this.nonContiguousReads = new Counter(in);
            this.cachedBytesRead = new Counter(in);
            this.cachedBytesWritten = new Counter(in);
            this.directBytesRead = new Counter(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fileName);
            out.writeVLong(fileLength);
            out.writeVLong(openCount);
            out.writeVLong(innerCount);
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

        public long getInnerCount() {
            return innerCount;
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

        public Counter getCachedBytesWritten() {
            return cachedBytesWritten;
        }

        public Counter getDirectBytesRead() {
            return directBytesRead;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", getFileName());
                builder.field("length", getFileLength());
                builder.field("open_count", getOpenCount());
                builder.field("inner_count", getInnerCount());
                builder.field("close_count", getCloseCount());
                builder.field("contiguous_bytes_read", getContiguousReads());
                builder.field("non_contiguous_bytes_read", getNonContiguousReads());
                builder.field("cached_bytes_read", getCachedBytesRead());
                builder.field("cached_bytes_written", getCachedBytesWritten());
                builder.field("direct_bytes_read", getDirectBytesRead());
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
                && innerCount == stats.innerCount
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
                && Objects.equals(directBytesRead, stats.directBytesRead);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, fileLength, openCount, innerCount, closeCount,
                forwardSmallSeeks, backwardSmallSeeks,
                forwardLargeSeeks, backwardLargeSeeks,
                contiguousReads, nonContiguousReads,
                cachedBytesRead, cachedBytesWritten,
                directBytesRead);
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("count", count);
                builder.field("sum", total);
                builder.field("min", min);
                builder.field("max", max);
            }
            builder.endObject();
            return builder;
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
}
