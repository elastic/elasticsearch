/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

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

    public SearchableSnapshotShardStats(
        ShardRouting shardRouting,
        SnapshotId snapshotId,
        IndexId indexId,
        List<CacheIndexInputStats> stats
    ) {
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
                    .sorted(Comparator.comparing(CacheIndexInputStats::getFileExt))
                    .collect(toList());
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

        private final String fileExt;
        private final long numFiles;
        private final ByteSizeValue totalSize;
        private final ByteSizeValue minSize;
        private final ByteSizeValue maxSize;

        private final long openCount;
        private final long closeCount;

        private final Counter forwardSmallSeeks;
        private final Counter backwardSmallSeeks;
        private final Counter forwardLargeSeeks;
        private final Counter backwardLargeSeeks;
        private final Counter contiguousReads;
        private final Counter nonContiguousReads;
        private final Counter cachedBytesRead;
        private final Counter indexCacheBytesRead;
        private final TimedCounter cachedBytesWritten;
        private final TimedCounter directBytesRead;
        private final TimedCounter optimizedBytesRead;
        private final Counter blobStoreBytesRequested;
        private final Counter luceneBytesRead;
        private final long currentIndexCacheFills;

        public CacheIndexInputStats(
            String fileExt,
            long numFiles,
            ByteSizeValue totalSize,
            ByteSizeValue minSize,
            ByteSizeValue maxSize,
            long openCount,
            long closeCount,
            Counter forwardSmallSeeks,
            Counter backwardSmallSeeks,
            Counter forwardLargeSeeks,
            Counter backwardLargeSeeks,
            Counter contiguousReads,
            Counter nonContiguousReads,
            Counter cachedBytesRead,
            Counter indexCacheBytesRead,
            TimedCounter cachedBytesWritten,
            TimedCounter directBytesRead,
            TimedCounter optimizedBytesRead,
            Counter blobStoreBytesRequested,
            Counter luceneBytesRead,
            long currentIndexCacheFills
        ) {
            this.fileExt = fileExt;
            this.numFiles = numFiles;
            this.totalSize = totalSize;
            this.minSize = minSize;
            this.maxSize = maxSize;
            this.openCount = openCount;
            this.closeCount = closeCount;
            this.forwardSmallSeeks = forwardSmallSeeks;
            this.backwardSmallSeeks = backwardSmallSeeks;
            this.forwardLargeSeeks = forwardLargeSeeks;
            this.backwardLargeSeeks = backwardLargeSeeks;
            this.contiguousReads = contiguousReads;
            this.nonContiguousReads = nonContiguousReads;
            this.cachedBytesRead = cachedBytesRead;
            this.indexCacheBytesRead = indexCacheBytesRead;
            this.cachedBytesWritten = cachedBytesWritten;
            this.directBytesRead = directBytesRead;
            this.optimizedBytesRead = optimizedBytesRead;
            this.blobStoreBytesRequested = blobStoreBytesRequested;
            this.luceneBytesRead = luceneBytesRead;
            this.currentIndexCacheFills = currentIndexCacheFills;
        }

        CacheIndexInputStats(final StreamInput in) throws IOException {
            if (in.getVersion().before(Version.V_7_12_0)) {
                // This API is currently only used internally for testing, so BWC breaking changes are OK.
                // We just throw an exception here to get a better error message in case this would be called
                // in a mixed version cluster
                throw new IllegalArgumentException("BWC breaking change for internal API");
            }
            this.fileExt = in.readString();
            this.numFiles = in.readVLong();
            if (in.getVersion().before(Version.V_7_13_0)) {
                this.totalSize = new ByteSizeValue(in.readVLong());
                this.minSize = ByteSizeValue.ZERO;
                this.maxSize = ByteSizeValue.ZERO;
            } else {
                this.totalSize = new ByteSizeValue(in);
                this.minSize = new ByteSizeValue(in);
                this.maxSize = new ByteSizeValue(in);
            }
            this.openCount = in.readVLong();
            this.closeCount = in.readVLong();
            this.forwardSmallSeeks = new Counter(in);
            this.backwardSmallSeeks = new Counter(in);
            this.forwardLargeSeeks = new Counter(in);
            this.backwardLargeSeeks = new Counter(in);
            this.contiguousReads = new Counter(in);
            this.nonContiguousReads = new Counter(in);
            this.cachedBytesRead = new Counter(in);
            this.indexCacheBytesRead = new Counter(in);
            this.cachedBytesWritten = new TimedCounter(in);
            this.directBytesRead = new TimedCounter(in);
            this.optimizedBytesRead = new TimedCounter(in);
            this.blobStoreBytesRequested = new Counter(in);
            if (in.getVersion().onOrAfter(Version.V_7_13_0)) {
                this.luceneBytesRead = new Counter(in);
            } else {
                this.luceneBytesRead = new Counter(0, 0, 0, 0);
            }
            this.currentIndexCacheFills = in.readVLong();
        }

        public static CacheIndexInputStats combine(CacheIndexInputStats cis1, CacheIndexInputStats cis2) {
            if (cis1.getFileExt().equals(cis2.getFileExt()) == false) {
                assert false : "can only combine same file extensions";
                throw new IllegalArgumentException(
                    "can only combine same file extensions but was " + cis1.fileExt + " and " + cis2.fileExt
                );
            }
            return new CacheIndexInputStats(
                cis1.fileExt,
                cis1.numFiles + cis2.numFiles,
                new ByteSizeValue(Math.addExact(cis1.totalSize.getBytes(), cis2.totalSize.getBytes())),
                new ByteSizeValue(Math.min(cis1.minSize.getBytes(), cis2.minSize.getBytes())),
                new ByteSizeValue(Math.max(cis1.maxSize.getBytes(), cis2.maxSize.getBytes())),
                cis1.openCount + cis2.openCount,
                cis1.closeCount + cis2.closeCount,
                cis1.forwardSmallSeeks.add(cis2.forwardSmallSeeks),
                cis1.backwardSmallSeeks.add(cis2.backwardSmallSeeks),
                cis1.forwardLargeSeeks.add(cis2.forwardLargeSeeks),
                cis1.backwardLargeSeeks.add(cis2.backwardLargeSeeks),
                cis1.contiguousReads.add(cis2.contiguousReads),
                cis1.nonContiguousReads.add(cis2.nonContiguousReads),
                cis1.cachedBytesRead.add(cis2.cachedBytesRead),
                cis1.indexCacheBytesRead.add(cis2.indexCacheBytesRead),
                cis1.cachedBytesWritten.add(cis2.cachedBytesWritten),
                cis1.directBytesRead.add(cis2.directBytesRead),
                cis1.optimizedBytesRead.add(cis2.optimizedBytesRead),
                cis1.blobStoreBytesRequested.add(cis2.blobStoreBytesRequested),
                cis1.luceneBytesRead.add(cis2.luceneBytesRead),
                cis1.currentIndexCacheFills + cis2.currentIndexCacheFills
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(Version.V_7_12_0)) {
                // This API is currently only used internally for testing, so BWC breaking changes are OK.
                // We just throw an exception here to get a better error message in case this would be called
                // in a mixed version cluster
                throw new IllegalArgumentException("BWC breaking change for internal API");
            }
            out.writeString(fileExt);
            out.writeVLong(numFiles);
            if (out.getVersion().before(Version.V_7_13_0)) {
                out.writeVLong(totalSize.getBytes());
            } else {
                totalSize.writeTo(out);
                minSize.writeTo(out);
                maxSize.writeTo(out);
            }
            out.writeVLong(openCount);
            out.writeVLong(closeCount);

            forwardSmallSeeks.writeTo(out);
            backwardSmallSeeks.writeTo(out);
            forwardLargeSeeks.writeTo(out);
            backwardLargeSeeks.writeTo(out);
            contiguousReads.writeTo(out);
            nonContiguousReads.writeTo(out);
            cachedBytesRead.writeTo(out);
            indexCacheBytesRead.writeTo(out);
            cachedBytesWritten.writeTo(out);
            directBytesRead.writeTo(out);
            optimizedBytesRead.writeTo(out);
            blobStoreBytesRequested.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
                luceneBytesRead.writeTo(out);
            }
            out.writeVLong(currentIndexCacheFills);
        }

        public String getFileExt() {
            return fileExt;
        }

        public long getNumFiles() {
            return numFiles;
        }

        public ByteSizeValue getTotalSize() {
            return totalSize;
        }

        public ByteSizeValue getMinSize() {
            return minSize;
        }

        public ByteSizeValue getMaxSize() {
            return maxSize;
        }

        public ByteSizeValue getAverageSize() {
            final double average = (double) totalSize.getBytes() / (double) numFiles;
            return new ByteSizeValue(Math.round(average));
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

        public Counter getIndexCacheBytesRead() {
            return indexCacheBytesRead;
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

        public Counter getBlobStoreBytesRequested() {
            return blobStoreBytesRequested;
        }

        public Counter getLuceneBytesRead() {
            return luceneBytesRead;
        }

        public long getCurrentIndexCacheFills() {
            return currentIndexCacheFills;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("file_ext", getFileExt());
                builder.field("num_files", getNumFiles());
                builder.field("open_count", getOpenCount());
                builder.field("close_count", getCloseCount());
                {
                    builder.startObject("size");
                    builder.humanReadableField("total_in_bytes", "total", getTotalSize());
                    builder.humanReadableField("min_in_bytes", "min", getMinSize());
                    builder.humanReadableField("max_in_bytes", "max", getMaxSize());
                    builder.humanReadableField("average_in_bytes", "average", getAverageSize());
                    builder.endObject();
                }
                builder.field("contiguous_bytes_read", getContiguousReads(), params);
                builder.field("non_contiguous_bytes_read", getNonContiguousReads(), params);
                builder.field("cached_bytes_read", getCachedBytesRead(), params);
                builder.field("index_cache_bytes_read", getIndexCacheBytesRead(), params);
                builder.field("cached_bytes_written", getCachedBytesWritten(), params);
                builder.field("direct_bytes_read", getDirectBytesRead(), params);
                builder.field("optimized_bytes_read", getOptimizedBytesRead(), params);
                {
                    builder.startObject("forward_seeks");
                    builder.field("small", getForwardSmallSeeks(), params);
                    builder.field("large", getForwardLargeSeeks(), params);
                    builder.endObject();
                }
                {
                    builder.startObject("backward_seeks");
                    builder.field("small", getBackwardSmallSeeks(), params);
                    builder.field("large", getBackwardLargeSeeks(), params);
                    builder.endObject();
                }
                builder.field("blob_store_bytes_requested", getBlobStoreBytesRequested(), params);
                builder.field("lucene_bytes_read", getLuceneBytesRead(), params);
                builder.field("current_index_cache_fills", getCurrentIndexCacheFills());
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
            return numFiles == stats.numFiles
                && openCount == stats.openCount
                && closeCount == stats.closeCount
                && Objects.equals(fileExt, stats.fileExt)
                && Objects.equals(totalSize, stats.totalSize)
                && Objects.equals(minSize, stats.minSize)
                && Objects.equals(maxSize, stats.maxSize)
                && Objects.equals(forwardSmallSeeks, stats.forwardSmallSeeks)
                && Objects.equals(backwardSmallSeeks, stats.backwardSmallSeeks)
                && Objects.equals(forwardLargeSeeks, stats.forwardLargeSeeks)
                && Objects.equals(backwardLargeSeeks, stats.backwardLargeSeeks)
                && Objects.equals(contiguousReads, stats.contiguousReads)
                && Objects.equals(nonContiguousReads, stats.nonContiguousReads)
                && Objects.equals(cachedBytesRead, stats.cachedBytesRead)
                && Objects.equals(indexCacheBytesRead, stats.indexCacheBytesRead)
                && Objects.equals(cachedBytesWritten, stats.cachedBytesWritten)
                && Objects.equals(directBytesRead, stats.directBytesRead)
                && Objects.equals(optimizedBytesRead, stats.optimizedBytesRead)
                && Objects.equals(blobStoreBytesRequested, stats.blobStoreBytesRequested)
                && Objects.equals(luceneBytesRead, stats.luceneBytesRead)
                && currentIndexCacheFills == stats.currentIndexCacheFills;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                fileExt,
                numFiles,
                totalSize,
                minSize,
                maxSize,
                openCount,
                closeCount,
                forwardSmallSeeks,
                backwardSmallSeeks,
                forwardLargeSeeks,
                backwardLargeSeeks,
                contiguousReads,
                nonContiguousReads,
                cachedBytesRead,
                indexCacheBytesRead,
                cachedBytesWritten,
                directBytesRead,
                optimizedBytesRead,
                blobStoreBytesRequested,
                luceneBytesRead,
                currentIndexCacheFills
            );
        }
    }

    public static class Counter implements Writeable, ToXContentObject {

        protected final long count;
        protected final long total;
        protected final long min;
        protected final long max;

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

        public Counter add(Counter counter) {
            return new Counter(count + counter.count, total + counter.total, Math.min(min, counter.min), Math.max(max, counter.max));
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

        void innerToXContent(XContentBuilder builder, Params params) throws IOException {}

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
            return count == that.count && total == that.total && min == that.min && max == that.max;
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

        public TimedCounter add(TimedCounter counter) {
            return new TimedCounter(
                count + counter.count,
                total + counter.total,
                Math.min(min, counter.min),
                Math.max(max, counter.max),
                totalNanoseconds + counter.totalNanoseconds
            );
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
