/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.stats.IndexInputStats;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Directory} that exposes files from a snapshot as a Lucene directory. Because snapshot are immutable this
 * implementation does not allow modification of the directory files and only supports {@link #listAll()}, {@link #fileLength(String)} and
 * {@link #openInput(String, IOContext)} methods.
 *
 * To create a {@link SearchableSnapshotDirectory} both the list of the snapshot files and a {@link BlobContainer} to read these files must
 * be provided. The definition of the snapshot files are provided using a {@link BlobStoreIndexShardSnapshot} object which contains the name
 * of the snapshot and all the files it contains along with their metadata. Because there is no one-to-one relationship between the original
 * shard files and what it stored in the snapshot the {@link BlobStoreIndexShardSnapshot} is used to map a physical file name as expected by
 * Lucene with the one (or the ones) corresponding blob(s) in the snapshot.
 */
public class SearchableSnapshotDirectory extends BaseDirectory {

    private final BlobStoreIndexShardSnapshot snapshot;
    private final BlobContainer blobContainer;
    private final Map<String, LiveStats> stats;

    public SearchableSnapshotDirectory(final BlobStoreIndexShardSnapshot snapshot, final BlobContainer blobContainer) {
        super(new SingleInstanceLockFactory());
        this.snapshot = Objects.requireNonNull(snapshot);
        this.blobContainer = Objects.requireNonNull(blobContainer);
        this.stats = ConcurrentCollections.newConcurrentMap();
    }

    public Map<String, IndexInputStats> getStats() {
        return stats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toIndexInputStats()));
    }

    public IndexInputStats getStatsOrNull(final String name) {
        final LiveStats stats = this.stats.get(name);
        if (stats != null) {
            return stats.toIndexInputStats();
        }
        return null;
    }

    private FileInfo fileInfo(final String name) throws FileNotFoundException {
        return snapshot.indexFiles().stream()
            .filter(fileInfo -> fileInfo.physicalName().equals(name))
            .findFirst()
            .orElseThrow(() -> new FileNotFoundException(name));
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return snapshot.indexFiles().stream()
            .map(FileInfo::physicalName)
            .sorted(String::compareTo)
            .toArray(String[]::new);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        ensureOpen();
        return fileInfo(name).length();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        final FileInfo fileInfo = fileInfo(name);
        final IndexInput input = new SearchableSnapshotIndexInput(blobContainer, fileInfo);

        final LiveStats stats = this.stats.computeIfAbsent(name, n -> new LiveStats(fileInfo.length())).incrementOpenCount();
        return new StatsIndexInputWrapper(input, stats);
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + snapshot.snapshot() + " lockFactory=" + lockFactory;
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        return new UnsupportedOperationException("Searchable snapshot directory does not support this operation");
    }

    /**
     * Wraps an {@link IndexInput} to record stats about how it is accessed.
     */
    private static class StatsIndexInputWrapper extends IndexInput {

        /**
         * The IndexInput to delegate operations to
         **/
        private final IndexInput input;
        /**
         * The statistics to update on IndexInput operations
         **/
        private final LiveStats stats;
        /**
         * The last read position is kept around in order to detect (non)contiguous reads
         **/
        private long lastReadPosition;
        /**
         * Indicate if the IndexInput is a clone (or slice)
         **/
        private boolean isClone;

        private StatsIndexInputWrapper(final IndexInput indexInput, final LiveStats stats) {
            super("StatsIndexInputWrapper(" + indexInput + ")");
            this.input = Objects.requireNonNull(indexInput);
            this.stats = Objects.requireNonNull(stats);
        }

        @Override
        public long getFilePointer() {
            return input.getFilePointer();
        }

        @Override
        public long length() {
            return input.length();
        }

        @Override
        public void close() throws IOException {
            try {
                input.close();
            } finally {
                if (isClone == false) {
                    stats.incrementCloseCount();
                }
            }
        }

        @Override
        public byte readByte() throws IOException {
            final long filePointer = getFilePointer();
            final byte readByte = input.readByte();
            stats.incrementBytesRead(lastReadPosition == filePointer);
            lastReadPosition = getFilePointer();
            return readByte;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            final long filePointer = getFilePointer();
            input.readBytes(b, offset, len);
            stats.incrementBytesRead(lastReadPosition == filePointer, len);
            lastReadPosition = getFilePointer();
        }

        @Override
        public void seek(final long pos) throws IOException {
            final long filePointer = getFilePointer();
            input.seek(pos);
            stats.incrementSeekCount(pos - filePointer);
        }

        @Override
        public IndexInput clone() {
            final StatsIndexInputWrapper clone = new StatsIndexInputWrapper(input.clone(), stats.incrementCloneCount());
            assert this.stats == clone.stats : "same live stats object should be propagated to clones";
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDesc, long off, long len) throws IOException {
            final StatsIndexInputWrapper slice = new StatsIndexInputWrapper(input.slice(sliceDesc, off, len), stats.incrementSliceCount());
            assert this.stats == slice.stats : "same live stats object should be propagated to slices";
            slice.isClone = true;
            return slice;
        }
    }

    /**
     * {@link LiveStats} records stats for a given {@link IndexInput} and all its clones and slices.
     */
    private static class LiveStats {

        final long length;

        final LongAdder opened = new LongAdder();
        final LongAdder closed = new LongAdder();
        final LongAdder sliced = new LongAdder();
        final LongAdder cloned = new LongAdder();

        final LiveCounter forwardSmallSeeks = new LiveCounter();
        final LiveCounter backwardSmallSeeks = new LiveCounter();

        final LiveCounter forwardLargeSeeks = new LiveCounter();
        final LiveCounter backwardLargeSeeks = new LiveCounter();

        final LiveCounter contiguousReads = new LiveCounter();
        final LiveCounter nonContiguousReads = new LiveCounter();

        LiveStats(long length) {
            this.length = length;
        }

        LiveStats incrementOpenCount() {
            opened.increment();
            return this;
        }

        LiveStats incrementCloseCount() {
            closed.increment();
            return this;
        }

        LiveStats incrementSliceCount() {
            sliced.increment();
            return this;
        }

        LiveStats incrementCloneCount() {
            cloned.increment();
            return this;
        }

        void incrementSeekCount(final long n) {
            LongConsumer incSeekCount;
            if (n >= 0) {
                incSeekCount = n < (length * 0.25d) ? forwardSmallSeeks::add : forwardLargeSeeks::add;
            } else {
                incSeekCount = n > -(length * 0.25d) ? backwardSmallSeeks::add : backwardLargeSeeks::add;
            }
            incSeekCount.accept(n);
        }

        void incrementBytesRead(final boolean contiguous) {
            incrementBytesRead(contiguous, 1L);
        }

        void incrementBytesRead(final boolean contiguous, final long length) {
            LongConsumer incBytesRead = contiguous ? contiguousReads::add : nonContiguousReads::add;
            incBytesRead.accept(length);
        }

        IndexInputStats toIndexInputStats() {
            return new IndexInputStats(
                length,
                opened.sum(),
                closed.sum(),
                sliced.sum(),
                cloned.sum(),
                forwardSmallSeeks.toCounter(),
                backwardSmallSeeks.toCounter(),
                forwardLargeSeeks.toCounter(),
                backwardLargeSeeks.toCounter(),
                contiguousReads.toCounter(),
                nonContiguousReads.toCounter());
        }
    }

    static class LiveCounter {

        final LongAdder count = new LongAdder();
        final LongAdder total = new LongAdder();
        final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        void add(final long value) {
            count.increment();
            total.add(value);
            min.updateAndGet(prev -> Math.min(prev, value));
            max.updateAndGet(prev -> Math.max(prev, value));
        }

        long count() {
            return count.sum();
        }

        long total() {
            return total.sum();
        }

        long min() {
            final long value = min.get();
            if (value == Long.MAX_VALUE) {
                return 0L;
            }
            return value;
        }

        long max() {
            final long value = max.get();
            if (value == Long.MIN_VALUE) {
                return 0L;
            }
            return value;
        }

        IndexInputStats.Counter toCounter() {
            return new IndexInputStats.Counter(count(), total(), min(), max());
        }
    }
}
