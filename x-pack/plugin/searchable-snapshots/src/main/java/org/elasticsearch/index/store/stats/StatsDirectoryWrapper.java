/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.stats;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

/**
 * Directory wrapper that records various information about how {@link IndexInput} are accessed.
 */
public class StatsDirectoryWrapper extends FilterDirectory {

    private final Map<String, LiveStats> records = ConcurrentCollections.newConcurrentMap();

    protected StatsDirectoryWrapper(final Directory in) {
        super(in);
    }

    public Map<String, IndexInputStats> getStats() {
        return records.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toIndexInputStats()));
    }

    public IndexInputStats getStatsOrNull(final String name) {
        final LiveStats stats = records.get(name);
        if (stats != null) {
            return stats.toIndexInputStats();
        }
        return null;
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        final LiveStats stats = records.computeIfAbsent(name, n -> new LiveStats()).incrementOpenCount();
        return new StatsIndexInputWrapper(super.openInput(name, context), stats);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            records.clear();
        }
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
        private final AtomicLong lastReadPosition;
        /**
         * Indicate if the IndexInput is a clone (or slice)
         **/
        private boolean isClone;

        private StatsIndexInputWrapper(final IndexInput indexInput, final LiveStats stats) {
            super("StatsIndexInputWrapper(" + indexInput + ")");
            this.input = Objects.requireNonNull(indexInput);
            this.stats = Objects.requireNonNull(stats);
            this.lastReadPosition = new AtomicLong();
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
            if (readByte != -1) {
                stats.incrementBytesRead(lastReadPosition.getAndUpdate(l -> getFilePointer()) == filePointer);
            }
            return readByte;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            final long filePointer = getFilePointer();
            input.readBytes(b, offset, len);
            stats.incrementBytesRead(lastReadPosition.getAndUpdate(l -> getFilePointer()) == filePointer, len);
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

        final LongAdder opened = new LongAdder();
        final LongAdder closed = new LongAdder();
        final LongAdder sliced = new LongAdder();
        final LongAdder cloned = new LongAdder();

        final LiveCounter forwardSeeks = new LiveCounter();
        final LiveCounter backwardSeeks = new LiveCounter();

        final LiveCounter contiguousReads = new LiveCounter();
        final LiveCounter nonContiguousReads = new LiveCounter();

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
            if (n >= 0) {
                forwardSeeks.add(n);
            } else {
                backwardSeeks.add(n != Long.MIN_VALUE ? -n : 0L);
            }
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
                opened.sum(),
                closed.sum(),
                sliced.sum(),
                cloned.sum(),
                forwardSeeks.toCounter(),
                backwardSeeks.toCounter(),
                contiguousReads.toCounter(),
                nonContiguousReads.toCounter());
        }
    }

    private static class LiveCounter {

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
