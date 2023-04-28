/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.searchablesnapshots.store.input.CachedBlobContainerIndexInput;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * {@link IndexInputStats} records stats for a given {@link CachedBlobContainerIndexInput}.
 */
public class IndexInputStats {

    /* A threshold beyond which an index input seeking is counted as "large" */
    static final ByteSizeValue SEEKING_THRESHOLD = new ByteSizeValue(8, ByteSizeUnit.MB);

    private final long numFiles;
    private final long totalSize;
    private final long minSize;
    private final long maxSize;

    private final long seekingThreshold;
    private final LongSupplier currentTimeNanos;

    private final LongAdder opened = new LongAdder();
    private final LongAdder closed = new LongAdder();

    private final Counter forwardSmallSeeks = new Counter();
    private final Counter backwardSmallSeeks = new Counter();

    private final Counter forwardLargeSeeks = new Counter();
    private final Counter backwardLargeSeeks = new Counter();

    private final Counter contiguousReads = new Counter();
    private final Counter nonContiguousReads = new Counter();

    private final TimedCounter directBytesRead = new TimedCounter();
    private final TimedCounter optimizedBytesRead = new TimedCounter();

    private final Counter cachedBytesRead = new Counter();
    private final Counter indexCacheBytesRead = new Counter();
    private final TimedCounter cachedBytesWritten = new TimedCounter();

    private final Counter blobStoreBytesRequested = new Counter();
    private final AtomicLong currentIndexCacheFills = new AtomicLong();

    private final Counter luceneBytesRead = new Counter();

    public IndexInputStats(long numFiles, long totalSize, long minSize, long maxSize, LongSupplier currentTimeNanos) {
        this(numFiles, totalSize, minSize, maxSize, SEEKING_THRESHOLD.getBytes(), currentTimeNanos);
    }

    public IndexInputStats(
        long numFiles,
        long totalSize,
        long minSize,
        long maxSize,
        long seekingThreshold,
        LongSupplier currentTimeNanos
    ) {
        this.numFiles = numFiles;
        this.totalSize = totalSize;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.seekingThreshold = seekingThreshold;
        this.currentTimeNanos = currentTimeNanos;
    }

    /**
     * @return the current time in nanoseconds that should be used to measure statistics.
     */
    public long currentTimeNanos() {
        return currentTimeNanos.getAsLong();
    }

    public void incrementOpenCount() {
        opened.increment();
    }

    public void incrementCloseCount() {
        closed.increment();
    }

    public void addCachedBytesRead(int bytesRead) {
        cachedBytesRead.add(bytesRead);
    }

    public void addIndexCacheBytesRead(int bytesRead) {
        indexCacheBytesRead.add(bytesRead);
    }

    public void addCachedBytesWritten(long bytesWritten, long nanoseconds) {
        cachedBytesWritten.add(bytesWritten, nanoseconds);
    }

    public void addDirectBytesRead(int bytesRead, long nanoseconds) {
        directBytesRead.add(bytesRead, nanoseconds);
    }

    public void addOptimizedBytesRead(int bytesRead, long nanoseconds) {
        optimizedBytesRead.add(bytesRead, nanoseconds);
    }

    public void incrementBytesRead(long previousPosition, long currentPosition, int bytesRead) {
        LongConsumer incBytesRead = (previousPosition == currentPosition) ? contiguousReads::add : nonContiguousReads::add;
        incBytesRead.accept(bytesRead);
    }

    public void incrementSeeks(long currentPosition, long newPosition) {
        final long delta = newPosition - currentPosition;
        if (delta == 0L) {
            return;
        }
        final boolean isLarge = isLargeSeek(delta);
        if (delta > 0) {
            if (isLarge) {
                forwardLargeSeeks.add(delta);
            } else {
                forwardSmallSeeks.add(delta);
            }
        } else {
            if (isLarge) {
                backwardLargeSeeks.add(-delta);
            } else {
                backwardSmallSeeks.add(-delta);
            }
        }
    }

    public void addBlobStoreBytesRequested(long bytesRequested) {
        blobStoreBytesRequested.add(bytesRequested);
    }

    public Releasable addIndexCacheFill() {
        final long openValue = currentIndexCacheFills.incrementAndGet();
        assert openValue > 0 : openValue;
        return () -> {
            final long closeValue = currentIndexCacheFills.decrementAndGet();
            assert closeValue >= 0 : closeValue;
        };
    }

    public void addLuceneBytesRead(int bytesRead) {
        luceneBytesRead.add(bytesRead);
    }

    public long getNumFiles() {
        return numFiles;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getMinSize() {
        return minSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public LongAdder getOpened() {
        return opened;
    }

    public LongAdder getClosed() {
        return closed;
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

    public TimedCounter getDirectBytesRead() {
        return directBytesRead;
    }

    public TimedCounter getOptimizedBytesRead() {
        return optimizedBytesRead;
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

    public Counter getBlobStoreBytesRequested() {
        return blobStoreBytesRequested;
    }

    public Counter getLuceneBytesRead() {
        return luceneBytesRead;
    }

    @SuppressForbidden(reason = "Handles Long.MIN_VALUE before using Math.abs()")
    public boolean isLargeSeek(long delta) {
        return delta != Long.MIN_VALUE && Math.abs(delta) > seekingThreshold;
    }

    public long getCurrentIndexCacheFills() {
        return currentIndexCacheFills.get();
    }

    public static class Counter {

        private final LongAdder count = new LongAdder();
        private final LongAdder total = new LongAdder();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        void add(final long value) {
            count.increment();
            total.add(value);
            min.accumulateAndGet(value, Math::min);
            max.accumulateAndGet(value, Math::max);
        }

        public long count() {
            return count.sum();
        }

        public long total() {
            return total.sum();
        }

        public long min() {
            final long value = min.get();
            if (value == Long.MAX_VALUE) {
                return 0L;
            }
            return value;
        }

        public long max() {
            final long value = max.get();
            if (value == Long.MIN_VALUE) {
                return 0L;
            }
            return value;
        }
    }

    public static class TimedCounter extends Counter {

        private final LongAdder totalNanoseconds = new LongAdder();

        void add(final long value, final long nanoseconds) {
            super.add(value);
            totalNanoseconds.add(nanoseconds);
        }

        public long totalNanoseconds() {
            return totalNanoseconds.sum();
        }
    }

}
