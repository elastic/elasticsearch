/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory.CacheBufferedIndexInput;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;

/**
 * {@link IndexInputStats} records stats for a given {@link CacheBufferedIndexInput}.
 */
public class IndexInputStats {

    /* A threshold beyond which an index input seeking is counted as "large" */
    static final ByteSizeValue SEEKING_THRESHOLD = new ByteSizeValue(8, ByteSizeUnit.MB);

    private final long fileLength;

    private final LongAdder opened = new LongAdder();
    private final LongAdder inner = new LongAdder();
    private final LongAdder closed = new LongAdder();

    private final Counter forwardSmallSeeks = new Counter();
    private final Counter backwardSmallSeeks = new Counter();

    private final Counter forwardLargeSeeks = new Counter();
    private final Counter backwardLargeSeeks = new Counter();

    private final Counter contiguousReads = new Counter();
    private final Counter nonContiguousReads = new Counter();

    private final Counter directBytesRead = new Counter();
    private final LongAdder directReadNanoseconds = new LongAdder();

    private final Counter cachedBytesRead = new Counter();
    private final Counter cachedBytesWritten = new Counter();
    private final LongAdder cachedBytesWrittenNanoseconds = new LongAdder();

    /**
     * Linear regression model for the time taken to fetch data from the blob store: time (ns) == bytes * slope + intercept
     */
    private final LinearModelAccumulator fetchedBytesLinearModel = new LinearModelAccumulator();

    public IndexInputStats(long fileLength) {
        this.fileLength = fileLength;
    }

    public void incrementOpenCount() {
        opened.increment();
    }

    public void incrementInnerOpenCount() {
        inner.increment();
    }

    public void incrementCloseCount() {
        closed.increment();
    }

    public void addCachedBytesRead(int bytesRead) {
        cachedBytesRead.add(bytesRead);
    }

    public void addCachedBytesWritten(int bytesWritten, long nanoseconds) {
        cachedBytesWritten.add(bytesWritten);
        cachedBytesWrittenNanoseconds.add(nanoseconds);
        fetchedBytesLinearModel.addPoint(bytesWritten, nanoseconds);
    }

    public void addDirectBytesRead(int bytesRead, long nanoseconds) {
        directBytesRead.add(bytesRead);
        directReadNanoseconds.add(nanoseconds);
        fetchedBytesLinearModel.addPoint(bytesRead, nanoseconds);
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
                backwardLargeSeeks.add(delta);
            } else {
                backwardSmallSeeks.add(delta);
            }
        }
    }

    long getFileLength() {
        return fileLength;
    }

    LongAdder getOpened() {
        return opened;
    }

    LongAdder getInnerOpened() {
        return inner;
    }

    LongAdder getClosed() {
        return closed;
    }

    Counter getForwardSmallSeeks() {
        return forwardSmallSeeks;
    }

    Counter getBackwardSmallSeeks() {
        return backwardSmallSeeks;
    }

    Counter getForwardLargeSeeks() {
        return forwardLargeSeeks;
    }

    Counter getBackwardLargeSeeks() {
        return backwardLargeSeeks;
    }

    Counter getContiguousReads() {
        return contiguousReads;
    }

    Counter getNonContiguousReads() {
        return nonContiguousReads;
    }

    Counter getDirectBytesRead() {
        return directBytesRead;
    }

    LongAdder getDirectReadNanoseconds() {
        return directReadNanoseconds;
    }

    Counter getCachedBytesRead() {
        return cachedBytesRead;
    }

    Counter getCachedBytesWritten() {
        return cachedBytesWritten;
    }

    LongAdder getCachedBytesWrittenNanoseconds() {
        return cachedBytesWrittenNanoseconds;
    }

    /**
     * Returns a linear regression model for predicting the speed of fetching bytes of the form time (ns) == bytes * slope + intercept
     */
    LinearModel getFetchedBytesLinearModel() {
        return fetchedBytesLinearModel.getModel();
    }

    @SuppressForbidden(reason = "Handles Long.MIN_VALUE before using Math.abs()")
    boolean isLargeSeek(long delta) {
        return delta != Long.MIN_VALUE && Math.abs(delta) > SEEKING_THRESHOLD.getBytes();
    }

    static class Counter {

        private final LongAdder count = new LongAdder();
        private final LongAdder total = new LongAdder();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

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
    }

    static class LinearModelAccumulator {
        private final LongAdder s1 = new LongAdder();
        private final DoubleAdder sx = new DoubleAdder();
        private final DoubleAdder sy = new DoubleAdder();
        private final DoubleAdder sxx = new DoubleAdder();
        private final DoubleAdder sxy = new DoubleAdder();

        /**
         * @noinspection SuspiciousNameCombination since the parameter to {@link DoubleAdder#add} is called {@code x}
         */
        void addPoint(final double x, final double y) {
            s1.increment();
            sx.add(x);
            sy.add(y);
            sxx.add(x * x);
            sxy.add(x * y);
        }

        LinearModel getModel() {
            final double n = s1.sum();
            final double x = sx.sum();
            final double y = sy.sum();
            final double xx = sxx.sum();
            final double xy = sxy.sum();
            final double slope = (n * xy - x * y) / (n * xx - x * x); // divide by zero results in Infinity or NaN, no exception thrown
            return new LinearModel(slope, (y - slope * x) / n);
        }
    }

    public static class LinearModel {
        final double slope;
        final double intercept;

        LinearModel(double slope, double intercept) {
            this.slope = slope;
            this.intercept = intercept;
        }
    }
}
