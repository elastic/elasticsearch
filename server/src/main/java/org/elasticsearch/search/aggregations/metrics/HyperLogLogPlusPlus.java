/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.Arrays;

/**
 * Hyperloglog++ counter, implemented based on pseudo code from
 * <a href="http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf">this paper</a> and
 * <a href="https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen">its appendix</a>
 *
 * This implementation is different from the original implementation in that linear counting keeps, per bucket, a sorted prefix plus an
 * unsorted tail of appended values that is sorted, deduplicated and merged into the prefix lazily. This keeps inserts cheap (appends rather
 * than random hash probes) and iteration proportional to the number of values.
 *
 * Trying to understand what this class does without having read the paper is considered adventurous.
 *
 * The HyperLogLogPlusPlus contains two algorithms, one for linear counting and the HyperLogLog algorithm. Initially hashes added to the
 * data structure are processed using the linear counting until a threshold defined by the precision is reached where the data is replayed
 * to the HyperLogLog algorithm and then this is used.
 *
 * It supports storing several HyperLogLogPlusPlus structures which are identified by a bucket number.
 */
public final class HyperLogLogPlusPlus extends AbstractHyperLogLogPlusPlus {

    private static final float MAX_LOAD_FACTOR = 0.75f;

    public static final int DEFAULT_PRECISION = 14;

    private final BigArrays bigArrays;
    private final CircuitBreaker breaker;
    private LongArray hllBuckets;
    final HyperLogLog hll;
    private final LinearCounting lc;

    /**
     * Compute the required precision so that <code>count</code> distinct entries would be counted with linear counting.
     */
    public static int precisionFromThreshold(long count) {
        final long hashTableEntries = (long) Math.ceil(count / MAX_LOAD_FACTOR);
        int precision = PackedInts.bitsRequired(hashTableEntries * Integer.BYTES);
        precision = Math.max(precision, AbstractHyperLogLog.MIN_PRECISION);
        precision = Math.min(precision, AbstractHyperLogLog.MAX_PRECISION);
        return precision;
    }

    /**
     * Return the expected per-bucket memory usage for the given precision.
     */
    public static long memoryUsage(int precision) {
        return 1L << precision;
    }

    public HyperLogLogPlusPlus(int precision, BigArrays bigArrays, long initialBucketCount) {
        this(precision, bigArrays, breaker(bigArrays), initialBucketCount);
    }

    private static CircuitBreaker breaker(BigArrays bigArrays) {
        final CircuitBreakerService breakerService = bigArrays.breakerService();
        final CircuitBreaker breaker = breakerService != null ? breakerService.getBreaker(CircuitBreaker.REQUEST) : null;
        if (breaker != null) {
            return breaker;
        } else {
            return new NoopCircuitBreaker("hll");
        }
    }

    public HyperLogLogPlusPlus(int precision, BigArrays bigArrays, CircuitBreaker breaker, long initialBucketCount) {
        super(precision);
        // TODO if initialBucketCount is > 0 we allocate dense arrays for each one.
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        HyperLogLog hll = null;
        LinearCounting lc = null;
        LongArray hllBuckets = null;
        boolean success = false;
        try {
            hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
            lc = new LinearCounting(bigArrays, breaker, initialBucketCount, precision);
            hllBuckets = bigArrays.newLongArray(1);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(hll, lc, hllBuckets);
            }
        }
        this.hll = hll;
        this.lc = lc;
        this.hllBuckets = hllBuckets;
    }

    public long maxOrd() {
        return Math.max(hll.maxOrd(), lc.maxOrd());
    }

    /**
     * Returns the number of buckets currently in HyperLogLog mode (as opposed to LinearCounting mode).
     * Together with {@link #maxOrd()}, lets callers estimate serialized size.
     */
    public long hllBucketCount() {
        return hll.totalBuckets;
    }

    @Override
    public long cardinality(long bucketOrd) {
        upgradeIfAboveThreshold(bucketOrd);
        final long hllBucket = bucketOrd < hllBuckets.size() ? hllBuckets.get(bucketOrd) : 0;
        if (hllBucket > 0) {
            return hll.cardinality(hllBucket - 1);
        } else {
            return lc.cardinality(bucketOrd);
        }
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        upgradeIfAboveThreshold(bucketOrd);
        return bucketOrd < hllBuckets.size() && hllBuckets.get(bucketOrd) > 0;
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        return lc.values(bucketOrd);
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        return hll.getRunLens(hllBuckets.get(bucketOrd) - 1);
    }

    @Override
    public void collect(long bucket, long hash) {
        final long hllBucket = bucket < hllBuckets.size() ? hllBuckets.get(bucket) : 0;
        if (hllBucket > 0) {
            hll.collect(hllBucket - 1, hash);
        } else {
            final int newSize = lc.collect(bucket, hash);
            if (newSize > lc.threshold) {
                upgradeToHll(bucket);
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(hllBuckets, hll, lc);
    }

    long ramBytesUsed() {
        return hllBuckets.ramBytesUsed() + hll.ramBytesUsed() + lc.ramBytesUsed();
    }

    void addRunLen(long bucketOrd, int register, int runLen) {
        long hllBucket = bucketOrd < hllBuckets.size() ? hllBuckets.get(bucketOrd) - 1 : -1;
        if (hllBucket < 0) {
            hllBucket = upgradeToHll(bucketOrd);
        }
        hll.addRunLen(hllBucket, register, runLen);
    }

    /**
     * Linear counting defers deduplication so it can notice that a bucket holds more than the threshold of distinct values late.
     * Since sets only grow and collecting into HyperLogLog is idempotent, the end state only depends on the final number of
     * distinct values, so it is enough to settle this whenever the bucket is observed.
     */
    private void upgradeIfAboveThreshold(long bucketOrd) {
        if ((bucketOrd < hllBuckets.size() && hllBuckets.get(bucketOrd) > 0) == false && lc.size(bucketOrd) > lc.threshold) {
            upgradeToHll(bucketOrd);
        }
    }

    long upgradeToHll(long bucketOrd) {
        long hllBucket = bucketOrd < hllBuckets.size() ? hllBuckets.get(bucketOrd) : 0;
        if (hllBucket > 0) {
            return hllBucket - 1;
        }
        hllBucket = hll.newBucket();
        lc.copyToHll(bucketOrd, hll, hllBucket);
        hllBuckets = bigArrays.grow(hllBuckets, bucketOrd + 1);
        hllBuckets.set(bucketOrd, hllBucket + 1);
        return hllBucket;
    }

    /**
     * Sorts {@code values[0, n)} ascending and removes duplicates, unless they already are strictly ascending, which is the case
     * for values read from another linear counting.
     * @return the number of distinct values, which are moved to the start of the array
     */
    private static int sortDistinct(int[] values, int n) {
        boolean strictlyAscending = true;
        for (int i = 1; i < n; i++) {
            if (values[i - 1] >= values[i]) {
                strictlyAscending = false;
                break;
            }
        }
        if (strictlyAscending) {
            return n;
        }
        Arrays.sort(values, 0, n);
        int w = 1;
        for (int r = 1; r < n; r++) {
            if (values[r] != values[w - 1]) {
                values[w++] = values[r];
            }
        }
        return w;
    }

    public void combine(long bucket, BytesRef other) throws IOException {
        ByteArrayStreamInput in = new ByteArrayStreamInput(other.bytes);
        in.reset(other.bytes, other.offset, other.length);
        final int precision = in.readVInt();
        final boolean algorithm = in.readBoolean();
        if (algorithm == LINEAR_COUNTING && getAlgorithm(bucket) == LINEAR_COUNTING) {
            final int length = Math.toIntExact(in.readVLong());
            final long bytesUsed = (long) length * Integer.BYTES;
            breaker.addEstimateBytesAndMaybeBreak(bytesUsed, "merge linear counting");
            try {
                int[] values = new int[length];
                for (int i = 0; i < length; i++) {
                    values[i] = in.readInt();
                }
                final int n = sortDistinct(values, length);
                if (lc.addSorted(bucket, values, n) > lc.threshold) {
                    upgradeToHll(bucket);
                }
            } finally {
                breaker.addWithoutBreaking(-bytesUsed);
            }
            return;
        }
        // fallback
        in.reset(other.bytes, other.offset, other.length);
        try (AbstractHyperLogLogPlusPlus otherHll = readFrom(in, hll.bigArrays)) {
            merge(bucket, otherHll, 0);
        }
    }

    public void merge(long thisBucket, AbstractHyperLogLogPlusPlus other, long otherBucket) {
        if (precision() != other.precision()) {
            throw new IllegalArgumentException();
        }
        if (other.getAlgorithm(otherBucket) == LINEAR_COUNTING) {
            merge(thisBucket, other.getLinearCounting(otherBucket));
        } else {
            merge(thisBucket, other.getHyperLogLog(otherBucket));
        }
    }

    private void merge(long bucketOrd, AbstractLinearCounting.HashesIterator values) {
        long hllBucket = bucketOrd < hllBuckets.size() ? hllBuckets.get(bucketOrd) - 1 : -1;
        if (hllBucket < 0) {
            final int length = values.size();
            final long bytesUsed = (long) length * Integer.BYTES;
            breaker.addEstimateBytesAndMaybeBreak(bytesUsed, "merge linear counting");
            try {
                final int[] encoded = new int[length];
                for (int i = 0; i < length; i++) {
                    values.next();
                    encoded[i] = values.value();
                }
                final int n = sortDistinct(encoded, length);
                if (lc.addSorted(bucketOrd, encoded, n) > lc.threshold) {
                    upgradeToHll(bucketOrd);
                }
            } finally {
                breaker.addWithoutBreaking(-bytesUsed);
            }
            return;
        }
        while (values.next()) {
            hll.collectEncoded(hllBucket, values.value());
        }
    }

    private void merge(long bucketOrd, AbstractHyperLogLog.RunLenIterator runLens) {
        long hllBucket = bucketOrd < hllBuckets.size() ? hllBuckets.get(bucketOrd) - 1 : -1;
        if (hllBucket < 0) {
            hllBucket = upgradeToHll(bucketOrd);
        }
        for (int i = 0; i < hll.m; ++i) {
            runLens.next();
            hll.addRunLen(hllBucket, i, runLens.value());
        }
    }

    private static class HyperLogLog extends AbstractHyperLogLog implements Releasable {
        private final BigArrays bigArrays;
        // array for holding the runlens.
        private ByteArray runLens;
        private long totalBuckets = 0;

        HyperLogLog(BigArrays bigArrays, long initialBucketCount, int precision) {
            super(precision);
            this.runLens = bigArrays.newByteArray(initialBucketCount << precision);
            this.bigArrays = bigArrays;
        }

        public long maxOrd() {
            return runLens.size() >>> precision();
        }

        @Override
        protected void addRunLen(long bucketOrd, int register, int encoded) {
            final long bucketIndex = (bucketOrd << p) + register;
            runLens.set(bucketIndex, (byte) Math.max(encoded, runLens.get(bucketIndex)));
        }

        @Override
        protected RunLenIterator getRunLens(long bucketOrd) {
            return new HyperLogLogIterator(this, bucketOrd);
        }

        protected long newBucket() {
            long bucket = totalBuckets++;
            runLens = bigArrays.grow(runLens, totalBuckets << p);
            return bucket;
        }

        @Override
        public void close() {
            Releasables.close(runLens);
        }

        long ramBytesUsed() {
            return runLens.ramBytesUsed();
        }
    }

    private static class HyperLogLogIterator implements AbstractHyperLogLog.RunLenIterator {

        private final HyperLogLog hll;
        int pos;
        final long start;
        private byte value;

        HyperLogLogIterator(HyperLogLog hll, long bucket) {
            this.hll = hll;
            start = bucket << hll.p;
        }

        @Override
        public boolean next() {
            if (pos < hll.m) {
                value = hll.runLens.get(start + pos);
                pos++;
                return true;
            }
            return false;
        }

        @Override
        public byte value() {
            return value;
        }
    }

    /**
     * A single bucket's linear counting set: an int array laid out as {@code [sorted+deduplicated prefix | unsorted tail]}.
     * Inserts append to the tail; the tail is sorted, deduplicated and merged into the prefix ("compacted") only when the array
     * is full or when the exact content is needed. Values are never removed, so the set only grows.
     */
    private static final class LinearCountingCell {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LinearCountingCell.class);
        /** Number of used slots in {@link #values}; includes the unsorted tail, which may contain duplicates. */
        private int len;
        /** Length of the sorted, duplicate free prefix. Always a lower bound of the number of distinct values. */
        private int sortedLen;
        private int[] values;

        LinearCountingCell(int capacity) {
            this.values = new int[capacity];
        }

        static long bytesUsed(int length) {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * length
            );
        }

        int capacity() {
            return values.length;
        }
    }

    /** Iterates the distinct values of a compacted cell, in ascending order. */
    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {
        private final LinearCountingCell cell;
        private int index;

        LinearCountingIterator(LinearCountingCell cell) {
            assert cell.len == cell.sortedLen : "iterating a cell that was not compacted";
            this.cell = cell;
            this.index = 0;
        }

        @Override
        public int size() {
            return cell.len;
        }

        @Override
        public boolean next() {
            if (index < cell.len) {
                index++;
                return true;
            }
            return false;
        }

        @Override
        public int value() {
            return cell.values[index - 1];
        }
    }

    /**
     * Linear counting where each bucket keeps its values in a {@link LinearCountingCell sorted buffer} rather than a hash table.
     * Compared to hashing this avoids random memory access on insert and iterates in proportion to the number of values. Two cheap
     * shortcuts drop inserts of values that are provably already present: a check against the last element of the buffer and a
     * shared direct-mapped filter of recently seen {@code (bucket, value)} pairs. Both are optimizations only: a miss defers
     * deduplication to compaction.
     *
     * Because deduplication is deferred, a size returned by {@link #addEncoded} is only guaranteed to be above the threshold when
     * the bucket really holds more distinct values. A bucket that is above the threshold is detected at the latest when its buffer
     * is full, or when it is read, see {@link HyperLogLogPlusPlus#upgradeIfAboveThreshold}.
     *
     * This class is not thread safe, and reading values or sizes compacts (mutates) the buffers.
     */
    private static class LinearCounting extends AbstractLinearCounting implements Releasable {
        private static final int INITIAL_CELL_CAPACITY = 8;
        /** Number of slots of the recent-values filter; a power of two. */
        private static final int FILTER_BITS = 14;
        /** Compaction doubles the buffer when more than this fraction of it holds distinct values. */
        private static final float GROW_ABOVE = 0.5f;

        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long bytesUsed;
        private final int threshold;
        private ObjectArray<LinearCountingCell> cells;
        private final int capacity;
        /** Scratch space for merging a tail into a prefix, shared by all buckets. */
        private int[] scratch = new int[0];
        /**
         * Direct-mapped cache of recently inserted {@code (bucketOrd + 1) << 32 | value} keys, shared by all buckets. A hit implies the
         * value is in the bucket, which holds because sets only grow. Allocated lazily as small aggregations never need it.
         */
        private long[] filter;

        LinearCounting(BigArrays bigArrays, CircuitBreaker breaker, long initialBucketCount, int precision) {
            super(precision);
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.capacity = (1 << precision) / 4;
            this.threshold = (int) (capacity * MAX_LOAD_FACTOR);
            this.cells = bigArrays.newObjectArray(initialBucketCount);
        }

        private int initialCellSize(long bucket) {
            // Pre-allocate full capacity for the first few buckets to bypass the cost of multiple resizes.
            // Optimized for ungrouped aggregations or those with few groups but high cardinality.
            if (bucket < 10) {
                return capacity;
            } else {
                return Math.min(capacity, INITIAL_CELL_CAPACITY);
            }
        }

        private LinearCountingCell newCell(int capacity) {
            long bytes = LinearCountingCell.bytesUsed(capacity);
            breaker.addEstimateBytesAndMaybeBreak(bytes, "linear counting cell");
            bytesUsed += bytes;
            return new LinearCountingCell(capacity);
        }

        private void closeCell(LinearCountingCell cell) {
            long bytes = LinearCountingCell.bytesUsed(cell.values.length);
            breaker.addWithoutBreaking(-bytes);
            bytesUsed -= bytes;
        }

        private void resize(LinearCountingCell cell, int newCapacity) {
            long newBytes = LinearCountingCell.bytesUsed(newCapacity);
            long oldBytes = LinearCountingCell.bytesUsed(cell.values.length);
            breaker.addEstimateBytesAndMaybeBreak(newBytes - oldBytes, "linear counting cell");
            bytesUsed += newBytes - oldBytes;
            cell.values = Arrays.copyOf(cell.values, newCapacity);
        }

        private void ensureScratch(int length) {
            if (scratch.length < length) {
                long newBytes = RamUsageEstimator.alignObjectSize(
                    (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * length
                );
                long oldBytes = RamUsageEstimator.sizeOf(scratch);
                breaker.addEstimateBytesAndMaybeBreak(newBytes - oldBytes, "linear counting scratch");
                bytesUsed += newBytes - oldBytes;
                scratch = new int[length];
            }
        }

        /** Returns true if {@code (bucketOrd, encoded)} was recently inserted, otherwise remembers it and returns false. */
        private boolean filterContainsOrAdd(long bucketOrd, int encoded) {
            final long key = ((bucketOrd + 1) << 32) | (encoded & 0xFFFFFFFFL);
            final int slot = (int) ((key * 0x9E3779B97F4A7C15L) >>> (64 - FILTER_BITS));
            if (filter[slot] == key) {
                return true;
            }
            filter[slot] = key;
            return false;
        }

        private void allocateFilter() {
            long bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * (1 << FILTER_BITS);
            breaker.addEstimateBytesAndMaybeBreak(bytes, "linear counting filter");
            bytesUsed += bytes;
            filter = new long[1 << FILTER_BITS];
        }

        /**
         * Sorts and deduplicates the tail of the cell and merges it into the sorted prefix, leaving {@code len == sortedLen}
         * equal to the number of distinct values.
         */
        private void compact(LinearCountingCell cell) {
            final int[] a = cell.values;
            final int sorted = cell.sortedLen;
            if (sorted == cell.len) {
                return;
            }
            Arrays.sort(a, sorted, cell.len);
            // Dedup the tail in place. This also drops tail values equal to the last element of the prefix.
            int w = sorted;
            for (int r = sorted; r < cell.len; r++) {
                final int v = a[r];
                if (w == 0 || a[w - 1] != v) {
                    a[w++] = v;
                }
            }
            if (w == sorted || sorted == 0 || a[sorted - 1] < a[sorted]) {
                // The (remaining) tail is entirely above the prefix: it is already in order.
                cell.len = cell.sortedLen = w;
                return;
            }
            ensureScratch(w);
            final int k = mergeDistinct(a, sorted, a, sorted, w, scratch);
            System.arraycopy(scratch, 0, a, 0, k);
            cell.len = cell.sortedLen = k;
        }

        /**
         * Merges the strictly ascending runs {@code x[0, xLen)} and {@code y[yFrom, yTo)} into {@code out}, writing each value once.
         * @return the number of values written
         */
        private static int mergeDistinct(int[] x, int xLen, int[] y, int yFrom, int yTo, int[] out) {
            int i = 0;
            int j = yFrom;
            int k = 0;
            while (i < xLen && j < yTo) {
                final int a = x[i];
                final int b = y[j];
                if (a < b) {
                    out[k++] = a;
                    i++;
                } else if (a > b) {
                    out[k++] = b;
                    j++;
                } else {
                    out[k++] = a;
                    i++;
                    j++;
                }
            }
            while (i < xLen) {
                out[k++] = x[i++];
            }
            while (j < yTo) {
                out[k++] = y[j++];
            }
            return k;
        }

        /**
         * Adds values that are strictly ascending (sorted and distinct) with a single linear merge into the bucket's sorted prefix,
         * rather than one insert per value. This is why merging buckets is cheaper than hashing each value.
         * @return the exact number of distinct values in the bucket afterwards
         */
        int addSorted(long bucketOrd, int[] sortedValues, int n) {
            if (n == 0) {
                return size(bucketOrd);
            }
            LinearCountingCell cell;
            if (bucketOrd >= cells.size()) {
                cells = bigArrays.grow(cells, bucketOrd + 1);
                cell = null;
            } else {
                cell = cells.get(bucketOrd);
            }
            if (cell == null) {
                cell = newCell(initialCellSize(bucketOrd));
                cells.set(bucketOrd, cell);
            }
            compact(cell);
            ensureScratch(cell.len + n);
            final int k = mergeDistinct(cell.values, cell.len, sortedValues, 0, n, scratch);
            if (k > cell.capacity()) {
                resize(cell, k);
            }
            System.arraycopy(scratch, 0, cell.values, 0, k);
            cell.len = cell.sortedLen = k;
            return k;
        }

        /** Frees at least one slot of a full cell by compacting it, growing it if it is mostly distinct values. */
        private void makeRoom(LinearCountingCell cell) {
            if (filter == null) {
                allocateFilter();
            }
            compact(cell);
            final int cap = cell.capacity();
            if (cell.len == cap) {
                // Everything is distinct and above the threshold: the caller upgrades, but needs room for the value being added.
                resize(cell, cap * 2);
            } else if (cell.len > (int) (cap * GROW_ABOVE) && cap < capacity) {
                resize(cell, Math.min(cap * 2, capacity));
            }
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            assert encoded != 0;
            LinearCountingCell cell;
            if (bucketOrd >= cells.size()) {
                cells = bigArrays.grow(cells, bucketOrd + 1);
                cell = null;
            } else {
                cell = cells.get(bucketOrd);
            }
            if (cell == null) {
                cell = newCell(initialCellSize(bucketOrd));
                cells.set(bucketOrd, cell);
            } else {
                if (filter != null && filterContainsOrAdd(bucketOrd, encoded)) {
                    return Math.max(cell.sortedLen, Math.min(cell.len, threshold));
                }
                if (cell.len != 0 && cell.values[cell.len - 1] == encoded) {
                    return Math.max(cell.sortedLen, Math.min(cell.len, threshold));
                }
                if (cell.len == cell.values.length) {
                    makeRoom(cell);
                }
            }
            cell.values[cell.len++] = encoded;
            // len is an upper bound on the distinct count and sortedLen a lower bound. Only report exceeding the threshold when
            // that is certain; otherwise the true size is not worth compacting for.
            return Math.max(cell.sortedLen, Math.min(cell.len, threshold));
        }

        @Override
        protected int size(long bucketOrd) {
            final var cell = bucketOrd < cells.size() ? cells.get(bucketOrd) : null;
            if (cell == null) {
                return 0;
            }
            compact(cell);
            return cell.len;
        }

        private HashesIterator values(long bucketOrd) {
            LinearCountingCell cell = bucketOrd < cells.size() ? cells.get(bucketOrd) : null;
            if (cell == null) {
                return AbstractLinearCounting.HashesIterator.EMPTY;
            } else {
                compact(cell);
                return new LinearCountingIterator(cell);
            }
        }

        void copyToHll(long bucketOrd, HyperLogLog hll, long hllBucket) {
            final LinearCountingCell cell = bucketOrd < cells.size() ? cells.get(bucketOrd) : null;
            if (cell == null) {
                return;
            }
            // Duplicates in the tail are harmless as collecting is idempotent.
            for (int i = 0; i < cell.len; i++) {
                hll.collectEncoded(hllBucket, cell.values[i]);
            }
            closeCell(cell);
            cells.set(bucketOrd, null);
        }

        long maxOrd() {
            return cells.size();
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-bytesUsed);
            Releasables.close(cells);
        }

        long ramBytesUsed() {
            return cells.ramBytesUsed() + bytesUsed;
        }
    }
}
