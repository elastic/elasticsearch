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
import org.elasticsearch.common.util.PageCacheRecycler;
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
 * This implementation is different from the original implementation in that it uses a hash table instead of a sorted list for linear
 * counting. Although this requires more space and makes hyperloglog (which is less accurate) used sooner, this is also considerably faster.
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

    enum Mode {
        EMPTY,      // 00
        LC_SINGLE,  // 01
        LC_HASH,    // 10
        HLL;        // 11

        static final Mode[] VALUES = values();
    }

    private final BigArrays bigArrays;
    // 2 bits per bucket packed 4-per-byte; plain array so mode reads bypass BigArray VarHandle overhead.
    private byte[] modeTags;
    // Payload per bucket: encoded hash for LC_SINGLE, hllOrd for HLL; unread for EMPTY/LC_HASH.
    private LongArray bucketState;
    final HyperLogLog hll;
    private final LinearCounting lc;

    private Mode getMode(long bucket) {
        int b = Math.toIntExact(bucket);
        int byteIdx = b >>> 2;
        return byteIdx < modeTags.length ? Mode.VALUES[(modeTags[byteIdx] >>> ((b & 3) << 1)) & 3] : Mode.EMPTY;
    }

    private void setMode(long bucket, Mode mode) {
        int b = Math.toIntExact(bucket);
        int byteIdx = b >>> 2;
        if (byteIdx >= modeTags.length) {
            modeTags = Arrays.copyOf(modeTags, Math.max(byteIdx + 1, modeTags.length << 1));
        }
        int shift = (b & 3) << 1;
        modeTags[byteIdx] = (byte) ((modeTags[byteIdx] & ~(3 << shift)) | (mode.ordinal() << shift));
    }

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
        HyperLogLog hll = null;
        LinearCounting lc = null;
        LongArray bucketState = null;
        boolean success = false;
        try {
            hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
            lc = new LinearCounting(bigArrays, breaker, initialBucketCount, precision);
            bucketState = bigArrays.newLongArray(1);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(hll, lc, bucketState);
            }
        }
        this.hll = hll;
        this.lc = lc;
        this.bucketState = bucketState;
        this.modeTags = new byte[1];
    }

    public long maxOrd() {
        // LC_SINGLE buckets grow bucketState but don't touch lc.cells, so bucketState.size() is the reliable upper bound.
        return Math.max(bucketState.size(), lc.maxOrd());
    }

    @Override
    public long cardinality(long bucketOrd) {
        return switch (getMode(bucketOrd)) {
            case EMPTY -> 0L;
            case LC_SINGLE -> 1L;
            case LC_HASH -> lc.cardinality(bucketOrd);
            case HLL -> hll.cardinality(bucketState.get(bucketOrd));
        };
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return getMode(bucketOrd) == Mode.HLL;
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        return switch (getMode(bucketOrd)) {
            case LC_SINGLE -> new SingleHashIterator((int) bucketState.get(bucketOrd));
            case LC_HASH -> lc.values(bucketOrd);
            default -> AbstractLinearCounting.HashesIterator.EMPTY;
        };
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        return hll.getRunLens(bucketState.get(bucketOrd));
    }

    @Override
    public void collect(long bucket, long hash) {
        final Mode mode = getMode(bucket);
        if (mode == Mode.HLL) {
            hll.collect(bucketState.get(bucket), hash);
        } else {
            addEncodedToLcWithMode(bucket, mode, AbstractLinearCounting.encodeHash(hash, precision()));
        }
    }

    @Override
    public void close() {
        Releasables.close(bucketState, hll, lc);
    }

    long ramBytesUsed() {
        return bucketState.ramBytesUsed() + hll.ramBytesUsed() + lc.ramBytesUsed();
    }

    void addRunLen(long bucketOrd, int register, int runLen) {
        final long hllOrd = upgradeToHll(bucketOrd);
        hll.addRunLen(hllOrd, register, runLen);
    }

    long upgradeToHll(long bucketOrd) {
        final Mode mode = getMode(bucketOrd);
        if (mode == Mode.HLL) return bucketState.get(bucketOrd);
        final long hllOrd = hll.newBucket();
        switch (mode) {
            case LC_SINGLE -> hll.collectEncoded(hllOrd, (int) bucketState.get(bucketOrd));
            case LC_HASH -> lc.copyToHll(bucketOrd, hll, hllOrd);
            default -> {
            }
        }
        setMode(bucketOrd, Mode.HLL);
        bucketState = bigArrays.grow(bucketState, bucketOrd + 1);
        bucketState.set(bucketOrd, hllOrd);
        return hllOrd;
    }

    /** Inserts a pre-encoded hash into a bucket, routing to HLL or LC depending on current mode. */
    private void addEncoded(long bucket, int encoded) {
        final Mode mode = getMode(bucket);
        if (mode == Mode.HLL) {
            hll.collectEncoded(bucketState.get(bucket), encoded);
        } else {
            addEncodedToLcWithMode(bucket, mode, encoded);
        }
    }

    /** Shared core for LC-mode insertions; mode must already be read by the caller. HLL is a no-op. */
    private void addEncodedToLcWithMode(long bucket, Mode mode, int encoded) {
        switch (mode) {
            case EMPTY -> {
                setMode(bucket, Mode.LC_SINGLE);
                bucketState = bigArrays.grow(bucketState, bucket + 1);
                bucketState.set(bucket, encoded);
            }
            case LC_SINGLE -> {
                assert bucket < bucketState.size() : "LC_SINGLE bucket must already be in bucketState";
                final int prevEncoded = (int) bucketState.get(bucket);
                if (encoded == prevEncoded) return;
                lc.addEncoded(bucket, prevEncoded);
                final int newSize = lc.addEncoded(bucket, encoded);
                setMode(bucket, Mode.LC_HASH);
                assert newSize <= lc.threshold : "two elements cannot exceed LC threshold";
            }
            case LC_HASH -> {
                final int newSize = lc.addEncoded(bucket, encoded);
                if (newSize > lc.threshold) upgradeToHll(bucket);
            }
            case HLL -> throw new AssertionError("HLL state must be handled by caller before routing to addEncodedToLcWithMode");
        }
    }

    public void combine(long bucket, BytesRef other) throws IOException {
        ByteArrayStreamInput in = new ByteArrayStreamInput(other.bytes);
        in.reset(other.bytes, other.offset, other.length);
        final int precision = in.readVInt();
        final boolean algorithm = in.readBoolean();
        if (algorithm == LINEAR_COUNTING && getAlgorithm(bucket) == LINEAR_COUNTING) {
            final int length = Math.toIntExact(in.readVLong());
            for (int i = 0; i < length; i++) {
                final Mode curMode = getMode(bucket);
                if (curMode == Mode.HLL) {
                    final long hllOrd = bucketState.get(bucket);
                    do {
                        hll.collectEncoded(hllOrd, in.readInt());
                    } while (++i < length);
                    return;
                }
                addEncodedToLcWithMode(bucket, curMode, in.readInt());
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
        } else if (other instanceof HyperLogLogPlusPlus o) {
            // Fast path: bulk register max-merge directly on the backing byte arrays,
            // bypassing the per-register RunLenIterator.
            final long destHllOrd = upgradeToHll(thisBucket);
            hll.mergeRunLens(destHllOrd, o.hll, o.bucketState.get(otherBucket));
        } else {
            merge(thisBucket, other.getHyperLogLog(otherBucket));
        }
    }

    private void merge(long bucketOrd, AbstractLinearCounting.HashesIterator values) {
        while (values.next()) {
            addEncoded(bucketOrd, values.value());
        }
    }

    private void merge(long bucketOrd, AbstractHyperLogLog.RunLenIterator runLens) {
        final long hllOrd = upgradeToHll(bucketOrd);
        for (int i = 0; i < hll.m; ++i) {
            runLens.next();
            hll.addRunLen(hllOrd, i, runLens.value());
        }
    }

    private static class SingleHashIterator implements AbstractLinearCounting.HashesIterator {
        private final int encoded;
        private boolean done = false;

        SingleHashIterator(int encoded) {
            this.encoded = encoded;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public boolean next() {
            if (done == false) {
                done = true;
                return true;
            }
            return false;
        }

        @Override
        public int value() {
            return encoded;
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

        /**
         * Bulk register max-merge: sets dest[destOrd][i] = max(dest[destOrd][i], src[srcOrd][i]) for all m registers.
         * Processes in page-aligned chunks so ByteArray.get() is always zero-copy.
         */
        void mergeRunLens(long destOrd, HyperLogLog src, long srcOrd) {
            final long destStart = destOrd << p;
            final long srcStart = srcOrd << p;
            final int m = this.m;
            final int chunkSize = Math.min(m, PageCacheRecycler.BYTE_PAGE_SIZE);
            final BytesRef srcRef = new BytesRef();
            final BytesRef destRef = new BytesRef();
            for (int done = 0; done < m; done += chunkSize) {
                src.runLens.get(srcStart + done, chunkSize, srcRef);
                boolean destCopied = runLens.get(destStart + done, chunkSize, destRef);
                final byte[] s = srcRef.bytes;
                final byte[] d = destRef.bytes;
                final int so = srcRef.offset;
                final int doff = destRef.offset;
                for (int i = 0; i < chunkSize; i++) {
                    if (s[so + i] > d[doff + i]) {
                        d[doff + i] = s[so + i];
                    }
                }
                if (destCopied) {
                    runLens.set(destStart + done, destRef.bytes, destRef.offset, chunkSize);
                }
            }
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

    private static final class LinearCountingCell {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LinearCountingCell.class);
        private int size;
        private final int nextGrowSize;
        private final int mask;
        private final int[] values;

        LinearCountingCell(int capacity) {
            this.mask = capacity - 1;
            this.values = new int[capacity];
            this.nextGrowSize = (int) (capacity * MAX_LOAD_FACTOR);
        }

        static long bytesUsed(int length) {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * length
            );
        }

        void rehashTo(LinearCountingCell newCell) {
            final int[] newValues = newCell.values;
            final int newMask = newCell.mask;
            for (int v : this.values) {
                if (v != 0) {
                    int pos = v & newMask;
                    if (newValues[pos] != 0) {
                        do {
                            pos = (pos + 1) & newMask;
                        } while (newValues[pos] != 0);
                    }
                    newValues[pos] = v;
                }
            }
            newCell.size = this.size;
        }

        void add(int encoded) {
            assert encoded != 0;
            int pos = encoded & mask;
            while (values[pos] != 0) {
                if (values[pos] == encoded) {
                    return;
                }
                pos = (pos + 1) & mask;
            }
            values[pos] = encoded;
            ++size;
        }

        int capacity() {
            return values.length;
        }
    }

    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {
        private final LinearCountingCell cell;
        private int index;

        LinearCountingIterator(LinearCountingCell cell) {
            this.cell = cell;
            this.index = 0;
        }

        @Override
        public int size() {
            return cell.size;
        }

        @Override
        public boolean next() {
            while (index < cell.values.length) {
                int v = cell.values[index++];
                if (v != 0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int value() {
            return cell.values[index - 1];
        }
    }

    private static class LinearCounting extends AbstractLinearCounting implements Releasable {
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long bytesUsed;
        private final int threshold;
        private ObjectArray<LinearCountingCell> cells;
        private final int capacity;

        LinearCounting(BigArrays bigArrays, CircuitBreaker breaker, long initialBucketCount, int precision) {
            super(precision);
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.capacity = (1 << precision) / 4;
            this.threshold = (int) (capacity * MAX_LOAD_FACTOR);
            this.cells = bigArrays.newObjectArray(initialBucketCount);
        }

        private static int initialCellSize(long bucket, int capacity) {
            // Pre-allocate full capacity for the first few buckets to bypass the cost of multiple rehashes.
            // Optimized for ungrouped aggregations or those with few groups but high cardinality.
            if (bucket < 10) {
                return capacity;
            } else {
                return Math.min(capacity, 32);
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

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            assert encoded != 0;
            LinearCountingCell cell;
            if (bucketOrd >= cells.size()) {
                cells = bigArrays.grow(cells, bucketOrd + 1);
                cell = newCell(initialCellSize(bucketOrd, capacity));
                cells.set(bucketOrd, cell);
            } else {
                cell = cells.get(bucketOrd);
                if (cell != null) {
                    if (cell.size > cell.nextGrowSize) {
                        var newCell = newCell(cell.capacity() << 1);
                        cell.rehashTo(newCell);
                        cells.set(bucketOrd, newCell);
                        closeCell(cell);
                        cell = newCell;
                    }
                } else {
                    cell = newCell(initialCellSize(bucketOrd, capacity));
                    cells.set(bucketOrd, cell);
                }
            }
            cell.add(encoded);
            return cell.size;
        }

        @Override
        protected int size(long bucketOrd) {
            final var cell = bucketOrd < cells.size() ? cells.get(bucketOrd) : null;
            return cell != null ? cell.size : 0;
        }

        private HashesIterator values(long bucketOrd) {
            LinearCountingCell cell = bucketOrd < cells.size() ? cells.get(bucketOrd) : null;
            if (cell == null) {
                return AbstractLinearCounting.HashesIterator.EMPTY;
            } else {
                return new LinearCountingIterator(cell);
            }
        }

        void copyToHll(long bucketOrd, HyperLogLog hll, long hllBucket) {
            final LinearCountingCell cell = bucketOrd < cells.size() ? cells.get(bucketOrd) : null;
            if (cell == null) {
                return;
            }
            for (int v : cell.values) {
                if (v != 0) {
                    hll.collectEncoded(hllBucket, v);
                }
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
