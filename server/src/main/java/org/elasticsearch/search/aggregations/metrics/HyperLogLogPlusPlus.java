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
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;

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

    private final BitArray algorithm;
    private final CircuitBreaker breaker;
    private final HyperLogLog hll;
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
        this.breaker = breaker;
        HyperLogLog hll = null;
        LinearCounting lc = null;
        BitArray algorithm = null;
        boolean success = false;
        try {
            hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
            lc = new LinearCounting(bigArrays, breaker, initialBucketCount, precision);
            algorithm = new BitArray(1, bigArrays);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(hll, lc, algorithm);
            }
        }
        this.hll = hll;
        this.lc = lc;
        this.algorithm = algorithm;
    }

    public long maxOrd() {
        return Math.max(hll.maxOrd(), lc.maxOrd());
    }

    @Override
    public long cardinality(long bucketOrd) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            return lc.cardinality(bucketOrd);
        } else {
            return hll.cardinality(bucketOrd);
        }
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return algorithm.get(bucketOrd);
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        return lc.values(bucketOrd);
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        return hll.getRunLens(bucketOrd);
    }

    @Override
    public void collect(long bucket, long hash) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            final int newSize = lc.collect(bucket, hash);
            if (newSize > lc.threshold) {
                upgradeToHll(bucket);
            }
        } else {
            hll.ensureCapacity(bucket + 1);
            hll.collect(bucket, hash);
        }
    }

    @Override
    public void close() {
        Releasables.close(algorithm, hll, lc);
    }

    protected void addRunLen(long bucketOrd, int register, int runLen) {
        if (algorithm.get(bucketOrd) == LINEAR_COUNTING) {
            upgradeToHll(bucketOrd);
        }
        hll.addRunLen(0, register, runLen);
    }

    void upgradeToHll(long bucketOrd) {
        // We need to copy values into an arrays as we will override
        // the values on the buffer
        hll.ensureCapacity(bucketOrd + 1);
        // It's safe to reuse lc's readSpare because we're single threaded.
        final AbstractLinearCounting.HashesIterator hashes = lc.values(bucketOrd);
        try {
            int size = hashes.size();
            hll.reset(bucketOrd);
            for (int i = 0; i < size; i++) {
                hashes.next();
                hll.collectEncoded(bucketOrd, hashes.value());
            }
            algorithm.set(bucketOrd);
        } finally {
            lc.closeBucket(bucketOrd);
        }
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
                int i = 0;
                while (i < length) {
                    // TODO: bulk
                    int size = lc.addEncoded(bucket, values[i++]);
                    if (size > lc.threshold) {
                        upgradeToHll(bucket);
                        break;
                    }
                }
                while (i < length) {
                    hll.collectEncoded(bucket, values[i++]);
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
        hll.ensureCapacity(thisBucket + 1);
        if (other.getAlgorithm(otherBucket) == LINEAR_COUNTING) {
            merge(thisBucket, other.getLinearCounting(otherBucket));
        } else {
            merge(thisBucket, other.getHyperLogLog(otherBucket));
        }
    }

    private void merge(long thisBucket, AbstractLinearCounting.HashesIterator values) {
        while (values.next()) {
            final int encoded = values.value();
            if (algorithm.get(thisBucket) == LINEAR_COUNTING) {
                final int newSize = lc.addEncoded(thisBucket, encoded);
                if (newSize > lc.threshold) {
                    upgradeToHll(thisBucket);
                }
            } else {
                hll.collectEncoded(thisBucket, encoded);
            }
        }
    }

    private void merge(long thisBucket, AbstractHyperLogLog.RunLenIterator runLens) {
        if (algorithm.get(thisBucket) != HYPERLOGLOG) {
            upgradeToHll(thisBucket);
        }
        for (int i = 0; i < hll.m; ++i) {
            runLens.next();
            hll.addRunLen(thisBucket, i, runLens.value());
        }
    }

    private static class HyperLogLog extends AbstractHyperLogLog implements Releasable {
        private final BigArrays bigArrays;
        // array for holding the runlens.
        private ByteArray runLens;

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

        protected void reset(long bucketOrd) {
            runLens.fill(bucketOrd << p, (bucketOrd << p) + m, (byte) 0);
        }

        protected void ensureCapacity(long numBuckets) {
            runLens = bigArrays.grow(runLens, numBuckets << p);
        }

        @Override
        public void close() {
            Releasables.close(runLens);
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
        private final int initialCellSize;

        LinearCounting(BigArrays bigArrays, CircuitBreaker breaker, long initialBucketCount, int precision) {
            super(precision);
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            final int capacity = (1 << precision) / 4;
            this.initialCellSize = Math.min(capacity, 32);
            this.threshold = (int) (capacity * MAX_LOAD_FACTOR);
            this.cells = bigArrays.newObjectArray(initialBucketCount);
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
                cell = newCell(initialCellSize);
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
                    cell = newCell(initialCellSize);
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

        private void closeBucket(long bucketOrd) {
            if (bucketOrd < cells.size()) {
                LinearCountingCell cell = cells.get(bucketOrd);
                if (cell != null) {
                    closeCell(cell);
                    cells.set(bucketOrd, null);
                }
            }
        }

        long maxOrd() {
            return cells.size() - 1;
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-bytesUsed);
            Releasables.close(cells);
        }
    }
}
