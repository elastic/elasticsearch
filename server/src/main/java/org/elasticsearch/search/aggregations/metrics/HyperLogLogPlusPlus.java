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
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
 * data structure are processed using a small array and then linear counting until a threshold defined by the precision is reached where
 * the data is replayed to the HyperLogLog algorithm and then this is used. The small array delays allocating the full hash/HLL backing
 * bytes until a bucket shows meaningful cardinality.
 *
 * It supports storing several HyperLogLogPlusPlus structures which are identified by a bucket number.
 */
public final class HyperLogLogPlusPlus extends AbstractHyperLogLogPlusPlus {

    private static final float MAX_LOAD_FACTOR = 0.75f;
    private static final int SMALL_ARRAY_SIZE = 16;

    public static final int DEFAULT_PRECISION = 14;

    private final BigArrays bigArrays;
    private final BitArray algorithm;
    // Maps bucket ord to dense ord + 1 (0 means array tier / no dense slot).
    // Dense ords index into the shared runLens backing storage so memory grows with promoted buckets, not max bucket ord.
    private IntArray denseOrds;
    private long denseCount;
    private long maxBucketOrd;
    private final int arrayLimit;
    private final InlineSmallLinearCounting smallLc;
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
        super(precision);
        HyperLogLog hll = null;
        LinearCounting lc = null;
        InlineSmallLinearCounting smallLc = null;
        IntArray denseOrds = null;
        BitArray algorithm = null;
        int arrayLimit;
        boolean success = false;
        try {
            hll = new HyperLogLog(bigArrays, 0, precision);
            lc = new LinearCounting(bigArrays, 0, precision, hll);
            // Keep the array tier small and below the hash LC threshold to avoid immediate promotion.
            arrayLimit = Math.min(SMALL_ARRAY_SIZE, lc.threshold);
            smallLc = new InlineSmallLinearCounting(precision, bigArrays, initialBucketCount, arrayLimit);
            denseOrds = bigArrays.newIntArray(initialBucketCount);
            algorithm = new BitArray(1, bigArrays);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(hll, lc, smallLc, denseOrds, algorithm);
            }
        }
        this.bigArrays = bigArrays;
        this.hll = hll;
        this.lc = lc;
        this.smallLc = smallLc;
        this.denseOrds = denseOrds;
        this.algorithm = algorithm;
        this.arrayLimit = arrayLimit;
        this.maxBucketOrd = 0;
        this.denseCount = 0;
    }

    public long maxOrd() {
        return maxBucketOrd;
    }

    @Override
    public long cardinality(long bucketOrd) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            final int denseOrd = denseOrd(bucketOrd);
            if (denseOrd < 0) {
                return smallLc.cardinality(bucketOrd);
            }
            return lc.cardinality(denseOrd);
        } else {
            final int denseOrd = denseOrd(bucketOrd);
            assert denseOrd >= 0;
            if (denseOrd < 0) {
                return 0;
            }
            return hll.cardinality(denseOrd);
        }
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return algorithm.get(bucketOrd);
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        final int denseOrd = denseOrd(bucketOrd);
        if (denseOrd < 0) {
            return smallLc.values(bucketOrd);
        }
        return lc.values(denseOrd);
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        final int denseOrd = denseOrd(bucketOrd);
        if (denseOrd < 0) {
            throw new IllegalStateException("Missing dense ord for bucket " + bucketOrd);
        }
        return hll.getRunLens(denseOrd);
    }

    @Override
    public void collect(long bucket, long hash) {
        updateMaxBucketOrd(bucket);
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            int denseOrd = denseOrd(bucket);
            if (denseOrd < 0) {
                final int encoded = AbstractLinearCounting.encodeHash(hash, p);
                final int newSize = smallLc.addEncodedDirect(bucket, encoded);
                if (newSize > arrayLimit) {
                    denseOrd = promoteArrayToHash(bucket);
                    final int lcSize = lc.addEncoded(denseOrd, encoded);
                    if (lcSize > lc.threshold) {
                        upgradeToHll(bucket);
                    }
                }
            } else {
                final int newSize = lc.collect(denseOrd, hash);
                if (newSize > lc.threshold) {
                    upgradeToHll(bucket);
                }
            }
        } else {
            hll.collect(ensureDenseOrd(bucket), hash);
        }
    }

    @Override
    public void close() {
        Releasables.close(algorithm, hll, lc, smallLc, denseOrds);
    }

    private void updateMaxBucketOrd(long bucketOrd) {
        final long next = bucketOrd + 1;
        if (next > maxBucketOrd) {
            maxBucketOrd = next;
        }
    }

    private int denseOrd(long bucketOrd) {
        if (bucketOrd >= denseOrds.size()) {
            return -1;
        }
        final int value = denseOrds.get(bucketOrd);
        return value == 0 ? -1 : value - 1;
    }

    private int ensureDenseOrd(long bucketOrd) {
        // Ensure backing storage is grown before publishing the mapping so breaker failures do not leave
        // a bucket pointing at a dense ord without allocated backing bytes.
        denseOrds = bigArrays.grow(denseOrds, bucketOrd + 1);
        final int value = denseOrds.get(bucketOrd);
        if (value != 0) {
            return value - 1;
        }
        final long newDenseCount = denseCount + 1;
        lc.ensureCapacity(newDenseCount);
        hll.ensureCapacity(newDenseCount);
        final int ord = Math.toIntExact(denseCount);
        denseOrds.set(bucketOrd, ord + 1);
        denseCount = newDenseCount;
        return ord;
    }

    private int promoteArrayToHash(long bucketOrd) {
        final int denseOrd = ensureDenseOrd(bucketOrd);
        final AbstractLinearCounting.HashesIterator hashes = smallLc.values(bucketOrd);
        while (hashes.next()) {
            lc.addEncoded(denseOrd, hashes.value());
        }
        smallLc.clear(bucketOrd);
        return denseOrd;
    }

    protected void addRunLen(long bucketOrd, int register, int runLen) {
        updateMaxBucketOrd(bucketOrd);
        if (algorithm.get(bucketOrd) == LINEAR_COUNTING) {
            upgradeToHll(bucketOrd);
        }
        hll.addRunLen(ensureDenseOrd(bucketOrd), register, runLen);
    }

    void upgradeToHll(long bucketOrd) {
        // We need to copy values into an array since we will override buffers.
        final int existingDenseOrd = denseOrd(bucketOrd);
        final AbstractLinearCounting.HashesIterator hashes;
        if (existingDenseOrd >= 0) {
            // It's safe to reuse lc's readSpare because we're single threaded.
            hashes = new LinearCountingIterator(lc, lc.readSpare, existingDenseOrd);
        } else {
            hashes = smallLc.values(bucketOrd);
        }
        final int denseOrd = ensureDenseOrd(bucketOrd);
        final IntArray values = bigArrays.newIntArray(hashes.size());
        try {
            int i = 0;
            while (hashes.next()) {
                values.set(i++, hashes.value());
            }
            assert i == hashes.size();
            hll.reset(denseOrd);
            for (long j = 0; j < values.size(); ++j) {
                final int encoded = values.get(j);
                hll.collectEncoded(denseOrd, encoded);
            }
            algorithm.set(bucketOrd);
            if (existingDenseOrd < 0) {
                smallLc.clear(bucketOrd);
            }
        } finally {
            Releasables.close(values);
        }
    }

    public void merge(long thisBucket, AbstractHyperLogLogPlusPlus other, long otherBucket) {
        if (precision() != other.precision()) {
            throw new IllegalArgumentException();
        }
        updateMaxBucketOrd(thisBucket);
        if (other.getAlgorithm(otherBucket) == LINEAR_COUNTING) {
            merge(thisBucket, other.getLinearCounting(otherBucket));
        } else {
            merge(thisBucket, other.getHyperLogLog(otherBucket));
        }
    }

    public void mergeSerialized(long thisBucket, StreamInput in) throws IOException {
        final int otherPrecision = in.readVInt();
        if (precision() != otherPrecision) {
            throw new IllegalArgumentException();
        }
        updateMaxBucketOrd(thisBucket);
        final boolean otherAlgorithm = in.readBoolean();
        if (otherAlgorithm == LINEAR_COUNTING) {
            final int size = Math.toIntExact(in.readVLong());
            for (int i = 0; i < size; ++i) {
                mergeEncoded(thisBucket, in.readInt());
            }
        } else {
            if (algorithm.get(thisBucket) != HYPERLOGLOG) {
                upgradeToHll(thisBucket);
            }
            final int denseOrd = ensureDenseOrd(thisBucket);
            for (int i = 0; i < hll.m; ++i) {
                hll.addRunLen(denseOrd, i, in.readByte());
            }
        }
    }

    private void mergeEncoded(long thisBucket, int encoded) {
        if (algorithm.get(thisBucket) == LINEAR_COUNTING) {
            int denseOrd = denseOrd(thisBucket);
            if (denseOrd < 0) {
                final int newSize = smallLc.addEncodedDirect(thisBucket, encoded);
                if (newSize > arrayLimit) {
                    denseOrd = promoteArrayToHash(thisBucket);
                    final int lcSize = lc.addEncoded(denseOrd, encoded);
                    if (lcSize > lc.threshold) {
                        upgradeToHll(thisBucket);
                    }
                }
            } else {
                final int newSize = lc.addEncoded(denseOrd, encoded);
                if (newSize > lc.threshold) {
                    upgradeToHll(thisBucket);
                }
            }
        } else {
            hll.collectEncoded(ensureDenseOrd(thisBucket), encoded);
        }
    }

    private void merge(long thisBucket, AbstractLinearCounting.HashesIterator values) {
        while (values.next()) {
            mergeEncoded(thisBucket, values.value());
        }
    }

    private void merge(long thisBucket, AbstractHyperLogLog.RunLenIterator runLens) {
        if (algorithm.get(thisBucket) != HYPERLOGLOG) {
            upgradeToHll(thisBucket);
        }
        final int denseOrd = ensureDenseOrd(thisBucket);
        for (int i = 0; i < hll.m; ++i) {
            runLens.next();
            hll.addRunLen(denseOrd, i, runLens.value());
        }
    }

    private static class HyperLogLog extends AbstractHyperLogLog implements Releasable {
        private final BigArrays bigArrays;
        // array for holding the runlens.
        private ByteArray runLens;

        HyperLogLog(BigArrays bigArrays, long initialDenseCount, int precision) {
            super(precision);
            this.runLens = bigArrays.newByteArray(initialDenseCount << precision);
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

    private static class InlineSmallLinearCounting extends AbstractLinearCounting implements Releasable {

        private final BigArrays bigArrays;
        private final int arrayLimit;
        private IntArray values;
        private ByteArray sizes;

        InlineSmallLinearCounting(int p, BigArrays bigArrays, long initialBuckets, int arrayLimit) {
            super(p);
            this.bigArrays = bigArrays;
            this.arrayLimit = arrayLimit;
            IntArray values = null;
            ByteArray sizes = null;
            boolean success = false;
            try {
                long initialSize = Math.multiplyExact(initialBuckets, (long) arrayLimit);
                values = bigArrays.newIntArray(initialSize);
                sizes = bigArrays.newByteArray(initialBuckets);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(values);
                    Releasables.close(sizes);
                }
            }
            this.values = values;
            this.sizes = sizes;
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            ensureCapacity(bucketOrd);
            assert encoded != 0;
            final long base = baseOffset(bucketOrd);
            int size = Byte.toUnsignedInt(sizes.get(bucketOrd));
            for (int i = 0; i < size; ++i) {
                if (values.get(base + i) == encoded) {
                    return -1;
                }
            }
            if (size >= arrayLimit) {
                return size + 1;
            }
            values.set(base + size, encoded);
            sizes.set(bucketOrd, (byte) (size + 1));
            return size + 1;
        }

        @Override
        protected int size(long bucketOrd) {
            if (bucketOrd >= sizes.size()) {
                return 0;
            }
            return Byte.toUnsignedInt(sizes.get(bucketOrd));
        }

        private HashesIterator values(long bucketOrd) {
            return new InlineSmallLinearCountingIterator(this, bucketOrd);
        }

        private void clear(long bucketOrd) {
            if (bucketOrd >= sizes.size()) {
                return;
            }
            sizes.set(bucketOrd, (byte) 0);
        }

        private int addEncodedDirect(long bucketOrd, int encoded) {
            return addEncoded(bucketOrd, encoded);
        }

        private long baseOffset(long bucketOrd) {
            return Math.multiplyExact(bucketOrd, (long) arrayLimit);
        }

        private void ensureCapacity(long bucketOrd) {
            sizes = bigArrays.grow(sizes, bucketOrd + 1);
            long requiredSize = Math.multiplyExact(bucketOrd + 1, (long) arrayLimit);
            values = bigArrays.grow(values, requiredSize);
        }

        @Override
        public void close() {
            Releasables.close(values, sizes);
        }
    }

    private static class InlineSmallLinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final InlineSmallLinearCounting small;
        private final long bucketOrd;
        private final int size;
        private final long base;
        private int value;
        private long pos;

        InlineSmallLinearCountingIterator(InlineSmallLinearCounting small, long bucketOrd) {
            this.small = small;
            this.bucketOrd = bucketOrd;
            this.size = small.size(bucketOrd);
            this.base = size == 0 ? 0 : small.baseOffset(bucketOrd);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos >= size) {
                return false;
            }
            value = small.values.get(base + pos++);
            return true;
        }

        @Override
        public int value() {
            return value;
        }
    }

    private static class LinearCounting extends AbstractLinearCounting implements Releasable {
        // bucketOrd parameters are dense ords in this implementation.

        protected final int threshold;
        private final int mask;
        private final BytesRef readSpare;
        private final ByteBuffer writeSpare;
        private final BigArrays bigArrays;
        // We are actually using HyperLogLog's runLens array but interpreting it as a hash set for linear counting.
        private final HyperLogLog hll;
        private final int capacity;
        // Number of elements stored (keyed by dense ord).
        private IntArray sizes;

        LinearCounting(BigArrays bigArrays, long initialBucketCount, int p, HyperLogLog hll) {
            super(p);
            this.bigArrays = bigArrays;
            this.hll = hll;
            this.capacity = (1 << p) / 4; // because ints take 4 bytes
            threshold = (int) (capacity * MAX_LOAD_FACTOR);
            mask = capacity - 1;
            sizes = bigArrays.newIntArray(initialBucketCount);
            readSpare = new BytesRef();
            writeSpare = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            sizes = bigArrays.grow(sizes, bucketOrd + 1);
            assert encoded != 0;
            for (int i = (encoded & mask);; i = (i + 1) & mask) {
                hll.runLens.get(index(bucketOrd, i), 4, readSpare);
                final int v = ByteUtils.readIntLE(readSpare.bytes, readSpare.offset);
                if (v == 0) {
                    // means unused, take it!
                    set(bucketOrd, i, encoded);
                    return sizes.increment(bucketOrd, 1);
                } else if (v == encoded) {
                    // k is already in the set
                    return -1;
                }
            }
        }

        @Override
        protected int size(long bucketOrd) {
            if (bucketOrd >= sizes.size()) {
                return 0;
            }
            final int size = sizes.get(bucketOrd);
            assert size == recomputedSize(bucketOrd);
            return size;
        }

        private HashesIterator values(long bucketOrd) {
            // Make a fresh BytesRef for reading scratch work because this method can be called on many threads
            return new LinearCountingIterator(this, new BytesRef(), bucketOrd);
        }

        private long index(long bucketOrd, int index) {
            return (bucketOrd << p) + (index << 2);
        }

        private void set(long bucketOrd, int index, int value) {
            writeSpare.putInt(0, value);
            hll.runLens.set(index(bucketOrd, index), writeSpare.array(), 0, 4);
        }

        private int recomputedSize(long bucketOrd) {
            if (bucketOrd >= hll.maxOrd()) {
                return 0;
            }
            int size = 0;
            BytesRef spare = new BytesRef();
            for (int i = 0; i <= mask; ++i) {
                hll.runLens.get(index(bucketOrd, i), 4, spare);
                final int v = ByteUtils.readIntLE(spare.bytes, spare.offset);
                if (v != 0) {
                    ++size;
                }
            }
            return size;
        }

        @Override
        public void close() {
            Releasables.close(sizes);
        }

        private void ensureCapacity(long numBuckets) {
            sizes = bigArrays.grow(sizes, numBuckets);
        }
    }

    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final LinearCounting lc;
        private final BytesRef spare;
        private final long bucketOrd;
        private final int size;
        private int pos;
        private int value;

        LinearCountingIterator(LinearCounting lc, BytesRef spare, long bucketOrd) {
            this.lc = lc;
            this.spare = spare;
            this.bucketOrd = bucketOrd;
            this.size = lc.size(bucketOrd);
            this.pos = size == 0 ? lc.capacity : 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < lc.capacity) {
                for (; pos < lc.capacity; ++pos) {
                    lc.hll.runLens.get(lc.index(bucketOrd, pos), 4, spare);
                    int k = ByteUtils.readIntLE(spare.bytes, spare.offset);
                    if (k != 0) {
                        ++pos;
                        value = k;
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int value() {
            return value;
        }
    }
}
