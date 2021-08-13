/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.IntArray;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Hyperloglog++ counter, implemented based on pseudo code from
 * http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf and its appendix
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen
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
        BitArray algorithm = null;
        boolean success = false;
        try {
            hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
            lc = new LinearCounting(bigArrays, initialBucketCount, precision, hll);
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

    @Override
    public long maxOrd() {
        return hll.maxOrd();
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
        hll.ensureCapacity(bucket + 1);
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            final int newSize = lc.collect(bucket, hash);
            if (newSize > lc.threshold) {
                upgradeToHll(bucket);
            }
        } else {
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
        hll.ensureCapacity(bucketOrd + 1);
        final AbstractLinearCounting.HashesIterator hashes = lc.values(bucketOrd);
        // We need to copy values into an arrays as we will override
        // the values on the buffer
        final IntArray values = lc.bigArrays.newIntArray(hashes.size());
        try {
            int i = 0;
            while (hashes.next()) {
                values.set(i++, hashes.value());
            }
            assert i == hashes.size();
            hll.reset(bucketOrd);
            for (long j = 0; j < values.size(); ++j) {
                final int encoded = values.get(j);
                hll.collectEncoded(bucketOrd, encoded);
            }
            algorithm.set(bucketOrd);
        } finally {
            Releasables.close(values);
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
        private final HyperLogLogIterator iterator;
        // array for holding the runlens.
        private ByteArray runLens;


        HyperLogLog(BigArrays bigArrays, long initialBucketCount, int precision) {
            super(precision);
            this.runLens =  bigArrays.newByteArray(initialBucketCount << precision);
            this.bigArrays = bigArrays;
            this.iterator = new HyperLogLogIterator(this, precision, m);
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
            iterator.reset(bucketOrd);
            return iterator;
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
        private final int m, p;
        int pos;
        long start;
        private byte value;

        HyperLogLogIterator(HyperLogLog hll, int p, int m) {
            this.hll = hll;
            this.m = m;
            this.p = p;
        }

        void reset(long bucket) {
            pos = 0;
            start = bucket << p;
        }

        @Override
        public boolean next() {
            if (pos < m) {
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

    private static class LinearCounting extends AbstractLinearCounting implements Releasable {

        protected final int threshold;
        private final int mask;
        private final BytesRef readSpare;
        private final ByteBuffer writeSpare;
        private final BigArrays bigArrays;
        private final LinearCountingIterator iterator;
        // We are actually using HyperLogLog's runLens array but interpreting it as a hash set for linear counting.
        private final HyperLogLog hll;
        // Number of elements stored.
        private IntArray sizes;

        LinearCounting(BigArrays bigArrays, long initialBucketCount, int p, HyperLogLog hll) {
            super(p);
            this.bigArrays = bigArrays;
            this.hll = hll;
            final int capacity = (1 << p) / 4; // because ints take 4 bytes
            threshold = (int) (capacity * MAX_LOAD_FACTOR);
            mask = capacity - 1;
            sizes = bigArrays.newIntArray(initialBucketCount);
            readSpare = new BytesRef();
            writeSpare = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            iterator = new LinearCountingIterator(this, capacity);
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            sizes = bigArrays.grow(sizes, bucketOrd + 1);
            assert encoded != 0;
            for (int i = (encoded & mask);; i = (i + 1) & mask) {
                final int v = get(bucketOrd, i);
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

        @Override
        protected HashesIterator values(long bucketOrd) {
            iterator.reset(bucketOrd, size(bucketOrd));
            return iterator;
        }

        private long index(long bucketOrd, int index) {
            return (bucketOrd << p) + (index << 2);
        }

        private int get(long bucketOrd, int index) {
            hll.runLens.get(index(bucketOrd, index), 4, readSpare);
            return ByteUtils.readIntLE(readSpare.bytes, readSpare.offset);
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
            for (int i = 0; i <= mask; ++i) {
                final int v = get(bucketOrd, i);
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
    }

    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final LinearCounting lc;
        private final int capacity;
        private int pos, size;
        private long bucketOrd;
        private int value;

        LinearCountingIterator(LinearCounting lc, int capacity) {
            this.lc = lc;
            this.capacity = capacity;
        }

        void reset(long bucketOrd, int size) {
            this.bucketOrd = bucketOrd;
            this.size = size;
            this.pos = size == 0 ? capacity : 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < capacity) {
                for (; pos < capacity; ++pos) {
                    final int k = lc.get(bucketOrd, pos);
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
