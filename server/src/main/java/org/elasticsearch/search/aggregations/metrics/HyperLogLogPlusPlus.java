/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Hyperloglog++ counter, implemented based on pseudo code from
 * http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf and its appendix
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen
 *
 * This implementation is different from the original implementation in that it uses a hash table instead of a sorted list for linear
 * counting. Although this requires more space and makes hyperloglog (which is less accurate) used sooner, this is also considerably faster.
 * In order to limit the space requirements for low cardinality data at high precision, The Linear counting algorithm is first initialize
 * in a small unordered list that uses linear search.
 *
 * Trying to understand what this class does without having read the paper is considered adventurous.
 *
 * The HyperLogLogPlusPlus contains two algorithms, one for linear counting and the HyperLogLog algorithm. Initially hashes added to the
 * data structure are processed using the linear counting until a threshold defined by the precision is reached where the data is replayed
 * to the HyperLogLog algorithm and then this is used.
 *
 * It supports storing several HyperLogLogPlusPlus structures which are identified by a bucket number.
 */
public final class HyperLogLogPlusPlus implements Releasable {

    private static final float MAX_LOAD_FACTOR = 0.75f;
    private static final boolean LINEAR_COUNTING = false;
    private static final boolean HYPERLOGLOG = true;
    public static final int DEFAULT_PRECISION = 14;
    // Algorithm used by a bucketOrd.
    private final BitArray algorithm;
    // First structure created to handle low cardinality sketches.
    // Order is given by the bucketOrd
    private final LowCardinalityLinearCounting low;
    // Structure created when cardinality reaches capacity of LowCardinalityLinearCounting.
    // Order is given by the ordMapping
    private final LinearCounting lc;
    // Structure created when cardinality reaches capacity of LinearCounting.
    // Order is given by the ordMapping
    private final HyperLogLog hll;
    // Mapping between bucketOrd and ord in HLL and LinearCounting
    private LongArray ordMapping;
    private long maxOrd;
    private final BigArrays bigArrays;


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
        hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
        lc = new LinearCounting(bigArrays, initialBucketCount, precision, hll);
        low = new LowCardinalityLinearCounting(bigArrays, computeLowCardinalityForPrecision(precision), precision);
        algorithm = new BitArray(1, bigArrays);
        ordMapping = bigArrays.newLongArray(1, false);
        ordMapping.set(0, -1);
        this.bigArrays = bigArrays;
    }

    private static int computeLowCardinalityForPrecision(int precision) {
        // TODO: better heuristics here?
        return Math.max(1, precision - AbstractCardinalityAlgorithm.MIN_PRECISION);
    }

    public int precision() {
        return hll.precision();
    }

    public long maxBucket() {
        // This is an approximation, the value is always >= to the actual max bucket
        return low.hashes.size() / low.capacity;
    }

    public void merge(long thisBucket, HyperLogLogPlusPlus other, long otherBucket) {
        if (other.algorithm.get(otherBucket) == LINEAR_COUNTING) {
            other.low.bucket = otherBucket;
            if (other.low.exhausted()) {
                other.lc.ord = other.bucketToOrd(otherBucket);
                merge(thisBucket, other.lc);
            } else {
                merge(thisBucket, other.low);
            }
        } else {
            other.hll.ord = other.bucketToOrd(otherBucket);
            merge(thisBucket, other.hll);
        }
    }

    public void merge(long bucket, AbstractLinearCounting other) {
        if (precision() != other.precision()) {
            throw new IllegalArgumentException();
        }
        final long ord = bucketToOrd(bucket);
        final AbstractLinearCounting.HashesIterator values = other.values();
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            low.bucket = bucket;
            if (low.exhausted()) {
                mergeIntoLc(bucket, ord, values);
            } else {
                mergeIntoLow(bucket, ord, values);
            }
        } else {
            mergeIntoHLL(ord, values);
        }
    }

    private void mergeIntoLow(long bucket, long ord, AbstractLinearCounting.HashesIterator values) {
        while (values.next()) {
            low.addEncoded(values.value());
            if (low.exhausted()) {
                upgradeToLinearCounting(bucket, ord);
                mergeIntoLc(bucket, ord, values);
                return;
            }
        }
    }

    private void mergeIntoLc(long bucket, long ord, AbstractLinearCounting.HashesIterator values) {
        lc.ord = ord;
        while (values.next()) {
            if (lc.addEncoded(values.value()) > lc.threshold) {
                upgradeToHll(bucket, ord);
                mergeIntoHLL(ord, values);
                return;
            }
        }
    }

    private void mergeIntoHLL(long ord, AbstractLinearCounting.HashesIterator values) {
        hll.ord = ord;
        while (values.next()) {
            hll.collectEncoded(values.value());
        }
    }

    public void merge(long bucket, AbstractHyperLogLog other) {
        if (precision() != other.precision()) {
            throw new IllegalArgumentException();
        }
        final long ord = bucketToOrd(bucket);
        if (algorithm.get(bucket) != HYPERLOGLOG) {
            low.bucket = bucket;
            if (low.exhausted() == false) {
                upgradeToLinearCounting(bucket, ord);
            }
            upgradeToHll(bucket, ord);
        } else {
            hll.ord = ord;
        }
        hll.merge(other);
    }

    public void collect(long bucket, long hash) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            low.bucket = bucket;
            if (low.exhausted()) {
                lc.ord = bucketToOrd(bucket);
                if (lc.collect(hash) > lc.threshold) {
                    upgradeToHll(bucket, lc.ord);
                }
            } else {
                low.collect(hash);
                if (low.exhausted()) {
                    upgradeToLinearCounting(bucket, bucketToOrd(bucket));
                }
            }
        } else {
            hll.ord = bucketToOrd(bucket);
            hll.collect(hash);
        }
    }

    public long cardinality(long bucket) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            low.bucket = bucket;
            if (low.exhausted()) {
                lc.ord = bucketToOrd(bucket);
                return lc.cardinality();
            } else {
                return low.cardinality();
            }
        } else {
            hll.ord = bucketToOrd(bucket);
            return hll.cardinality();
        }
    }

    // protected for testing
    void upgradeToHll(long bucket, long ord) {
        lc.ord = ord;
        hll.ord = ord;
        final AbstractLinearCounting.HashesIterator hashes = lc.values();
        // We need to copy values into an arrays as we will override
        // the values on the buffer
        final IntArray values = lc.bigArrays.newIntArray(hashes.size());
        try {
            int i = 0;
            while (hashes.next()) {
                values.set(i++, hashes.value());
            }
            assert i == hashes.size();
            hll.reset();
            for (long j = 0; j < values.size(); ++j) {
                final int encoded = values.get(j);
                hll.collectEncoded(encoded);
            }
            algorithm.set(bucket);
        } finally {
            Releasables.close(values);
        }
    }

    // protected for testing
    void upgradeToLinearCounting(long bucket, long ord) {
        low.bucket = bucket;
        lc.ord = ord;
        hll.ensureCapacity(ord + 1);
        final AbstractLinearCounting.HashesIterator hashes = low.values();
        while (hashes.next()) {
            lc.addEncoded(hashes.value());
        }
    }

    // used for deserializing sketches
    private void collectEncoded(long bucket, int encoded) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            low.bucket = bucket;
            if (low.exhausted()) {
                lc.ord = bucketToOrd(bucket);
                if (lc.addEncoded(encoded) > lc.threshold) {
                    upgradeToHll(bucket, lc.ord);
                }
            } else {
                low.addEncoded(encoded);
                if (low.exhausted()) {
                    upgradeToLinearCounting(bucket, bucketToOrd(bucket));
                }
            }
        } else {
            hll.ord = bucketToOrd(bucket);
            hll.collectEncoded(encoded);
        }
    }

    private long bucketToOrd(long bucket) {
        long size = ordMapping.size();
        if (size <= bucket) {
            ordMapping = bigArrays.grow(ordMapping, bucket + 1);
            ordMapping.fill(size, ordMapping.size(), -1);
        }
        long ord = ordMapping.get(bucket);
        if (ord == -1) {
            ord = maxOrd;
            ordMapping.set(bucket, maxOrd++);
        }
        return ord;
    }

    @Override
    public void close() {
        Releasables.close(algorithm, hll, lc, low, ordMapping);
    }

    private Object getComparableData(long bucket) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            low.bucket = bucket;
            if (low.exhausted()) {
                lc.ord =  bucketToOrd(bucket);
                return lc.getComparableData();
            } else {
                return low.getComparableData();
            }
        } else {
            hll.ord = bucketToOrd(bucket);
            return hll.getComparableData();
        }
    }

    public int hashCode(long bucket) {
        return Objects.hash(precision(), algorithm.get(bucket), getComparableData(bucket));
    }

    public boolean equals(long bucket, HyperLogLogPlusPlus other) {
        return Objects.equals(precision(), other.precision())
            && Objects.equals(algorithm.get(bucket), other.algorithm.get(bucket))
            && Objects.equals(getComparableData(bucket), other.getComparableData(bucket));
    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(precision());
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            out.writeBoolean(LINEAR_COUNTING);
            low.bucket = bucket;
            AbstractLinearCounting.HashesIterator hashes;
            if (low.exhausted()) {
                lc.ord = bucketToOrd(bucket);
                hashes = lc.values();
            } else {
                hashes = low.values();
            }
            out.writeVLong(hashes.size());
            while (hashes.next()) {
                out.writeInt(hashes.value());
            }
        } else {
            out.writeBoolean(HYPERLOGLOG);
            hll.ord = bucketToOrd(bucket);
            AbstractHyperLogLog.RunLenIterator iterator = hll.getRunLens();
            while (iterator.next()){
                out.writeByte(iterator.value());
            }
        }
    }

    public static HyperLogLogPlusPlus readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        final int precision = in.readVInt();
        HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(precision, bigArrays, 1);
        final boolean algorithm = in.readBoolean();
        if (algorithm == LINEAR_COUNTING) {
            counts.algorithm.clear(0);
            final long size = in.readVLong();
            for (long i = 0; i < size; ++i) {
                final int encoded = in.readInt();
                counts.collectEncoded(0, encoded);
            }
        } else {
            counts.algorithm.set(0);
            counts.hll.ord = counts.bucketToOrd(0);
            for (int i = 0; i < counts.hll.m; ++i) {
                counts.hll.addRunLen(i, in.readByte());
            }
        }
        return counts;
    }

    private static class HyperLogLog extends AbstractHyperLogLog implements Releasable {
        private final BigArrays bigArrays;
        private final HyperLogLogIterator iterator;
        // array for holding the runlens.
        private ByteArray runLens;
        // Defines the position of the data structure. Callers of this object should set this value
        // before calling any of the methods.
        protected long ord;

        HyperLogLog(BigArrays bigArrays, long initialBucketCount, int precision) {
            super(precision);
            this.runLens =  bigArrays.newByteArray(initialBucketCount << precision);
            this.bigArrays = bigArrays;
            this.iterator = new HyperLogLogIterator(this, precision, m);
        }

        @Override
        protected void addRunLen(int register, int encoded) {
            final long bucketIndex = (ord << p) + register;
            runLens.set(bucketIndex, (byte) Math.max(encoded, runLens.get(bucketIndex)));
        }

        @Override
        protected RunLenIterator getRunLens() {
            iterator.reset(ord);
            return iterator;
        }

        protected void reset() {
            runLens.fill(ord << p, (ord << p) + m, (byte) 0);
        }

        protected Object getComparableData() {
            Map<Byte, Integer> values = new HashMap<>();
            for (long i = 0; i < runLens.size(); i++) {
                byte runLength = runLens.get((ord << p) + i);
                Integer numOccurances = values.get(runLength);
                if (numOccurances == null) {
                    values.put(runLength, 1);
                } else {
                    values.put(runLength, numOccurances + 1);
                }
            }
            return values;
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

        private final int capacity;
        protected final int threshold;
        private final int mask;
        private final BytesRef readSpare;
        private final ByteBuffer writeSpare;
        private final int p;
        private final BigArrays bigArrays;
        private final LinearCountingIterator iterator;
        // We are actually using HyperLogLog's runLens array but interpreting it as a hash set for linear counting.
        private final HyperLogLog hll;
        // Number of elements stored.
        private IntArray sizes;
        // Defines the position of the data structure. Callers of this object should set this value
        // before calling any of the methods.
        protected long ord;

        LinearCounting(BigArrays bigArrays, long initialBucketCount, int p, HyperLogLog hll) {
            super(p);
            this.bigArrays = bigArrays;
            this.hll = hll;
            capacity = (1 << p) / 4; // because ints take 4 bytes
            this.p = p;
            threshold = (int) (capacity * MAX_LOAD_FACTOR);
            mask = capacity - 1;
            sizes = bigArrays.newIntArray(initialBucketCount);
            readSpare = new BytesRef();
            writeSpare = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            iterator = new LinearCountingIterator(this, capacity);
        }

        @Override
        protected int addEncoded(int encoded) {
            sizes = bigArrays.grow(sizes, ord + 1);
            assert encoded != 0;
            for (int i = (encoded & mask);; i = (i + 1) & mask) {
                final int v = get(i);
                if (v == 0) {
                    // means unused, take it!
                    set(i, encoded);
                    return sizes.increment(ord, 1);
                } else if (v == encoded) {
                    // k is already in the set
                    return -1;
                }
            }
        }

        @Override
        protected int size() {
            if (ord >= sizes.size()) {
                return 0;
            }
            final int size = sizes.get(ord);
            assert size == recomputedSize();
            return size;
        }

        @Override
        protected HashesIterator values() {
            iterator.reset(size());
            return iterator;
        }

        protected Object getComparableData() {
            Set<Integer> values = new HashSet<>();
            HashesIterator iteratorValues = values();
            while (iteratorValues.next()) {
                values.add(iteratorValues.value());
            }
            return values;
        }

        private long index(int index) {
            return (ord << p) + (index << 2);
        }

        private int get(int index) {
            hll.runLens.get(index(index), 4, readSpare);
            return ByteUtils.readIntLE(readSpare.bytes, readSpare.offset);
        }

        private void set(int index, int value) {
            writeSpare.putInt(0, value);
            hll.runLens.set(index(index), writeSpare.array(), 0, 4);
        }

        private int recomputedSize() {
            int size = 0;
            for (int i = 0; i <= mask; ++i) {
                final int v = get(i);
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
        int pos;
        long size;
        private int value;

        LinearCountingIterator(LinearCounting lc, int capacity) {
            this.lc = lc;
            this.capacity = capacity;
        }

        void reset(long size) {
            this.pos = size == 0 ? capacity : 0;
            this.size = size;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < capacity) {
                for (; pos < capacity; ++pos) {
                    final int k = lc.get(pos);
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

    private static class LowCardinalityLinearCounting extends AbstractLinearCounting implements Releasable {
        private final BigArrays bigArrays;
        private final LowCardinalityLinearCountingIterator iterator;
        // max number of encoded hashes
        private final int capacity;
        // Number of elements stored.
        private IntArray sizes;
        // Actual encoded hashes stored.
        private IntArray hashes;
        // Defines the position of the data structure. Callers of this object should set this value
        // before calling any of the methods.
        protected long bucket;

        LowCardinalityLinearCounting(BigArrays bigArrays, int capacity, int p) {
            super(p);
            if (capacity < 1) {
                throw new IllegalArgumentException("Capacity must be bigger > 0, got " + capacity);
            }
            this.bigArrays = bigArrays;
            this.capacity = capacity;
            sizes = bigArrays.newIntArray(capacity);
            hashes = bigArrays.newIntArray(capacity);
            iterator = new LowCardinalityLinearCountingIterator(this);
        }

        @Override
        protected int addEncoded(int encoded) {
            assert encoded != 0;
            ensureCapacity(bucket);
            final int size = size();
            // we just iterate over all existing values
            for (int i = 0; i < size; i++) {
                if (get(i) == encoded) {
                    // encoded value is already in the set
                    return -1;
                }
            }
            assert get(size) == 0;
            set(size, encoded);
            return sizes.increment(bucket, 1);
        }

        @Override
        protected int size() {
            if (bucket >= sizes.size()) {
                return 0;
            }
            final int size = sizes.get(bucket);
            assert size == recomputedSize();
            return size;
        }

        protected boolean exhausted() {
            return capacity == size();
        }

        @Override
        protected HashesIterator values() {
            iterator.reset(size());
            return iterator;
        }

        protected Object getComparableData() {
            Set<Integer> values = new HashSet<>();
            HashesIterator iteratorValues = values();
            while (iteratorValues.next()) {
                values.add(iteratorValues.value());
            }
            return values;
        }

        private long index(int index) {
            return (bucket * capacity) + index;
        }

        private int get(int index) {
            return hashes.get(index(index));
        }

        private void set(int index, int value) {
            hashes.set(index(index), value);
        }

        private int recomputedSize() {
            if ((bucket + 1) * capacity > hashes.size()) {
                return 0;
            }
            for (int i = 0; i < capacity; ++i) {
                if (get(i) == 0) {
                    return i;
                }
            }
            return capacity;
        }

        private void ensureCapacity(long numBuckets) {
            sizes = bigArrays.grow(sizes, numBuckets + 1);
            hashes = bigArrays.grow(hashes, capacity * (numBuckets + 1));
        }

        @Override
        public void close() {
            Releasables.close(sizes, hashes);
        }
    }

    private static class LowCardinalityLinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final LowCardinalityLinearCounting lc;
        private int pos;
        private long size;
        private int value;

        LowCardinalityLinearCountingIterator(LowCardinalityLinearCounting lc) {
            this.lc = lc;
        }

        void reset(long size) {
            this.pos = 0;
            this.size = size;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < size) {
                value = lc.get(pos++);
                return true;
            }
            return false;
        }

        @Override
        public int value() {
            return value;
        }
    }
}
