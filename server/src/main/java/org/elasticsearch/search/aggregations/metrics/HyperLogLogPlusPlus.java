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
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.IntArray;

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

    private final OpenBitSet algorithm;
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
        hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
        lc = new LinearCounting(bigArrays, initialBucketCount, precision, hll);
        algorithm = new OpenBitSet();
    }

    public int precision() {
        return hll.precision();
    }

    public long maxBucket() {
        return hll.runLens.size() >>> hll.precision();
    }

    public void merge(long thisBucket, HyperLogLogPlusPlus other, long otherBucket) {
        if (precision() != other.precision()) {
            throw new IllegalArgumentException();
        }
        hll.bucket = thisBucket;
        lc.bucket = thisBucket;
        hll.ensureCapacity(thisBucket + 1);
        if (other.algorithm.get(otherBucket) == LINEAR_COUNTING) {
            other.lc.bucket = otherBucket;
            final AbstractLinearCounting.HashesIterator values = other.lc.values();
            while (values.next()) {
                final int encoded = values.value();
                if (algorithm.get(thisBucket) == LINEAR_COUNTING) {
                    final int newSize = lc.addEncoded(encoded);
                    if (newSize > lc.threshold) {
                        upgradeToHll(thisBucket);
                    }
                } else {
                    hll.collectEncoded(encoded);
                }
            }
        } else {
            if (algorithm.get(thisBucket) != HYPERLOGLOG) {
                upgradeToHll(thisBucket);
            }
            other.hll.bucket = otherBucket;
            hll.merge(other.hll);
        }
    }

    public void collect(long bucket, long hash) {
        hll.ensureCapacity(bucket + 1);
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            lc.bucket = bucket;
            final int newSize = lc.collect(hash);
            if (newSize > lc.threshold) {
                upgradeToHll(bucket);
            }
        } else {
            hll.bucket = bucket;
            hll.collect(hash);
        }
    }

    public long cardinality(long bucket) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            lc.bucket = bucket;
            return lc.cardinality();
        } else {
            hll.bucket = bucket;
            return hll.cardinality();
        }
    }

    void upgradeToHll(long bucket) {
        hll.ensureCapacity(bucket + 1);
        lc.bucket = bucket;
        hll.bucket = bucket;
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

    @Override
    public void close() {
        Releasables.close(hll, lc);
    }

    private Object getComparableData(long bucket) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            lc.bucket = bucket;
            return lc.getComparableData();
        } else {
            hll.bucket = bucket;
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
            lc.bucket = bucket;
            AbstractLinearCounting.HashesIterator hashes = lc.values();
            out.writeVLong(hashes.size());
            while (hashes.next()) {
                out.writeInt(hashes.value());
            }
        } else {
            out.writeBoolean(HYPERLOGLOG);
            hll.bucket = bucket;
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
            counts.lc.bucket = 0;
            for (long i = 0; i < size; ++i) {
                final int encoded = in.readInt();
                counts.lc.addEncoded(encoded);
            }
        } else {
            counts.algorithm.set(0);
            counts.hll.bucket = 0;
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
        protected long bucket;

        HyperLogLog(BigArrays bigArrays, long initialBucketCount, int precision) {
            super(precision);
            this.runLens =  bigArrays.newByteArray(initialBucketCount << precision);
            this.bigArrays = bigArrays;
            this.iterator = new HyperLogLogIterator(this, precision, m);
        }

        @Override
        protected void addRunLen(int register, int encoded) {
            final long bucketIndex = (bucket << p) + register;
            runLens.set(bucketIndex, (byte) Math.max(encoded, runLens.get(bucketIndex)));
        }

        @Override
        protected RunLenIterator getRunLens() {
            iterator.reset(bucket);
            return iterator;
        }

        protected void reset() {
            runLens.fill(bucket << p, (bucket << p) + m, (byte) 0);
        }

        protected Object getComparableData() {
            Map<Byte, Integer> values = new HashMap<>();
            for (long i = 0; i < runLens.size(); i++) {
                byte runLength = runLens.get((bucket << p) + i);
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
        protected long bucket;

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
            sizes = bigArrays.grow(sizes, bucket + 1);
            assert encoded != 0;
            for (int i = (encoded & mask);; i = (i + 1) & mask) {
                final int v = get(i);
                if (v == 0) {
                    // means unused, take it!
                    set(i, encoded);
                    return sizes.increment(bucket, 1);
                } else if (v == encoded) {
                    // k is already in the set
                    return -1;
                }
            }
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
            return (bucket << p) + (index << 2);
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

    /** looks and smells like the old openbitset. */
    static class OpenBitSet {
        LongBitSet impl = new LongBitSet(64);

        boolean get(long bit) {
            if (bit < impl.length()) {
                return impl.get(bit);
            } else {
                return false;
            }
        }

        void ensureCapacity(long bit) {
            impl = LongBitSet.ensureCapacity(impl, bit);
        }

        void set(long bit) {
            ensureCapacity(bit);
            impl.set(bit);
        }

        void clear(long bit) {
            ensureCapacity(bit);
            impl.clear(bit);
        }
    }
}
