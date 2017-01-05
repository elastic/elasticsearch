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

package org.elasticsearch.search.aggregations.metrics.cardinality;

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

/**
 * Hyperloglog++ counter, implemented based on pseudo code from
 * http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf
 * and its appendix
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen
 *
 * This implementation is different from the original implementation in that it
 * uses a hash table instead of a sorted list for linear counting. Although this
 * requires more space and makes hyperloglog (which is less accurate) used sooner,
 * this is also considerably faster.
 *
 * Trying to understand what this class does without having read the paper is
 * considered adventurous.
 */
public final class LogLogBeta implements Releasable {

    public static final int MIN_PRECISION = 4;
    public static final int DEFAULT_PRECISION = 14;
    public static final int MAX_PRECISION = 18;
    private static final boolean LINEAR_COUNTING = false;
    private static final boolean HYPERLOGLOG = true;
    private static final float MAX_LOAD_FACTOR = 0.75f;
    private static final int P2 = 25;

    // these static tables come from the appendix of the paper
    private static final double[][] BETA_FUNCTION_DATA = {
        // precision 4
            { 129.811426122, -127.758849345, -144.856462515, 185.084979526, -13.2281686587, 43.5841078986, -383.603665383, 154.492845304 },
        // precision 5
            { -13.0055889181, 8.58672362771, 9.72695761533, 16.5156287003, -17.0875475369, -4.31703226621, 10.912981826, -3.12448718477 },
        // precision 6
            { 1733.13875391, -1699.65637955, -1001.35164911, -79.5001457157, -232.449115309, 48.0467680133, -13.4033856565,
                    0.0432949807375 },
        // precision 7
            { -683.172241152, 699.316157869, 275.507508944, 219.266866262, -57.9057954518, 44.5955453694, -8.46896092799, 1.1725158865 },
        // precision 8
            { -19.2122824148, 16.5377254144, 12.9159210689, 5.15486460551, -3.55567694845, 2.41367059785, -0.485452949344,
                    0.0512917786702 },
        // precision 9
            { -4.85617520421, 3.35826651543, 2.90853842731, 2.93901916626, -2.37054651785, 1.1737214086, -0.22118210602, 0.0191092511669 },
        // precision 10
            { -3.11898253134, 9.25125002906, -17.8005229174, 21.5341553715, -10.8362087112, 3.00000412385, -0.408463351115,
                    0.0245033071993 },
        // precision 11
            { -0.172965890626, -8.81246455315, 21.0409860425, -16.7375649792, 6.44544077588, -1.30921425783, 0.136002575029,
                    -0.0058234826948 },
        // precision 12
            { -0.356378277813, 3.24074126277, -5.90931639379, 4.23324241571, -1.3182929368, 0.208792006071, -0.0152184183956,
                    0.000471786845185 },
        // precision 13
            { -0.382200101569, 1.80366843702, -2.96538207991, 2.36112694627, -0.822043918775, 0.158042001067, -0.0150086424267,
                    0.000708114274487 },
        // precision 14
            { -3.70393914146161e-01, 7.04718232678681e-02, 1.73936855679645e-01, 1.63398393221669e-01, -9.23774466279541e-02,
                    3.73802699931568e-02, -5.38415897770915e-03, 4.24187633936774e-04 },
            // { -0.495436847353, 14.6271048157, -33.1189427811, 25.6242143788,
            // -9.09805289784, 1.69007364635, -0.158878353733, 0.00629567981401
            // },
        // precision 15
            { -0.560387006169, 59.8108631214, -120.370073477, 86.0699330472, -28.9537963009, 5.03900955483, -0.439967193352,
                    0.0157440364892 },
        // precision 16
            { -0.391416234743, 1.85229689725, -8.882746972, 7.48086624254, -2.80472962045, 0.568918604145, -0.0583909163033,
                    0.00261029795878 },
        // precision 17
            { -0.339120524001, -72.1994426957, 113.185471625, -62.8282169476, 16.6562758098, -2.26144354617, 0.150939847827,
                    -0.0036642817302 },
        // precision 18
            { -0.372494978401, 39.9302213478, -69.8219564407, 43.7971215279, -13.1312309526, 2.0820456299, -0.1696126329, 0.00591592212173 },
            null, null, null, null, null, null,
        // precision 25
            { -0.483989632298, 10736579.3179, -61547.7057585, -101132.984054, 3981.598267, -147.235195282, 15.4398702925,
                    -0.378594684543 }
    };

    /**
     * Compute the required precision so that <code>count</code> distinct
     * entries would be counted with linear counting.
     */
    public static int precisionFromThreshold(long count) {
        final long hashTableEntries = (long) Math.ceil(count / MAX_LOAD_FACTOR);
        int precision = PackedInts.bitsRequired(hashTableEntries * Integer.BYTES);
        precision = Math.max(precision, MIN_PRECISION);
        precision = Math.min(precision, MAX_PRECISION);
        return precision;
    }

    /**
     * Return the expected per-bucket memory usage for the given precision.
     */
    public static long memoryUsage(int precision) {
        return 1L << precision;
    }

    private final BigArrays bigArrays;
    private final OpenBitSet algorithm;
    private ByteArray runLens;
    private final Hashset hashSet;
    private final int p, m;
    private final double alphaM;

    public LogLogBeta(int precision, BigArrays bigArrays, long initialBucketCount) {
        this(precision, bigArrays, initialBucketCount, true);
    }

    public LogLogBeta(int precision, BigArrays bigArrays, long initialBucketCount, boolean useSparseRepresentation) {
        // if (precision < 4) {
        // throw new IllegalArgumentException("precision must be >= 4");
        // }
        // if (precision > 18) {
        // throw new IllegalArgumentException("precision must be <= 18");
        // }
        p = precision;
        m = 1 << p;
        this.bigArrays = bigArrays;
        algorithm = new OpenBitSet();
        runLens = bigArrays.newByteArray(initialBucketCount << p);
        hashSet = new Hashset(initialBucketCount);
        this.alphaM = calculateAlphaM(p, m);
        if (!useSparseRepresentation) {
            for (int i = 0; i < initialBucketCount; i++) {
                upgradeToHll(i);
            }
        }
    }

    private double calculateAlphaM(int p, int m) {
        final double alpha;
        switch (p) {
        case 4:
            alpha = 0.673;
            break;
        case 5:
            alpha = 0.697;
            break;
        default:
            alpha = 0.7213 / (1 + 1.079 / m);
            break;
        }
        return alpha * m;
    }

    public int precision() {
        return p;
    }

    public long maxBucket() {
        return runLens.size() >>> p;
    }

    private void ensureCapacity(long numBuckets) {
        runLens = bigArrays.grow(runLens, numBuckets << p);
    }

    public void merge(long thisBucket, LogLogBeta other, long otherBucket) {
        if (p != other.p) {
            throw new IllegalArgumentException();
        }
        ensureCapacity(thisBucket + 1);
        if (other.algorithm.get(otherBucket) == LINEAR_COUNTING) {
            final IntArray values = other.hashSet.values(otherBucket);
            try {
                for (long i = 0; i < values.size(); ++i) {
                    final int encoded = values.get(i);
                    if (algorithm.get(thisBucket) == LINEAR_COUNTING) {
                        collectLcEncoded(thisBucket, encoded);
                    } else {
                        collectHllEncoded(thisBucket, encoded);
                    }
                }
            } finally {
                Releasables.close(values);
            }
        } else {
            if (algorithm.get(thisBucket) != HYPERLOGLOG) {
                upgradeToHll(thisBucket);
            }
            final long thisStart = thisBucket << p;
            final long otherStart = otherBucket << p;
            for (int i = 0; i < m; ++i) {
                runLens.set(thisStart + i, (byte) Math.max(runLens.get(thisStart + i), other.runLens.get(otherStart + i)));
            }
        }
    }

    public void collect(long bucket, long hash) {
        ensureCapacity(bucket + 1);
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            collectLc(bucket, hash);
        } else {
            collectHll(bucket, hash);
        }
    }

    private void collectLc(long bucket, long hash) {
        final int k = encodeHash(hash, p);
        collectLcEncoded(bucket, k);
    }

    private void collectLcEncoded(long bucket, int encoded) {
        final int newSize = hashSet.add(bucket, encoded);
        if (newSize > hashSet.threshold) {
            upgradeToHll(bucket);
        }
    }

    private void collectHll(long bucket, long hash) {
        final long index = index(hash, p);
        final int runLen = runLen(hash, p);
        collectHll(bucket, index, runLen);
    }

    private void collectHllEncoded(long bucket, int encoded) {
        final int runLen = decodeRunLen(encoded, p);
        final int index = decodeIndex(encoded, p);
        collectHll(bucket, index, runLen);
    }

    private void collectHll(long bucket, long index, int runLen) {
        final long bucketIndex = (bucket << p) + index;
        runLens.set(bucketIndex, (byte) Math.max(runLen, runLens.get(bucketIndex)));
    }

    public long cardinality(long bucket) {
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            return cardinalityHllSparse(bucket);
        } else {
            return cardinalityHll(bucket);
        }
    }

    private long cardinalityLc(long bucket) {
        final long m = 1 << P2;
        final long v = m - hashSet.size(bucket);
        return linearCounting(m, v);
    }

    private long cardinalityHll(long bucket) {
        // E = ( a(m) * m * (m-z) ) / ( B(m,z) + Sum[0,m-1](2^-M[i]))
        double inverseSum = 0;
        int z = 0;
        for (long i = bucket << p, end = i + m; i < end; ++i) {
            final int runLen = runLens.get(i);
            inverseSum += 1. / (1L << runLen);
            if (runLen == 0) {
                ++z;
            }
        }
        double e1 = (alphaM * (m - z)) / (calculateBeta(p, z) + inverseSum);
        long h = (long) e1;
        return h;
    }

    private long cardinalityHllSparse(long bucket) {
        // E = ( a(m) * m * (m-z) ) / ( B(m,z) + Sum[0,m-1](2^-M[i]))
        int m2 = 1 << P2;
        double inverseSum = 0;
        int z = m2 - hashSet.size(bucket);
        final IntArray values = hashSet.values(bucket);
        for (long i = 0; i < values.size(); i++) {
            final int runLen = values.get(i);
            inverseSum += 1. / (1L << runLen);
        }
        inverseSum += z;
        double alphaM2 = calculateAlphaM(P2, m2);
        double e1 = (alphaM2 * (m2 - z)) / (calculateBeta(P2, z) + inverseSum);
        long h = (long) e1;
        return h;
    }

    public int getZ(long bucket) {
        int z = 0;
        for (long i = bucket << p, end = i + m; i < end; ++i) {
            final int runLen = runLens.get(i);
            if (runLen == 0) {
                ++z;
            }
        }
        return z;
    }

    public double calculateBeta(int p, int z) {
        double[] betaCoefficients = BETA_FUNCTION_DATA[p - MIN_PRECISION];
        double zl = Math.log(z + 1);
        double beta = betaCoefficients[0] * z;
        for (int i = 1; i < betaCoefficients.length; i++) {
            beta += betaCoefficients[i] * Math.pow(zl, i);
        }
        return beta;
    }

    public double calculateIdealBeta(long bucket, long knownCardinality) {
        double inverseSum = 0;
        int z = 0;
        for (long i = bucket << p, end = i + m; i < end; ++i) {
            final int runLen = runLens.get(i);
            inverseSum += 1. / (1L << runLen);
            if (runLen == 0) {
                ++z;
            }
        }
        double idealBeta = (alphaM * (m - z) / knownCardinality) - inverseSum;

        return idealBeta;
    }

    void upgradeToHll(long bucket) {
        ensureCapacity(bucket + 1);
        final IntArray values = hashSet.values(bucket);
        try {
            runLens.fill(bucket << p, (bucket << p) + m, (byte) 0);
            for (long i = 0; i < values.size(); ++i) {
                final int encoded = values.get(i);
                collectHllEncoded(bucket, encoded);
            }
            algorithm.set(bucket);
        } finally {
            Releasables.close(values);
        }
    }

    static long linearCounting(long m, long v) {
        return Math.round(m * Math.log((double) m / v));
    }

    static long mask(int bits) {
        return (1L << bits) - 1;
    }

    /**
     * Encode the hash on 32 bits. The encoded hash cannot be equal to <code>0</code>.
     */
    static int encodeHash(long hash, int p) {
        final long e = hash >>> (64 - P2);
        final long encoded;
        if ((e & mask(P2 - p)) == 0) {
            final int runLen = 1 + Math.min(Long.numberOfLeadingZeros(hash << P2), 64 - P2);
            encoded = (e << 7) | (runLen << 1) | 1;
        } else {
            encoded = e << 1;
        }
        assert PackedInts.bitsRequired(encoded) <= 32;
        assert encoded != 0;
        return (int) encoded;
    }

    static int decodeRunLen(int encoded, int p) {
        if ((encoded & 1) == 1) {
            return (((encoded >>> 1) & 0x3F) + (P2 - p));
        } else {
            final int bits = encoded << (31 + p - P2);
            assert bits != 0;
            return 1 + Integer.numberOfLeadingZeros(bits);
        }
    }

    static int decodeIndex(int encoded, int p) {
        long index;
        if ((encoded & 1) == 1) {
            index = encoded >>> 7;
        } else {
            index = encoded >>> 1;
        }
        return (int) (index >>> (P2 - p));
    }

    static long index(long hash, int p) {
        return hash >>> (64 - p);
    }

    static int runLen(long hash, int p) {
        return 1 + Math.min(Long.numberOfLeadingZeros(hash << p), 64 - p);
    }

    @Override
    public void close() {
        Releasables.close(runLens, hashSet.sizes);
    }

    /**
     * We are actually using HyperLogLog's runLens array but interpreting it as a hash set
     * for linear counting.
     */
    private class Hashset {

        private final int capacity;
        private final int threshold;
        private final int mask;
        private IntArray sizes;
        private final BytesRef readSpare;
        private final ByteBuffer writeSpare;

        public Hashset(long initialBucketCount) {
            capacity = m / 4; // because ints take 4 bytes
            threshold = (int) (capacity * MAX_LOAD_FACTOR);
            mask = capacity - 1;
            sizes = bigArrays.newIntArray(initialBucketCount);
            readSpare = new BytesRef();
            writeSpare = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        }

        private long index (long bucket, int index) {
            return (bucket << p) + (index << 2);
        }

        private int get(long bucket, int index) {
            runLens.get(index(bucket, index), 4, readSpare);
            return ByteUtils.readIntLE(readSpare.bytes, readSpare.offset);
        }

        private void set(long bucket, int index, int value) {
            writeSpare.putInt(0, value);
            runLens.set(index(bucket, index), writeSpare.array(), 0, 4);
        }

        private int recomputedSize(long bucket) {
            int size = 0;
            for (int i = 0; i <= mask; ++i) {
                final int v = get(bucket, i);
                if (v != 0) {
                    ++size;
                }
            }
            return size;
        }

        public int size(long bucket) {
            if (bucket >= sizes.size()) {
                return 0;
            }
            final int size = sizes.get(bucket);
            assert size == recomputedSize(bucket);
            return size;
        }

        /**
         * Add <code>k</code> to the hash table associated with <code>bucket</code>.
         * Return <tt>-1</tt> if the value was already in the set or the new set size if it was added.
         */
        public int add(long bucket, int k) {
            sizes = bigArrays.grow(sizes, bucket + 1);
            assert k != 0;
            for (int i = (k & mask); ; i = (i + 1) & mask) {
                final int v = get(bucket, i);
                if (v == 0) {
                    // means unused, take it!
                    set(bucket, i, k);
                    return sizes.increment(bucket, 1);
                } else if (v == k) {
                    // k is already in the set
                    return -1;
                }
            }
        }

        public IntArray values(final long bucket) {
            final int size = size(bucket);
            final IntArray values = bigArrays.newIntArray(size);
            if (size == 0) {
                return values;
            }
            int i = 0;
            for (int j = 0; j < capacity; ++j) {
                final int k = get(bucket, j);
                if (k != 0) {
                    values.set(i++, k);
                }
            }
            assert i == values.size();
            return values;
        }

    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(p);
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            out.writeBoolean(LINEAR_COUNTING);
            try (IntArray hashes = hashSet.values(bucket)) {
                out.writeVLong(hashes.size());
                for (long i = 0; i < hashes.size(); ++i) {
                    out.writeInt(hashes.get(i));
                }
            }
        } else {
            out.writeBoolean(HYPERLOGLOG);
            for (long i = bucket << p, end = i + m; i < end; ++i) {
                out.writeByte(runLens.get(i));
            }
        }
    }

    public static LogLogBeta readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        final int precision = in.readVInt();
        LogLogBeta counts = new LogLogBeta(precision, bigArrays, 1);
        final boolean algorithm = in.readBoolean();
        if (algorithm == LINEAR_COUNTING) {
            counts.algorithm.clear(0);
            final long size = in.readVLong();
            for (long i = 0; i < size; ++i) {
                final int encoded = in.readInt();
                counts.hashSet.add(0, encoded);
            }
        } else {
            counts.algorithm.set(0);
            for (int i = 0; i < counts.m; ++i) {
                counts.runLens.set(i, in.readByte());
            }
        }
        return counts;
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
