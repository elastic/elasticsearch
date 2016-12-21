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

import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;

import java.io.IOException;

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
public final class HyperLogLogBeta implements Releasable {

    public static final int MIN_PRECISION = 4;
    public static final int DEFAULT_PRECISION = 14;
    public static final int MAX_PRECISION = 18;
    private static final float MAX_LOAD_FACTOR = 0.75f;
    private static final int P2 = 25;

    // these static tables come from the appendix of the paper
    private static final double[][] BETA_FUNCTION_DATA = {
        // precision 4
        {},
        // precision 5
        {},
        // precision 6
        {},
        // precision 7
        {},
        // precision 8
        {},
        // precision 9
        {},
        // precision 10
        {},
        // precision 11
        {},
        // precision 12
        {},
        // precision 13
        {},
        // precision 14
            { -0.370393911, 0.070471823, 0.17393686, 0.16339839, -0.09237745, 0.03738027, -0.005384159, 0.00042419 },
        // precision 15
        {},
        // precision 16
        {},
        // precision 17
        {},
        // precision 18
        {}
    };

    /**
     * Compute the required precision so that <code>count</code> distinct entries
     * would be counted with linear counting.
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
    private final int p, m;
    private final double alphaM;

    public HyperLogLogBeta(int precision, BigArrays bigArrays, long initialBucketCount) {
        if (precision < 4) {
            throw new IllegalArgumentException("precision must be >= 4");
        }
        if (precision > 18) {
            throw new IllegalArgumentException("precision must be <= 18");
        }
        p = precision;
        m = 1 << p;
        this.bigArrays = bigArrays;
        algorithm = new OpenBitSet();
        runLens = bigArrays.newByteArray(initialBucketCount << p);
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
        alphaM = alpha * m;
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

    public void merge(long thisBucket, HyperLogLogBeta other, long otherBucket) {
        if (p != other.p) {
            throw new IllegalArgumentException();
        }
        ensureCapacity(thisBucket + 1);
        final long thisStart = thisBucket << p;
        final long otherStart = otherBucket << p;
        for (int i = 0; i < m; ++i) {
            runLens.set(thisStart + i, (byte) Math.max(runLens.get(thisStart + i), other.runLens.get(otherStart + i)));
        }
    }

    public void collect(long bucket, long hash) {
        ensureCapacity(bucket + 1);
        collectHll(bucket, hash);
    }

    private void collectHll(long bucket, long hash) {
        final long index = index(hash, p);
        final int runLen = runLen(hash, p);
        collectHll(bucket, index, runLen);
    }

    private void collectHll(long bucket, long index, int runLen) {
        final long bucketIndex = (bucket << p) + index;
        runLens.set(bucketIndex, (byte) Math.max(runLen, runLens.get(bucketIndex)));
    }

    public long cardinality(long bucket) {
        return cardinalityHll(bucket);
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
        long h = Math.round(e1);
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

    private double calculateBeta(int p, int z) {
        double[] betaCoefficients = BETA_FUNCTION_DATA[p - MIN_PRECISION];
        double zl = Math.log(z + 1);
        double beta = betaCoefficients[0] * z;
        for (int i = 1; i < betaCoefficients.length; i++) {
            beta += betaCoefficients[i] * Math.pow(zl, i);
        }
        return beta;
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
        Releasables.close(runLens);
    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(p);
        for (long i = bucket << p, end = i + m; i < end; ++i) {
            out.writeByte(runLens.get(i));
        }
    }

    public static HyperLogLogBeta readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        final int precision = in.readVInt();
        HyperLogLogBeta counts = new HyperLogLogBeta(precision, bigArrays, 1);
        counts.algorithm.set(0);
        for (int i = 0; i < counts.m; ++i) {
            counts.runLens.set(i, in.readByte());
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
