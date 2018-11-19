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
package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A bloom filter. Inspired by Guava bloom filter implementation though with some optimizations.
 */
public class BloomFilter implements Writeable, Releasable {

    // Some numbers:
    // 10k =0.001: 140.4kb  , 10 Hashes
    // 10k =0.01 :  93.6kb  ,  6 Hashes
    // 100k=0.01 : 936.0kb  ,  6 Hashes
    // 100k=0.03 : 712.7kb  ,  5 Hashes
    // 500k=0.01 :   4.5mb  ,  6 Hashes
    // 500k=0.03 :   3.4mb  ,  5 Hashes
    // 500k=0.05 :   2.9mb  ,  4 Hashes
    //   1m=0.01 :   9.1mb  ,  6 Hashes
    //   1m=0.03 :   6.9mb  ,  5 Hashes
    //   1m=0.05 :   5.9mb  ,  4 Hashes
    //   5m=0.01 :  45.7mb  ,  6 Hashes
    //   5m=0.03 :  34.8mb  ,  5 Hashes
    //   5m=0.05 :  29.7mb  ,  4 Hashes
    //  50m=0.01 : 457.0mb  ,  6 Hashes
    //  50m=0.03 : 297.3mb  ,  4 Hashes
    //  50m=0.10 : 228.5mb  ,  3 Hashes

    /**
     * The bit set of the BloomFilter (not necessarily power of 2!)
     */
    private final BitArray bits;

    /**
     * Number of hashes per element
     */
    private final int numHashFunctions;

    private final Hashing hashing = Hashing.V1;

    /**
     * Creates a bloom filter based on the with the expected number
     * of insertions and expected false positive probability.
     *
     * @param expectedInsertions the number of expected insertions to the constructed
     * @param fpp                the desired false positive probability (must be positive and less than 1.0)
     */
    public BloomFilter(int expectedInsertions, double fpp) {
        this(expectedInsertions, fpp, -1);
    }

    /**
     * Creates a bloom filter based on the expected number of insertions, expected false positive probability,
     * and number of hash functions.
     *
     * @param expectedInsertions the number of expected insertions to the constructed
     * @param fpp                the desired false positive probability (must be positive and less than 1.0)
     * @param numHashFunctions   the number of hash functions to use (must be less than or equal to 255)
     */
    public BloomFilter(int expectedInsertions, double fpp, int numHashFunctions) {
        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }
        /*
         * TODO(user): Put a warning in the javadoc about tiny fpp values,
         * since the resulting size is proportional to -log(p), but there is not
         * much of a point after all, e.g. optimalM(1000, 0.0000000000000001) = 76680
         * which is less that 10kb. Who cares!
         */
        long numBits = optimalNumOfBits(expectedInsertions, fpp);

        // calculate the optimal number of hash functions
        if (numHashFunctions == -1) {
            numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        }

        if (numHashFunctions > 255) {
            throw new IllegalArgumentException("BloomFilters with more than 255 hash functions are not allowed.");
        }

        this.bits = new BitArray(numBits);
        this.numHashFunctions = numHashFunctions;
    }

    public BloomFilter(StreamInput in) throws IOException {
        int numLongs = in.readVInt();
        long[] data = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            data[i] = in.readLong();
        }
        this.numHashFunctions = in.readVInt();
        this.bits = new BitArray(data);
    }

    public void merge(BloomFilter other) {
        this.bits.putAll(other.bits);
    }

    public boolean put(BytesRef value) {
        return hashing.put(value, numHashFunctions, bits);
    }

    public boolean put(byte[] value) {
        return hashing.put(value, 0, value.length, numHashFunctions, bits);
    }

    public boolean put(long value) {
        return put(Numbers.longToBytes(value));
    }

    public boolean mightContain(BytesRef value) {
        return hashing.mightContain(value, numHashFunctions, bits);
    }

    public boolean mightContain(byte[] value) {
        return hashing.mightContain(value, 0, value.length, numHashFunctions, bits);
    }

    public boolean mightContain(long value) {
        return mightContain(Numbers.longToBytes(value));
    }

    public int getNumHashFunctions() {
        return this.numHashFunctions;
    }

    public long getSizeInBytes() {
        return bits.ramBytesUsed();
    }

    @Override
    public int hashCode() {
        return bits.hashCode() + numHashFunctions;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final BloomFilter that = (BloomFilter) other;
        return Objects.equals(this.bits, that.bits)
            && Objects.equals(this.hashing, that.hashing)
            && Objects.equals(this.numHashFunctions, that.numHashFunctions);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bits.data.length);
        for (long l : bits.data) {
            out.writeLong(l);
        }
        out.writeVInt(numHashFunctions);
    }

    @Override
    public void close() {

    }

    /*
     * Cheat sheet:
     *
     * m: total bits
     * n: expected insertions
     * b: m/n, bits per insertion

     * p: expected false positive probability
     *
     * 1) Optimal k = b * ln2
     * 2) p = (1 - e ^ (-kn/m))^k
     * 3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
     * 4) For optimal k: m = -nlnp / ((ln2) ^ 2)
     */

    /**
     * Computes the optimal k (number of hashes per element inserted in Bloom filter), given the
     * expected insertions and total number of bits in the Bloom filter.
     * <p>
     * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param m total number of bits in Bloom filter (must be positive)
     */
    private static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round(m / n * Math.log(2)));
    }

    /**
     * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
     * expected insertions, the required false positive probability.
     * <p>
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param p false positive rate (must be 0 &lt; p &lt; 1)
     */
    private static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    // Note: We use this instead of java.util.BitSet because we need access to the long[] data field
    static final class BitArray {
        final long[] data;
        final long bitSize;
        long bitCount;

        BitArray(long bits) {
            this(new long[size(bits)]);
        }

        private static int size(long bits) {
            long quotient = bits / 64;
            long remainder = bits - quotient * 64;
            return Math.toIntExact(remainder == 0 ? quotient : 1 + quotient);
        }

        // Used by serialization
        BitArray(long[] data) {
            this.data = data;
            long bitCount = 0;
            for (long value : data) {
                bitCount += Long.bitCount(value);
            }
            this.bitCount = bitCount;
            this.bitSize = data.length * Long.SIZE;
        }

        /**
         * Returns true if the bit changed value.
         */
        boolean set(long index) {
            if (!get(index)) {
                data[(int) (index >>> 6)] |= (1L << index);
                bitCount++;
                return true;
            }
            return false;
        }

        boolean get(long index) {
            return (data[(int) (index >>> 6)] & (1L << index)) != 0;
        }

        /**
         * Number of bits
         */
        long bitSize() {
            return bitSize;
        }

        /**
         * Number of set bits (1s)
         */
        long bitCount() {
            return bitCount;
        }

        BitArray copy() {
            return new BitArray(data.clone());
        }

        /**
         * Combines the two BitArrays using bitwise OR.
         */
        void putAll(BitArray array) {
            bitCount = 0;
            for (int i = 0; i < data.length; i++) {
                data[i] |= array.data[i];
                bitCount += Long.bitCount(data[i]);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof BitArray) {
                BitArray bitArray = (BitArray) o;
                return Arrays.equals(data, bitArray.data);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }


        public long ramBytesUsed() {
            return Long.BYTES * data.length + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 16;
        }
    }

    enum Hashing {
        V1() {
            @Override
            protected boolean put(byte[] bytes, int offset, int length, int numHashFunctions, BitArray bits) {
                long bitSize = bits.bitSize();
                MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(bytes, offset, length, 0, new MurmurHash3.Hash128());

                boolean bitsChanged = false;
                long combinedHash = hash128.h1;
                for (int i = 0; i < numHashFunctions; i++) {
                    // Make the combined hash positive and indexable
                    bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
                    combinedHash += hash128.h2;
                }
                return bitsChanged;
            }

            @Override
            protected boolean mightContain(byte[] bytes, int offset, int length, int numHashFunctions, BitArray bits) {
                long bitSize = bits.bitSize();
                MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(bytes, offset, length, 0, new MurmurHash3.Hash128());

                long combinedHash = hash128.h1;
                for (int i = 0; i < numHashFunctions; i++) {
                    // Make the combined hash positive and indexable
                    if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
                        return false;
                    }
                    combinedHash += hash128.h2;
                }
                return true;
            }

            @Override
            protected int type() {
                return 1;
            }
        };

        protected boolean put(BytesRef value, int numHashFunctions, BitArray bits) {
            return put(value.bytes, value.offset, value.length, numHashFunctions, bits);
        }

        protected abstract boolean put(byte[] bytes, int offset, int length, int numHashFunctions, BitArray bits);

        protected boolean mightContain(BytesRef value, int numHashFunctions, BitArray bits) {
            return mightContain(value.bytes, value.offset, value.length, numHashFunctions, bits);
        }

        protected abstract boolean mightContain(byte[] bytes, int offset, int length, int numHashFunctions, BitArray bits);

        protected abstract int type();
    }
}
