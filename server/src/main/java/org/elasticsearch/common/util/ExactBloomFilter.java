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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A bloom filter which keeps an exact set of values until a threshold is reached, then the values
 * are replayed into a traditional bloom filter for approximate tracking
 */
public class ExactBloomFilter implements Writeable {

    // Some anecdotal sizing numbers:
    // expected insertions, false positive probability, bloom size, num hashes
    //  10k, 0.001, 140.4kb, 10 Hashes
    //  10k,  0.01,  93.6kb,  6 Hashes
    // 100k,  0.01, 936.0kb,  6 Hashes
    // 100k,  0.03, 712.7kb,  5 Hashes
    // 500k,  0.01,   4.5mb,  6 Hashes
    // 500k,  0.03,   3.4mb,  5 Hashes
    // 500k,  0.05,   2.9mb,  4 Hashes
    //   1m,  0.01,   9.1mb,  6 Hashes
    //   1m,  0.03,   6.9mb,  5 Hashes
    //   1m,  0.05,   5.9mb,  4 Hashes
    //   5m,  0.01,  45.7mb,  6 Hashes
    //   5m,  0.03,  34.8mb,  5 Hashes
    //   5m,  0.05,  29.7mb,  4 Hashes
    //  50m,  0.01, 457.0mb,  6 Hashes
    //  50m,  0.03, 297.3mb,  4 Hashes
    //  50m,  0.10, 228.5mb,  3 Hashes

    /**
     * The bit set of the ExactBloomFilter (not necessarily power of 2!)
     */
    BitArray bits;
    Set<MurmurHash3.Hash128> hashedValues = new HashSet<>();

    /**
     * Number of hashes per element
     */
    private final int numHashFunctions;

    /**
     * The number of bits in the bloom
     */
    private long numBits;

    /**
     * The threshold (in bytes) before we convert the exact set into an approximate bloom filter
     */
    private final long threshold;

    /**
     * True if we are still tracking with a Set
     */
    private boolean setMode = true;

    /**
     * Creates a bloom filter with the expected number
     * of insertions and expected false positive probability.
     *
     * @param expectedInsertions the number of expected insertions to the constructed
     * @param fpp                the desired false positive probability (must be positive and less than 1.0)
     * @param threshold          number of bytes to record exactly before converting to Bloom filter
     */
    public ExactBloomFilter(int expectedInsertions, double fpp, long threshold) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("BloomFilter threshold must be a non-negative number");
        }

        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }
        this.threshold = threshold;
        /*
         * TODO(user): Put a warning in the javadoc about tiny fpp values,
         * since the resulting size is proportional to -log(p), but there is not
         * much of a point after all, e.g. optimalNumOfBits(1000, 0.0000000000000001) = 76680
         * which is less that 10kb. Who cares!
         */
        this.numBits = optimalNumOfBits(expectedInsertions, fpp);

        // calculate the optimal number of hash functions
        this.numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        if (numHashFunctions > 255) {
            throw new IllegalArgumentException("BloomFilters with more than 255 hash functions are not allowed.");
        }
    }

    /**
     * Copy constructor.  The new Bloom will be an identical copy of the provided bloom
     */
    public ExactBloomFilter(ExactBloomFilter otherBloom) {
        this.numHashFunctions = otherBloom.getNumHashFunctions();
        this.threshold = otherBloom.getThreshold();
        this.numBits = otherBloom.getNumBits();
        this.setMode = otherBloom.setMode;
        this.hashedValues = new HashSet<>(otherBloom.hashedValues);
        if (otherBloom.bits != null) {
            this.bits = new BitArray(otherBloom.numBits);
            this.bits.putAll(otherBloom.bits);
        }
    }

    public ExactBloomFilter(StreamInput in) throws IOException {
        this.setMode = in.readBoolean();
        if (setMode) {
            this.hashedValues = in.readSet(in1 -> {
                MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                hash.h1 = in1.readLong();
                hash.h2 = in1.readLong();
                return hash;
            });
        } else {
            this.bits = new BitArray(in);
        }
        this.numHashFunctions = in.readVInt();
        this.threshold = in.readVLong();
        this.numBits = in.readVLong();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(setMode);
        if (setMode) {
            out.writeCollection(hashedValues, (out1, hash) -> {
                out1.writeLong(hash.h1);
                out1.writeLong(hash.h2);
            });
        } else {
            bits.writeTo(out);
        }
        out.writeVInt(numHashFunctions);
        out.writeVLong(threshold);
        out.writeVLong(numBits);
    }

    /**
     * Merge `other` bloom filter into this bloom.  After merging, this bloom's state will
     * be the union of the two.  During the merging process, the internal Set may be upgraded
     * to a Bloom if it goes over threshold
     */
    public void merge(ExactBloomFilter other) {
        assert this.numBits == other.numBits;
        if (setMode && other.setMode) {
            // Both in sets, merge collections then see if we need to convert to bloom
            hashedValues.addAll(other.hashedValues);
            checkAndConvertToBloom();
        } else if (setMode && other.setMode == false) {
            // Other is in bloom mode, so we convert our set to a bloom then merge
            convertToBloom();
            this.bits.putAll(other.bits);
        } else if (setMode == false && other.setMode) {
            // we're in bloom mode, so convert other's set and merge
            other.convertToBloom();
            this.bits.putAll(other.bits);
        } else {
            this.bits.putAll(other.bits);
        }
    }

    public boolean put(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, new MurmurHash3.Hash128());
        return put(hash);
    }

    public boolean put(byte[] value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value, 0, value.length, 0, new MurmurHash3.Hash128());
        return put(hash);
    }

    public boolean put(long value) {
        return put(Numbers.longToBytes(value));
    }

    private boolean put(MurmurHash3.Hash128 hash) {
        if (setMode) {
            boolean newItem = hashedValues.add(hash);
            checkAndConvertToBloom();
            return newItem;
        } else {
            return putBloom(hash);
        }
    }

    private boolean putBloom(MurmurHash3.Hash128 hash128) {
        long bitSize = bits.bitSize();
        boolean bitsChanged = false;
        long combinedHash = hash128.h1;
        for (int i = 0; i < numHashFunctions; i++) {
            // Make the combined hash positive and indexable
            bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
            combinedHash += hash128.h2;
        }
        return bitsChanged;
    }

    public boolean mightContain(BytesRef value) {
        return mightContain(value.bytes, value.offset, value.length);
    }

    public boolean mightContain(byte[] value) {
        return mightContain(value, 0, value.length);
    }

    public boolean mightContain(long value) {
        return mightContain(Numbers.longToBytes(value));
    }

    private boolean mightContain(byte[] bytes, int offset, int length) {
        MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(bytes, offset, length, 0, new MurmurHash3.Hash128());

        if (setMode) {
            return hashedValues.contains(hash128);
        } else {
            long bitSize = bits.bitSize();
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
    }

    private int getNumHashFunctions() {
        return this.numHashFunctions;
    }

    private long getNumBits() {
        return numBits;
    }

    public long getThreshold() {
        return threshold;
    }

    /**
     * Get the approximate size of this datastructure.  Approximate because only the Set occupants
     * are tracked, not the overhead of the Set itself.
     */
    public long getSizeInBytes() {
        long bytes = (hashedValues.size() * 16) + 8 + 4 + 1;
        if (bits != null) {
            bytes += bits.ramBytesUsed();
        }
        return bytes;
    }

    private void checkAndConvertToBloom() {
        if (hashedValues.size() * 16 > threshold) {
            convertToBloom();
        }
    }

    private void convertToBloom() {
        bits = new BitArray(numBits);
        setMode = false;
        for (MurmurHash3.Hash128 hash : hashedValues) {
            putBloom(hash);
        }
        hashedValues.clear();
    }

    @Override
    public int hashCode() {
        return Objects.hash(numHashFunctions, hashedValues, bits, setMode, threshold, numBits);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final ExactBloomFilter that = (ExactBloomFilter) other;
        return Objects.equals(this.bits, that.bits)
            && Objects.equals(this.numHashFunctions, that.numHashFunctions)
            && Objects.equals(this.threshold, that.threshold)
            && Objects.equals(this.setMode, that.setMode)
            && Objects.equals(this.hashedValues, that.hashedValues)
            && Objects.equals(this.numBits, that.numBits);
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

    static final class BitArray implements Writeable {
        private final long[] data;
        private final long bitSize;
        private long bitCount;

        BitArray(long bits) {
            this.data = new long[size(bits)];
            long bitCount = 0;
            for (long value : data) {
                bitCount += Long.bitCount(value);
            }
            this.bitCount = bitCount;
            this.bitSize = data.length * Long.SIZE;
        }

        private static int size(long bits) {
            long quotient = bits / 64;
            long remainder = bits - quotient * 64;
            return Math.toIntExact(remainder == 0 ? quotient : 1 + quotient);
        }

        BitArray(StreamInput in) throws IOException {
            this.data = in.readVLongArray();
            this.bitSize = in.readVLong();
            this.bitCount = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLongArray(data);
            out.writeVLong(bitSize);
            out.writeVLong(bitCount);
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

}
