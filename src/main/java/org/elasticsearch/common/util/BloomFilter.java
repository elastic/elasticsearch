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

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.SizeValue;

import java.io.IOException;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Comparator;

/**
 * A bloom filter. Inspired by Guava bloom filter implementation though with some optimizations.
 */
public class BloomFilter {

    /**
     * A factory that can use different fpp based on size.
     */
    public static class Factory {

        public static final Factory DEFAULT = buildDefault();

        private static Factory buildDefault() {
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
            return buildFromString("10k=0.01,1m=0.03");
        }

        /**
         * Supports just passing fpp, as in "0.01", and also ranges, like "50k=0.01,1m=0.05". If
         * its null, returns {@link #buildDefault()}.
         */
        public static Factory buildFromString(@Nullable String config) {
            if (config == null) {
                return buildDefault();
            }
            String[] sEntries = Strings.splitStringToArray(config, ',');
            if (sEntries.length == 0) {
                if (config.length() > 0) {
                    return new Factory(new Entry[]{new Entry(0, Double.parseDouble(config))});
                }
                return buildDefault();
            }
            Entry[] entries = new Entry[sEntries.length];
            for (int i = 0; i < sEntries.length; i++) {
                int index = sEntries[i].indexOf('=');
                entries[i] = new Entry(
                        (int) SizeValue.parseSizeValue(sEntries[i].substring(0, index).trim()).singles(),
                        Double.parseDouble(sEntries[i].substring(index + 1).trim())
                );
            }
            return new Factory(entries);
        }

        private final Entry[] entries;

        public Factory(Entry[] entries) {
            this.entries = entries;
            // the order is from the upper most expected insertions to the lowest
            Arrays.sort(this.entries, new Comparator<Entry>() {
                @Override
                public int compare(Entry o1, Entry o2) {
                    return o2.expectedInsertions - o1.expectedInsertions;
                }
            });
        }

        public BloomFilter createFilter(int expectedInsertions) {
            for (Entry entry : entries) {
                if (expectedInsertions > entry.expectedInsertions) {
                    return BloomFilter.create(expectedInsertions, entry.fpp);
                }
            }
            return BloomFilter.create(expectedInsertions, 0.03);
        }

        public static class Entry {
            public final int expectedInsertions;
            public final double fpp;

            Entry(int expectedInsertions, double fpp) {
                this.expectedInsertions = expectedInsertions;
                this.fpp = fpp;
            }
        }
    }

    /**
     * Creates a bloom filter based on the with the expected number
     * of insertions and expected false positive probability.
     *
     * @param expectedInsertions the number of expected insertions to the constructed
     * @param fpp                the desired false positive probability (must be positive and less than 1.0)
     */
    public static BloomFilter create(int expectedInsertions, double fpp) {
        return create(expectedInsertions, fpp, -1);
    }

    /**
     * Creates a bloom filter based on the expected number of insertions, expected false positive probability,
     * and number of hash functions.
     *
     * @param expectedInsertions the number of expected insertions to the constructed
     * @param fpp                the desired false positive probability (must be positive and less than 1.0)
     * @param numHashFunctions   the number of hash functions to use (must be less than or equal to 255)
     */
    public static BloomFilter create(int expectedInsertions, double fpp, int numHashFunctions) {
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

        try {
            return new BloomFilter(new BitArray(numBits), numHashFunctions, Hashing.DEFAULT);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " bits", e);
        }
    }

    public static void skipBloom(IndexInput in) throws IOException {
        int version = in.readInt(); // we do nothing with this now..., defaults to 0
        final int numLongs = in.readInt();
        in.seek(in.getFilePointer() + (numLongs * 8) + 4 + 4); // filter + numberOfHashFunctions + hashType
    }

    public static BloomFilter deserialize(DataInput in) throws IOException {
        int version = in.readInt(); // we do nothing with this now..., defaults to 0
        int numLongs = in.readInt();
        long[] data = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            data[i] = in.readLong();
        }
        int numberOfHashFunctions = in.readInt();
        int hashType = in.readInt();
        return new BloomFilter(new BitArray(data), numberOfHashFunctions, Hashing.fromType(hashType));
    }

    public static void serilaize(BloomFilter filter, DataOutput out) throws IOException {
        out.writeInt(0); // version
        BitArray bits = filter.bits;
        out.writeInt(bits.data.length);
        for (long l : bits.data) {
            out.writeLong(l);
        }
        out.writeInt(filter.numHashFunctions);
        out.writeInt(filter.hashing.type()); // hashType
    }

    public static BloomFilter readFrom(StreamInput in) throws IOException {
        int version = in.readVInt(); // we do nothing with this now..., defaults to 0
        int numLongs = in.readVInt();
        long[] data = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            data[i] = in.readLong();
        }
        int numberOfHashFunctions = in.readVInt();
        int hashType = in.readVInt(); // again, nothing to do now...
        return new BloomFilter(new BitArray(data), numberOfHashFunctions, Hashing.fromType(hashType));
    }

    public static void writeTo(BloomFilter filter, StreamOutput out) throws IOException {
        out.writeVInt(0); // version
        BitArray bits = filter.bits;
        out.writeVInt(bits.data.length);
        for (long l : bits.data) {
            out.writeLong(l);
        }
        out.writeVInt(filter.numHashFunctions);
        out.writeVInt(filter.hashing.type()); // hashType
    }

    /**
     * The bit set of the BloomFilter (not necessarily power of 2!)
     */
    final BitArray bits;
    /**
     * Number of hashes per element
     */
    final int numHashFunctions;

    final Hashing hashing;

    BloomFilter(BitArray bits, int numHashFunctions, Hashing hashing) {
        this.bits = bits;
        this.numHashFunctions = numHashFunctions;
        this.hashing = hashing;
    /*
     * This only exists to forbid BFs that cannot use the compact persistent representation.
     * If it ever throws, at a user who was not intending to use that representation, we should
     * reconsider
     */
        if (numHashFunctions > 255) {
            throw new IllegalArgumentException("Currently we don't allow BloomFilters that would use more than 255 hash functions");
        }
    }

    public boolean put(BytesRef value) {
        return hashing.put(value, numHashFunctions, bits);
    }

    public boolean mightContain(BytesRef value) {
        return hashing.mightContain(value, numHashFunctions, bits);
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
     * <p/>
     * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param m total number of bits in Bloom filter (must be positive)
     */
    static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round(m / n * Math.log(2)));
    }

    /**
     * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
     * expected insertions, the required false positive probability.
     * <p/>
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param p false positive rate (must be 0 < p < 1)
     */
    static long optimalNumOfBits(long n, double p) {
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
            this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))]);
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

        /** Returns true if the bit changed value. */
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

        /** Number of bits */
        long bitSize() {
            return bitSize;
        }

        /** Number of set bits (1s) */
        long bitCount() {
            return bitCount;
        }

        BitArray copy() {
            return new BitArray(data.clone());
        }

        /** Combines the two BitArrays using bitwise OR. */
        void putAll(BitArray array) {
            bitCount = 0;
            for (int i = 0; i < data.length; i++) {
                data[i] |= array.data[i];
                bitCount += Long.bitCount(data[i]);
            }
        }

        @Override public boolean equals(Object o) {
            if (o instanceof BitArray) {
                BitArray bitArray = (BitArray) o;
                return Arrays.equals(data, bitArray.data);
            }
            return false;
        }

        @Override public int hashCode() {
            return Arrays.hashCode(data);
        }

        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_LONG * data.length + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 16;
        }
    }

    static enum Hashing {

        V0() {
            @Override
            protected boolean put(BytesRef value, int numHashFunctions, BitArray bits) {
                long bitSize = bits.bitSize();
                long hash64 = hash3_x64_128(value.bytes, value.offset, value.length, 0);
                int hash1 = (int) hash64;
                int hash2 = (int) (hash64 >>> 32);
                boolean bitsChanged = false;
                for (int i = 1; i <= numHashFunctions; i++) {
                    int nextHash = hash1 + i * hash2;
                    if (nextHash < 0) {
                        nextHash = ~nextHash;
                    }
                    bitsChanged |= bits.set(nextHash % bitSize);
                }
                return bitsChanged;
            }

            @Override
            protected boolean mightContain(BytesRef value, int numHashFunctions, BitArray bits) {
                long bitSize = bits.bitSize();
                long hash64 = hash3_x64_128(value.bytes, value.offset, value.length, 0);
                int hash1 = (int) hash64;
                int hash2 = (int) (hash64 >>> 32);
                for (int i = 1; i <= numHashFunctions; i++) {
                    int nextHash = hash1 + i * hash2;
                    if (nextHash < 0) {
                        nextHash = ~nextHash;
                    }
                    if (!bits.get(nextHash % bitSize)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            protected int type() {
                return 0;
            }
        },
        V1() {
            @Override
            protected boolean put(BytesRef value, int numHashFunctions, BitArray bits) {
                long bitSize = bits.bitSize();
                MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, new MurmurHash3.Hash128());

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
            protected boolean mightContain(BytesRef value, int numHashFunctions, BitArray bits) {
                long bitSize = bits.bitSize();
                MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, new MurmurHash3.Hash128());

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
        }
        ;

        protected abstract boolean put(BytesRef value, int numHashFunctions, BitArray bits);

        protected abstract boolean mightContain(BytesRef value, int numHashFunctions, BitArray bits);

        protected abstract int type();

        public static final Hashing DEFAULT = Hashing.V1;

        public static Hashing fromType(int type) {
            if (type == 0) {
                return Hashing.V0;
            } if (type == 1) {
                return Hashing.V1;
            } else {
                throw new IllegalArgumentException("no hashing type matching " + type);
            }
        }
    }

    // START : MURMUR 3_128 USED FOR Hashing.V0
    // NOTE: don't replace this code with the o.e.common.hashing.MurmurHash3 method which returns a different hash

    protected static long getblock(byte[] key, int offset, int index) {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((long) key[blockOffset + 0] & 0xff) + (((long) key[blockOffset + 1] & 0xff) << 8) +
                (((long) key[blockOffset + 2] & 0xff) << 16) + (((long) key[blockOffset + 3] & 0xff) << 24) +
                (((long) key[blockOffset + 4] & 0xff) << 32) + (((long) key[blockOffset + 5] & 0xff) << 40) +
                (((long) key[blockOffset + 6] & 0xff) << 48) + (((long) key[blockOffset + 7] & 0xff) << 56);
    }

    protected static long rotl64(long v, int n) {
        return ((v << n) | (v >>> (64 - n)));
    }

    protected static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }

    public static long hash3_x64_128(byte[] key, int offset, int length, long seed) {
        final int nblocks = length >> 4; // Process as 128-bit blocks.

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for (int i = 0; i < nblocks; i++) {
            long k1 = getblock(key, offset, i * 2 + 0);
            long k2 = getblock(key, offset, i * 2 + 1);

            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= ((long) key[offset + 14]) << 48;
            case 14:
                k2 ^= ((long) key[offset + 13]) << 40;
            case 13:
                k2 ^= ((long) key[offset + 12]) << 32;
            case 12:
                k2 ^= ((long) key[offset + 11]) << 24;
            case 11:
                k2 ^= ((long) key[offset + 10]) << 16;
            case 10:
                k2 ^= ((long) key[offset + 9]) << 8;
            case 9:
                k2 ^= ((long) key[offset + 8]) << 0;
                k2 *= c2;
                k2 = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) key[offset + 7]) << 56;
            case 7:
                k1 ^= ((long) key[offset + 6]) << 48;
            case 6:
                k1 ^= ((long) key[offset + 5]) << 40;
            case 5:
                k1 ^= ((long) key[offset + 4]) << 32;
            case 4:
                k1 ^= ((long) key[offset + 3]) << 24;
            case 3:
                k1 ^= ((long) key[offset + 2]) << 16;
            case 2:
                k1 ^= ((long) key[offset + 1]) << 8;
            case 1:
                k1 ^= ((long) key[offset]);
                k1 *= c1;
                k1 = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
        }

        //----------
        // finalization

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        //return (new long[]{h1, h2});
        // SAME AS GUAVA, they take the first long out of the 128bit
        return h1;
    }

    // END: MURMUR 3_128
}
