/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * A class used to represent a set of many, potentially large, values (e.g. many long strings such
 * as URLs), using a significantly smaller amount of memory.
 *
 * <p>The set is "lossy" in that it cannot definitively state that is does contain a value but it
 * <em>can</em> definitively say if a value is <em>not</em> in the set. It can therefore be used as
 * a Bloom Filter. Another application of the set is that it can be used to perform fuzzy counting
 * because it can estimate reasonably accurately how many unique values are contained in the set.
 *
 * <p>This class is NOT threadsafe.
 *
 * <p>Internally a Bitset is used to record values and once a client has finished recording a stream
 * of values the {@link #downsize(float)} method can be used to create a suitably smaller set that
 * is sized appropriately for the number of values recorded and desired saturation levels.
 *
 * @lucene.experimental
 */
public class FuzzySet implements Accountable {

    public static final int VERSION_SPI = 1; // HashFunction used to be loaded through a SPI
    public static final int VERSION_START = VERSION_SPI;
    public static final int VERSION_CURRENT = 2;

    public static HashFunction hashFunctionForVersion(int version) {
        if (version < VERSION_START) {
            throw new IllegalArgumentException("Version " + version + " is too old, expected at least " + VERSION_START);
        } else if (version > VERSION_CURRENT) {
            throw new IllegalArgumentException("Version " + version + " is too new, expected at most " + VERSION_CURRENT);
        }
        return MurmurHash2.INSTANCE;
    }

    /**
     * Result from {@link FuzzySet#contains(BytesRef)}: can never return definitively YES (always
     * MAYBE), but can sometimes definitely return NO.
     */
    public enum ContainsResult {
        MAYBE,
        NO
    };

    private HashFunction hashFunction;
    private FixedBitSet filter;
    private int bloomSize;

    // The sizes of BitSet used are all numbers that, when expressed in binary form,
    // are all ones. This is to enable fast downsizing from one bitset to another
    // by simply ANDing each set index in one bitset with the size of the target bitset
    // - this provides a fast modulo of the number. Values previously accumulated in
    // a large bitset and then mapped to a smaller set can be looked up using a single
    // AND operation of the query term's hash rather than needing to perform a 2-step
    // translation of the query term that mirrors the stored content's reprojections.
    static final int[] usableBitSetSizes;

    static {
        usableBitSetSizes = new int[30];
        int mask = 1;
        int size = mask;
        for (int i = 0; i < usableBitSetSizes.length; i++) {
            size = (size << 1) | mask;
            usableBitSetSizes[i] = size;
        }
    }

    /**
     * Rounds down required maxNumberOfBits to the nearest number that is made up of all ones as a
     * binary number. Use this method where controlling memory use is paramount.
     */
    public static int getNearestSetSize(int maxNumberOfBits) {
        int result = usableBitSetSizes[0];
        for (int i = 0; i < usableBitSetSizes.length; i++) {
            if (usableBitSetSizes[i] <= maxNumberOfBits) {
                result = usableBitSetSizes[i];
            }
        }
        return result;
    }

    /**
     * Use this method to choose a set size where accuracy (low content saturation) is more important
     * than deciding how much memory to throw at the problem.
     *
     * @param desiredSaturation A number between 0 and 1 expressing the % of bits set once all values
     *     have been recorded
     * @return The size of the set nearest to the required size
     */
    public static int getNearestSetSize(int maxNumberOfValuesExpected, float desiredSaturation) {
        // Iterate around the various scales of bitset from smallest to largest looking for the first
        // that
        // satisfies value volumes at the chosen saturation level
        for (int i = 0; i < usableBitSetSizes.length; i++) {
            int numSetBitsAtDesiredSaturation = (int) (usableBitSetSizes[i] * desiredSaturation);
            int estimatedNumUniqueValues = getEstimatedNumberUniqueValuesAllowingForCollisions(
                usableBitSetSizes[i],
                numSetBitsAtDesiredSaturation
            );
            if (estimatedNumUniqueValues > maxNumberOfValuesExpected) {
                return usableBitSetSizes[i];
            }
        }
        return -1;
    }

    public static FuzzySet createSetBasedOnMaxMemory(int maxNumBytes) {
        int setSize = getNearestSetSize(maxNumBytes);
        return new FuzzySet(new FixedBitSet(setSize + 1), setSize, hashFunctionForVersion(VERSION_CURRENT));
    }

    public static FuzzySet createSetBasedOnQuality(int maxNumUniqueValues, float desiredMaxSaturation) {
        int setSize = getNearestSetSize(maxNumUniqueValues, desiredMaxSaturation);
        return new FuzzySet(new FixedBitSet(setSize + 1), setSize, hashFunctionForVersion(VERSION_CURRENT));
    }

    private FuzzySet(FixedBitSet filter, int bloomSize, HashFunction hashFunction) {
        super();
        this.filter = filter;
        this.bloomSize = bloomSize;
        this.hashFunction = hashFunction;
    }

    /**
     * The main method required for a Bloom filter which, given a value determines set membership.
     * Unlike a conventional set, the fuzzy set returns NO or MAYBE rather than true or false.
     *
     * @return NO or MAYBE
     */
    public ContainsResult contains(BytesRef value) {
        int hash = hashFunction.hash(value);
        if (hash < 0) {
            hash = hash * -1;
        }
        return mayContainValue(hash);
    }

    /**
     * Serializes the data set to file using the following format:
     *
     * <ul>
     *   <li>FuzzySet --&gt;FuzzySetVersion,HashFunctionName,BloomSize,
     *       NumBitSetWords,BitSetWord<sup>NumBitSetWords</sup>
     *   <li>HashFunctionName --&gt; {@link DataOutput#writeString(String) String} The name of a
     *       ServiceProvider registered {@link HashFunction}
     *   <li>FuzzySetVersion --&gt; {@link DataOutput#writeInt Uint32} The version number of the
     *       {@link FuzzySet} class
     *   <li>BloomSize --&gt; {@link DataOutput#writeInt Uint32} The modulo value used to project
     *       hashes into the field's Bitset
     *   <li>NumBitSetWords --&gt; {@link DataOutput#writeInt Uint32} The number of longs (as returned
     *       from {@link FixedBitSet#getBits})
     *   <li>BitSetWord --&gt; {@link DataOutput#writeLong Long} A long from the array returned by
     *       {@link FixedBitSet#getBits}
     * </ul>
     *
     * @param out Data output stream
     * @throws IOException If there is a low-level I/O error
     */
    public void serialize(DataOutput out) throws IOException {
        out.writeInt(VERSION_CURRENT);
        out.writeInt(bloomSize);
        long[] bits = filter.getBits();
        out.writeInt(bits.length);
        for (int i = 0; i < bits.length; i++) {
            // Can't used VLong encoding because cant cope with negative numbers
            // output by FixedBitSet
            out.writeLong(bits[i]);
        }
    }

    public static FuzzySet deserialize(DataInput in) throws IOException {
        int version = in.readInt();
        if (version == VERSION_SPI) {
            in.readString();
        }
        final HashFunction hashFunction = hashFunctionForVersion(version);
        int bloomSize = in.readInt();
        int numLongs = in.readInt();
        long[] longs = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            longs[i] = in.readLong();
        }
        FixedBitSet bits = new FixedBitSet(longs, bloomSize + 1);
        return new FuzzySet(bits, bloomSize, hashFunction);
    }

    private ContainsResult mayContainValue(int positiveHash) {
        assert positiveHash >= 0;
        // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
        int pos = positiveHash & bloomSize;
        if (filter.get(pos)) {
            // This term may be recorded in this index (but could be a collision)
            return ContainsResult.MAYBE;
        }
        // definitely NOT in this segment
        return ContainsResult.NO;
    }

    /**
     * Records a value in the set. The referenced bytes are hashed and then modulo n'd where n is the
     * chosen size of the internal bitset.
     *
     * @param value the key value to be hashed
     * @throws IOException If there is a low-level I/O error
     */
    public void addValue(BytesRef value) throws IOException {
        int hash = hashFunction.hash(value);
        if (hash < 0) {
            hash = hash * -1;
        }
        // Bitmasking using bloomSize is effectively a modulo operation.
        int bloomPos = hash & bloomSize;
        filter.set(bloomPos);
    }

    /**
     * @param targetMaxSaturation A number between 0 and 1 describing the % of bits that would ideally
     *     be set in the result. Lower values have better accuracy but require more space.
     * @return a smaller FuzzySet or null if the current set is already over-saturated
     */
    public FuzzySet downsize(float targetMaxSaturation) {
        int numBitsSet = filter.cardinality();
        FixedBitSet rightSizedBitSet = filter;
        int rightSizedBitSetSize = bloomSize;
        // Hopefully find a smaller size bitset into which we can project accumulated values while
        // maintaining desired saturation level
        for (int i = 0; i < usableBitSetSizes.length; i++) {
            int candidateBitsetSize = usableBitSetSizes[i];
            float candidateSaturation = (float) numBitsSet / (float) candidateBitsetSize;
            if (candidateSaturation <= targetMaxSaturation) {
                rightSizedBitSetSize = candidateBitsetSize;
                break;
            }
        }
        // Re-project the numbers to a smaller space if necessary
        if (rightSizedBitSetSize < bloomSize) {
            // Reset the choice of bitset to the smaller version
            rightSizedBitSet = new FixedBitSet(rightSizedBitSetSize + 1);
            // Map across the bits from the large set to the smaller one
            int bitIndex = 0;
            do {
                bitIndex = filter.nextSetBit(bitIndex);
                if (bitIndex != DocIdSetIterator.NO_MORE_DOCS) {
                    // Project the larger number into a smaller one effectively
                    // modulo-ing by using the target bitset size as a mask
                    int downSizedBitIndex = bitIndex & rightSizedBitSetSize;
                    rightSizedBitSet.set(downSizedBitIndex);
                    bitIndex++;
                }
            } while ((bitIndex >= 0) && (bitIndex <= bloomSize));
        } else {
            return null;
        }
        return new FuzzySet(rightSizedBitSet, rightSizedBitSetSize, hashFunction);
    }

    public int getEstimatedUniqueValues() {
        return getEstimatedNumberUniqueValuesAllowingForCollisions(bloomSize, filter.cardinality());
    }

    // Given a set size and a the number of set bits, produces an estimate of the number of unique
    // values recorded
    public static int getEstimatedNumberUniqueValuesAllowingForCollisions(int setSize, int numRecordedBits) {
        double setSizeAsDouble = setSize;
        double numRecordedBitsAsDouble = numRecordedBits;
        double saturation = numRecordedBitsAsDouble / setSizeAsDouble;
        double logInverseSaturation = Math.log(1 - saturation) * -1;
        return (int) (setSizeAsDouble * logInverseSaturation);
    }

    public float getSaturation() {
        int numBitsSet = filter.cardinality();
        return (float) numBitsSet / (float) bloomSize;
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.sizeOf(filter.getBits());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(hash=" + hashFunction + ")";
    }
}
