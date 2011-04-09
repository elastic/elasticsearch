/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.RamUsage;

import java.io.IOException;

/**
 * Derived from {@link org.apache.lucene.util.OpenBitSet} but works from a slice out of a provided long[] array.
 * It does not expand, as it assumes that the slice is from a cached long[] array, so we can't really expand...
 */
public class SlicedOpenBitSet extends DocSet {

    private final long[] bits;
    private final int wlen;   // number of words (elements) used in the array
    private final int from; // the from index in the array

    public SlicedOpenBitSet(long[] bits, int from, OpenBitSet setToCopy) {
        this.bits = bits;
        this.from = from;
        System.arraycopy(setToCopy.getBits(), 0, bits, from, setToCopy.getBits().length);
        this.wlen = setToCopy.getNumWords();
    }

    public SlicedOpenBitSet(long[] bits, int wlen, int from) {
        this.bits = bits;
        this.wlen = wlen;
        this.from = from;
    }

    @Override public boolean isCacheable() {
        return true;
    }

    @Override public long sizeInBytes() {
        return wlen * RamUsage.NUM_BYTES_LONG + RamUsage.NUM_BYTES_ARRAY_HEADER + RamUsage.NUM_BYTES_INT /* wlen */;
    }

    /**
     * Returns the current capacity in bits (1 greater than the index of the last bit)
     */
    public long capacity() {
        return (bits.length - from) << 6;
    }

    /**
     * Returns the current capacity of this set.  Included for
     * compatibility.  This is *not* equal to {@link #cardinality}
     */
    public long size() {
        return capacity();
    }

    /**
     * @return the number of set bits
     */
    public long cardinality() {
        return BitUtil.pop_array(bits, from, wlen);
    }

    /**
     * Returns true or false for the specified bit index.
     */
    public boolean get(int index) {
        return fastGet(index);
//        int i = index >> 6;               // div 64
//        // signed shift will keep a negative index and force an
//        // array-index-out-of-bounds-exception, removing the need for an explicit check.
//        if (from + i >= wlen) return false;
//
//        int bit = index & 0x3f;           // mod 64
//        long bitmask = 1L << bit;
//        return (bits[from + i] & bitmask) != 0;
    }

    /**
     * Returns true or false for the specified bit index.
     * The index should be less than the OpenBitSet size
     */
    public boolean fastGet(int index) {
        int i = index >> 6;               // div 64
        // signed shift will keep a negative index and force an
        // array-index-out-of-bounds-exception, removing the need for an explicit check.
        int bit = index & 0x3f;           // mod 64
        long bitmask = 1L << bit;
        return (bits[from + i] & bitmask) != 0;
    }


    /**
     * Returns true or false for the specified bit index
     */
    public boolean get(long index) {
        int i = (int) (index >> 6);             // div 64
        if (from + i >= wlen) return false;
        int bit = (int) index & 0x3f;           // mod 64
        long bitmask = 1L << bit;
        return (bits[from + i] & bitmask) != 0;
    }

    /**
     * Returns true or false for the specified bit index.
     * The index should be less than the OpenBitSet size.
     */
    public boolean fastGet(long index) {
        int i = (int) (index >> 6);               // div 64
        int bit = (int) index & 0x3f;           // mod 64
        long bitmask = 1L << bit;
        return (bits[from + i] & bitmask) != 0;
    }

    /**
     * Sets the bit at the specified index.
     * The index should be less than the OpenBitSet size.
     */
    public void fastSet(int index) {
        int wordNum = index >> 6;      // div 64
        int bit = index & 0x3f;     // mod 64
        long bitmask = 1L << bit;
        bits[from + wordNum] |= bitmask;
    }

    /**
     * Sets the bit at the specified index.
     * The index should be less than the OpenBitSet size.
     */
    public void fastSet(long index) {
        int wordNum = (int) (index >> 6);
        int bit = (int) index & 0x3f;
        long bitmask = 1L << bit;
        bits[from + wordNum] |= bitmask;
    }

    @Override public DocIdSetIterator iterator() throws IOException {
        return new SlicedIterator(this);
    }


    /**
     * An iterator to iterate over set bits in an OpenBitSet.
     * This is faster than nextSetBit() for iterating over the complete set of bits,
     * especially when the density of the bits set is high.
     */
    public static class SlicedIterator extends DocIdSetIterator {

        // The General Idea: instead of having an array per byte that has
        // the offsets of the next set bit, that array could be
        // packed inside a 32 bit integer (8 4 bit numbers).  That
        // should be faster than accessing an array for each index, and
        // the total array size is kept smaller (256*sizeof(int))=1K
        protected final static int[] bitlist = {
                0x0, 0x1, 0x2, 0x21, 0x3, 0x31, 0x32, 0x321, 0x4, 0x41, 0x42, 0x421, 0x43,
                0x431, 0x432, 0x4321, 0x5, 0x51, 0x52, 0x521, 0x53, 0x531, 0x532, 0x5321,
                0x54, 0x541, 0x542, 0x5421, 0x543, 0x5431, 0x5432, 0x54321, 0x6, 0x61, 0x62,
                0x621, 0x63, 0x631, 0x632, 0x6321, 0x64, 0x641, 0x642, 0x6421, 0x643,
                0x6431, 0x6432, 0x64321, 0x65, 0x651, 0x652, 0x6521, 0x653, 0x6531, 0x6532,
                0x65321, 0x654, 0x6541, 0x6542, 0x65421, 0x6543, 0x65431, 0x65432, 0x654321,
                0x7, 0x71, 0x72, 0x721, 0x73, 0x731, 0x732, 0x7321, 0x74, 0x741, 0x742,
                0x7421, 0x743, 0x7431, 0x7432, 0x74321, 0x75, 0x751, 0x752, 0x7521, 0x753,
                0x7531, 0x7532, 0x75321, 0x754, 0x7541, 0x7542, 0x75421, 0x7543, 0x75431,
                0x75432, 0x754321, 0x76, 0x761, 0x762, 0x7621, 0x763, 0x7631, 0x7632,
                0x76321, 0x764, 0x7641, 0x7642, 0x76421, 0x7643, 0x76431, 0x76432, 0x764321,
                0x765, 0x7651, 0x7652, 0x76521, 0x7653, 0x76531, 0x76532, 0x765321, 0x7654,
                0x76541, 0x76542, 0x765421, 0x76543, 0x765431, 0x765432, 0x7654321, 0x8,
                0x81, 0x82, 0x821, 0x83, 0x831, 0x832, 0x8321, 0x84, 0x841, 0x842, 0x8421,
                0x843, 0x8431, 0x8432, 0x84321, 0x85, 0x851, 0x852, 0x8521, 0x853, 0x8531,
                0x8532, 0x85321, 0x854, 0x8541, 0x8542, 0x85421, 0x8543, 0x85431, 0x85432,
                0x854321, 0x86, 0x861, 0x862, 0x8621, 0x863, 0x8631, 0x8632, 0x86321, 0x864,
                0x8641, 0x8642, 0x86421, 0x8643, 0x86431, 0x86432, 0x864321, 0x865, 0x8651,
                0x8652, 0x86521, 0x8653, 0x86531, 0x86532, 0x865321, 0x8654, 0x86541,
                0x86542, 0x865421, 0x86543, 0x865431, 0x865432, 0x8654321, 0x87, 0x871,
                0x872, 0x8721, 0x873, 0x8731, 0x8732, 0x87321, 0x874, 0x8741, 0x8742,
                0x87421, 0x8743, 0x87431, 0x87432, 0x874321, 0x875, 0x8751, 0x8752, 0x87521,
                0x8753, 0x87531, 0x87532, 0x875321, 0x8754, 0x87541, 0x87542, 0x875421,
                0x87543, 0x875431, 0x875432, 0x8754321, 0x876, 0x8761, 0x8762, 0x87621,
                0x8763, 0x87631, 0x87632, 0x876321, 0x8764, 0x87641, 0x87642, 0x876421,
                0x87643, 0x876431, 0x876432, 0x8764321, 0x8765, 0x87651, 0x87652, 0x876521,
                0x87653, 0x876531, 0x876532, 0x8765321, 0x87654, 0x876541, 0x876542,
                0x8765421, 0x876543, 0x8765431, 0x8765432, 0x87654321
        };
        /**
         * ** the python code that generated bitlist
         * def bits2int(val):
         * arr=0
         * for shift in range(8,0,-1):
         * if val & 0x80:
         * arr = (arr << 4) | shift
         * val = val << 1
         * return arr
         *
         * def int_table():
         * tbl = [ hex(bits2int(val)).strip('L') for val in range(256) ]
         * return ','.join(tbl)
         * ****
         */

        // hmmm, what about an iterator that finds zeros though,
        // or a reverse iterator... should they be separate classes
        // for efficiency, or have a common root interface?  (or
        // maybe both?  could ask for a SetBitsIterator, etc...

        private final long[] arr;
        private final int words;
        private final int from;
        private int i = -1;
        private long word;
        private int wordShift;
        private int indexArray;
        private int curDocId = -1;

        public SlicedIterator(SlicedOpenBitSet obs) {
            this.arr = obs.bits;
            this.words = obs.wlen;
            this.from = obs.from;
        }

        // 64 bit shifts
        private void shift() {
            if ((int) word == 0) {
                wordShift += 32;
                word = word >>> 32;
            }
            if ((word & 0x0000FFFF) == 0) {
                wordShift += 16;
                word >>>= 16;
            }
            if ((word & 0x000000FF) == 0) {
                wordShift += 8;
                word >>>= 8;
            }
            indexArray = bitlist[(int) word & 0xff];
        }

        /**
         * ** alternate shift implementations
         * // 32 bit shifts, but a long shift needed at the end
         * private void shift2() {
         * int y = (int)word;
         * if (y==0) {wordShift +=32; y = (int)(word >>>32); }
         * if ((y & 0x0000FFFF) == 0) { wordShift +=16; y>>>=16; }
         * if ((y & 0x000000FF) == 0) { wordShift +=8; y>>>=8; }
         * indexArray = bitlist[y & 0xff];
         * word >>>= (wordShift +1);
         * }
         *
         * private void shift3() {
         * int lower = (int)word;
         * int lowByte = lower & 0xff;
         * if (lowByte != 0) {
         * indexArray=bitlist[lowByte];
         * return;
         * }
         * shift();
         * }
         * ****
         */

        @Override
        public int nextDoc() {
            if (indexArray == 0) {
                if (word != 0) {
                    word >>>= 8;
                    wordShift += 8;
                }

                while (word == 0) {
                    if (++i >= words) {
                        return curDocId = NO_MORE_DOCS;
                    }
                    word = arr[from + i];
                    wordShift = -1; // loop invariant code motion should move this
                }

                // after the first time, should I go with a linear search, or
                // stick with the binary search in shift?
                shift();
            }

            int bitIndex = (indexArray & 0x0f) + wordShift;
            indexArray >>>= 4;
            // should i<<6 be cached as a separate variable?
            // it would only save one cycle in the best circumstances.
            return curDocId = (i << 6) + bitIndex;
        }

        @Override
        public int advance(int target) {
            indexArray = 0;
            i = target >> 6;
            if (i >= words) {
                word = 0; // setup so next() will also return -1
                return curDocId = NO_MORE_DOCS;
            }
            wordShift = target & 0x3f;
            word = arr[from + i] >>> wordShift;
            if (word != 0) {
                wordShift--; // compensate for 1 based arrIndex
            } else {
                while (word == 0) {
                    if (++i >= words) {
                        return curDocId = NO_MORE_DOCS;
                    }
                    word = arr[from + i];
                }
                wordShift = -1;
            }

            shift();

            int bitIndex = (indexArray & 0x0f) + wordShift;
            indexArray >>>= 4;
            // should i<<6 be cached as a separate variable?
            // it would only save one cycle in the best circumstances.
            return curDocId = (i << 6) + bitIndex;
        }

        @Override
        public int docID() {
            return curDocId;
        }

    }
}