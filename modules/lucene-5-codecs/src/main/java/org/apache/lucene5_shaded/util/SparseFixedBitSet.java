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
package org.apache.lucene5_shaded.util;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene5_shaded.search.DocIdSetIterator;

/**
 * A bit set that only stores longs that have at least one bit which is set.
 * The way it works is that the space of bits is divided into blocks of
 * 4096 bits, which is 64 longs. Then for each block, we have:<ul>
 * <li>a long[] which stores the non-zero longs for that block</li>
 * <li>a long so that bit <tt>i</tt> being set means that the <code>i-th</code>
 *     long of the block is non-null, and its offset in the array of longs is
 *     the number of one bits on the right of the <code>i-th</code> bit.</li></ul>
 *
 * @lucene.internal
 */
public class SparseFixedBitSet extends BitSet implements Bits, Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SparseFixedBitSet.class);
  private static final long SINGLE_ELEMENT_ARRAY_BYTES_USED = RamUsageEstimator.sizeOf(new long[1]);
  private static final int MASK_4096 = (1 << 12) - 1;

  private static int blockCount(int length) {
    int blockCount = length >>> 12;
    if ((blockCount << 12) < length) {
      ++blockCount;
    }
    assert (blockCount << 12) >= length;
    return blockCount;
  }

  final long[] indices;
  final long[][] bits;
  final int length;
  int nonZeroLongCount;
  long ramBytesUsed;

  /** Create a {@link SparseFixedBitSet} that can contain bits between
   *  <code>0</code> included and <code>length</code> excluded. */
  public SparseFixedBitSet(int length) {
    if (length < 1) {
      throw new IllegalArgumentException("length needs to be >= 1");
    }
    this.length = length;
    final int blockCount = blockCount(length);
    indices = new long[blockCount];
    bits = new long[blockCount][];
    ramBytesUsed = BASE_RAM_BYTES_USED
        + RamUsageEstimator.shallowSizeOf(indices)
        + RamUsageEstimator.shallowSizeOf(bits);
  }

  @Override
  public int length() {
    return length;
  }

  private boolean consistent(int index) {
    assert index >= 0 && index < length : "index=" + index + ",length=" + length;
    return true;
  }

  @Override
  public int cardinality() {
    int cardinality = 0;
    for (long[] bitArray : bits) {
      if (bitArray != null) {
        for (long bits : bitArray) {
          cardinality += Long.bitCount(bits);
        }
      }
    }
    return cardinality;
  }

  @Override
  public int approximateCardinality() {
    // we are assuming that bits are uniformly set and use the linear counting
    // algorithm to estimate the number of bits that are set based on the number
    // of longs that are different from zero
    final int totalLongs = (length + 63) >>> 6; // total number of longs in the space
    assert totalLongs >= nonZeroLongCount;
    final int zeroLongs = totalLongs - nonZeroLongCount; // number of longs that are zeros
    // No need to guard against division by zero, it will return +Infinity and things will work as expected
    final long estimate = Math.round(totalLongs * Math.log((double) totalLongs / zeroLongs));
    return (int) Math.min(length, estimate);
  }

  @Override
  public boolean get(int i) {
    assert consistent(i);
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    final int i64 = i >>> 6;
    // first check the index, if the i64-th bit is not set, then i is not set
    // note: this relies on the fact that shifts are mod 64 in java
    if ((index & (1L << i64)) == 0) {
      return false;
    }

    // if it is set, then we count the number of bits that are set on the right
    // of i64, and that gives us the index of the long that stores the bits we
    // are interested in
    final long bits = this.bits[i4096][Long.bitCount(index & ((1L << i64) - 1))];
    return (bits & (1L << i)) != 0;
  }

  private static int oversize(int s) {
    int newSize = s + (s >>> 1);
    if (newSize > 50) {
      newSize = 64;
    }
    return newSize;
  }

  /**
   * Set the bit at index <tt>i</tt>.
   */
  public void set(int i) {
    assert consistent(i);
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    final int i64 = i >>> 6;
    if ((index & (1L << i64)) != 0) {
      // in that case the sub 64-bits block we are interested in already exists,
      // we just need to set a bit in an existing long: the number of ones on
      // the right of i64 gives us the index of the long we need to update
      bits[i4096][Long.bitCount(index & ((1L << i64) - 1))] |= 1L << i; // shifts are mod 64 in java
    } else if (index == 0) {
      // if the index is 0, it means that we just found a block of 4096 bits
      // that has no bit that is set yet. So let's initialize a new block:
      insertBlock(i4096, i64, i);
    } else {
      // in that case we found a block of 4096 bits that has some values, but
      // the sub-block of 64 bits that we are interested in has no value yet,
      // so we need to insert a new long
      insertLong(i4096, i64, i, index);
    }
  }

  private void insertBlock(int i4096, int i64, int i) {
    indices[i4096] = 1L << i64; // shifts are mod 64 in java
    assert bits[i4096] == null;
    bits[i4096] = new long[] { 1L << i }; // shifts are mod 64 in java
    ++nonZeroLongCount;
    ramBytesUsed += SINGLE_ELEMENT_ARRAY_BYTES_USED;
  }

  private void insertLong(int i4096, int i64, int i, long index) {
    indices[i4096] |= 1L << i64; // shifts are mod 64 in java
    // we count the number of bits that are set on the right of i64
    // this gives us the index at which to perform the insertion
    final int o = Long.bitCount(index & ((1L << i64) - 1));
    final long[] bitArray = bits[i4096];
    if (bitArray[bitArray.length - 1] == 0) {
      // since we only store non-zero longs, if the last value is 0, it means
      // that we alreay have extra space, make use of it
      System.arraycopy(bitArray, o, bitArray, o + 1, bitArray.length - o - 1);
      bitArray[o] = 1L << i;
    } else {
      // we don't have extra space so we need to resize to insert the new long
      final int newSize = oversize(bitArray.length + 1);
      final long[] newBitArray = new long[newSize];
      System.arraycopy(bitArray, 0, newBitArray, 0, o);
      newBitArray[o] = 1L << i;
      System.arraycopy(bitArray, o, newBitArray, o + 1, bitArray.length - o);
      bits[i4096] = newBitArray;
      ramBytesUsed += RamUsageEstimator.sizeOf(newBitArray) - RamUsageEstimator.sizeOf(bitArray);
    }
    ++nonZeroLongCount;
  }

  /**
   * Clear the bit at index <tt>i</tt>.
   */
  public void clear(int i) {
    assert consistent(i);
    final int i4096 = i >>> 12;
    final int i64 = i >>> 6;
    and(i4096, i64, ~(1L << i));
  }

  private void and(int i4096, int i64, long mask) {
    final long index = indices[i4096];
    if ((index & (1L << i64)) != 0) {
      // offset of the long bits we are interested in in the array
      final int o = Long.bitCount(index & ((1L << i64) - 1));
      long bits = this.bits[i4096][o] & mask;
      if (bits == 0) {
        removeLong(i4096, i64, index, o);
      } else {
        this.bits[i4096][o] = bits;
      }
    }
  }

  private void removeLong(int i4096, int i64, long index, int o) {
    index &= ~(1L << i64);
    indices[i4096] = index;
    if (index == 0) {
      // release memory, there is nothing in this block anymore
      this.bits[i4096] = null;
    } else {
      final int length = Long.bitCount(index);
      final long[] bitArray = bits[i4096];
      System.arraycopy(bitArray, o + 1, bitArray, o, length - o);
      bitArray[length] = 0L;
    }
    nonZeroLongCount -= 1;
  }

  @Override
  public void clear(int from, int to) {
    assert from >= 0;
    assert to <= length;
    if (from >= to) {
      return;
    }
    final int firstBlock = from >>> 12;
    final int lastBlock = (to - 1) >>> 12;
    if (firstBlock == lastBlock) {
      clearWithinBlock(firstBlock, from & MASK_4096, (to - 1) & MASK_4096);
    } else {
      clearWithinBlock(firstBlock, from & MASK_4096, MASK_4096);
      for (int i = firstBlock + 1; i < lastBlock; ++i) {
        nonZeroLongCount -= Long.bitCount(indices[i]);
        indices[i] = 0;
        bits[i] = null;
      }
      clearWithinBlock(lastBlock, 0, (to - 1) & MASK_4096);
    }
  }

  // create a long that has bits set to one between from and to
  private static long mask(int from, int to) {
    return ((1L << (to - from) << 1) - 1) << from;
  }

  private void clearWithinBlock(int i4096, int from, int to) {
    int firstLong = from >>> 6;
    int lastLong = to >>> 6;

    if (firstLong == lastLong) {
      and(i4096, firstLong, ~mask(from, to));
    } else {
      assert firstLong < lastLong;
      and(i4096, lastLong, ~mask(0, to));
      for (int i = lastLong - 1; i >= firstLong + 1; --i) {
        and(i4096, i, 0L);
      }
      and(i4096, firstLong, ~mask(from, 63));
    }
  }

  /** Return the first document that occurs on or after the provided block index. */
  private int firstDoc(int i4096) {
    long index = 0;
    while (i4096 < indices.length) {
      index = indices[i4096];
      if (index != 0) {
        final int i64 = Long.numberOfTrailingZeros(index);
        return (i4096 << 12) | (i64 << 6) | Long.numberOfTrailingZeros(bits[i4096][0]);
      }
      i4096 += 1;
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int nextSetBit(int i) {
    assert i < length;
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    final long[] bitArray = this.bits[i4096];
    int i64 = i >>> 6;
    int o = Long.bitCount(index & ((1L << i64) - 1));
    if ((index & (1L << i64)) != 0) {
      // There is at least one bit that is set in the current long, check if
      // one of them is after i
      final long bits = bitArray[o] >>> i; // shifts are mod 64
      if (bits != 0) {
        return i + Long.numberOfTrailingZeros(bits);
      }
      o += 1;
    }
    final long indexBits = index >>> i64 >>> 1;
    if (indexBits == 0) {
      // no more bits are set in the current block of 4096 bits, go to the next one
      return firstDoc(i4096 + 1);
    }
    // there are still set bits
    i64 += 1 + Long.numberOfTrailingZeros(indexBits);
    final long bits = bitArray[o];
    return (i64 << 6) | Long.numberOfTrailingZeros(bits);
  }

  /** Return the last document that occurs on or before the provided block index. */
  private int lastDoc(int i4096) {
    long index;
    while (i4096 >= 0) {
      index = indices[i4096];
      if (index != 0) {
        final int i64 = 63 - Long.numberOfLeadingZeros(index);
        final long bits = this.bits[i4096][Long.bitCount(index) - 1];
        return (i4096 << 12) | (i64 << 6) | (63 - Long.numberOfLeadingZeros(bits));
      }
      i4096 -= 1;
    }
    return -1;
  }

  @Override
  public int prevSetBit(int i) {
    assert i >= 0;
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    final long[] bitArray = this.bits[i4096];
    int i64 = i >>> 6;
    final long indexBits = index & ((1L << i64) - 1);
    final int o = Long.bitCount(indexBits);
    if ((index & (1L << i64)) != 0) {
      // There is at least one bit that is set in the same long, check if there
      // is one bit that is set that is lower than i
      final long bits = bitArray[o] & ((1L << i << 1) - 1);
      if (bits != 0) {
        return (i64 << 6) | (63 - Long.numberOfLeadingZeros(bits));
      }
    }
    if (indexBits == 0) {
      // no more bits are set in this block, go find the last bit in the
      // previous block
      return lastDoc(i4096 - 1);
    }
    // go to the previous long
    i64 = 63 - Long.numberOfLeadingZeros(indexBits);
    final long bits = bitArray[o - 1];
    return (i4096 << 12) | (i64 << 6) | (63 - Long.numberOfLeadingZeros(bits));
  }

  /** Return the long bits at the given <code>i64</code> index. */
  private long longBits(long index, long[] bits, int i64) {
    if ((index & (1L << i64)) == 0) {
      return 0L;
    } else {
      return bits[Long.bitCount(index & ((1L << i64) - 1))];
    }
  }

  private void or(final int i4096, final long index, long[] bits, int nonZeroLongCount) {
    assert Long.bitCount(index) == nonZeroLongCount;
    final long currentIndex = indices[i4096];
    if (currentIndex == 0) {
      // fast path: if we currently have nothing in the block, just copy the data
      // this especially happens all the time if you call OR on an empty set
      indices[i4096] = index;
      this.bits[i4096] = Arrays.copyOf(bits, nonZeroLongCount);
      this.nonZeroLongCount += nonZeroLongCount;
      return;
    }
    final long[] currentBits = this.bits[i4096];
    final long[] newBits;
    final long newIndex = currentIndex | index;
    final int requiredCapacity = Long.bitCount(newIndex);
    if (currentBits.length >= requiredCapacity) {
      newBits = currentBits;
    } else {
      newBits = new long[oversize(requiredCapacity)];
    }
    // we iterate backwards in order to not override data we might need on the next iteration if the
    // array is reused
    for (int i = Long.numberOfLeadingZeros(newIndex), newO = Long.bitCount(newIndex) - 1;
        i < 64;
        i += 1 + Long.numberOfLeadingZeros(newIndex << (i + 1)), newO -= 1) {
      // bitIndex is the index of a bit which is set in newIndex and newO is the number of 1 bits on its right
      final int bitIndex = 63 - i;
      assert newO == Long.bitCount(newIndex & ((1L << bitIndex) - 1));
      newBits[newO] = longBits(currentIndex, currentBits, bitIndex) | longBits(index, bits, bitIndex);
    }
    indices[i4096] = newIndex;
    this.bits[i4096] = newBits;
    this.nonZeroLongCount += nonZeroLongCount - Long.bitCount(currentIndex & index);
  }

  private void or(SparseFixedBitSet other) {
    for (int i = 0; i < other.indices.length; ++i) {
      final long index = other.indices[i];
      if (index != 0) {
        or(i, index, other.bits[i], Long.bitCount(index));
      }
    }
  }

  /**
   * {@link #or(DocIdSetIterator)} impl that works best when <code>it</code> is dense
   */
  private void orDense(DocIdSetIterator it) throws IOException {
    assertUnpositioned(it);
    // The goal here is to try to take advantage of the ordering of documents
    // to build the data-structure more efficiently
    // NOTE: this heavily relies on the fact that shifts are mod 64
    final int firstDoc = it.nextDoc();
    if (firstDoc == DocIdSetIterator.NO_MORE_DOCS) {
      return;
    }
    int i4096 = firstDoc >>> 12;
    int i64 = firstDoc >>> 6;
    long index = 1L << i64;
    long currentLong = 1L << firstDoc;
    // we store at most 64 longs per block so preallocate in order never to have to resize
    long[] longs = new long[64];
    int numLongs = 0;

    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      final int doc64 = doc >>> 6;
      if (doc64 == i64) {
        // still in the same long, just set the bit
        currentLong |= 1L << doc;
      } else {
        longs[numLongs++] = currentLong;

        final int doc4096 = doc >>> 12;
        if (doc4096 == i4096) {
          index |= 1L << doc64;
        } else {
          // we are on a new block, flush what we buffered
          or(i4096, index, longs, numLongs);
          // and reset state for the new block
          i4096 = doc4096;
          index = 1L << doc64;
          numLongs = 0;
        }

        // we are on a new long, reset state
        i64 = doc64;
        currentLong = 1L << doc;
      }
    }

    // flush
    longs[numLongs++] = currentLong;
    or(i4096, index, longs, numLongs);
  }

  @Override
  public void or(DocIdSetIterator it) throws IOException {
    {
      // specialize union with another SparseFixedBitSet
      final SparseFixedBitSet other = BitSetIterator.getSparseFixedBitSetOrNull(it);
      if (other != null) {
        assertUnpositioned(it);
        or(other);
        return;
      }
    }

    // We do not specialize the union with a FixedBitSet since FixedBitSets are
    // supposed to be used for dense data and sparse fixed bit sets for sparse
    // data, so a sparse set would likely get upgraded by DocIdSetBuilder before
    // being or'ed with a FixedBitSet

    if (it.cost() < indices.length) {
      // the default impl is good for sparse iterators
      super.or(it);
    } else {
      orDense(it);
    }
  }

  // AND and AND_NOT do not need much specialization here since this sparse set
  // is supposed to be used on sparse data and the default AND/AND_NOT impl
  // (leap frog) is efficient when at least one of the sets contains sparse data

  @Override
  public void and(DocIdSetIterator it) throws IOException {
    final SparseFixedBitSet other = BitSetIterator.getSparseFixedBitSetOrNull(it);
    if (other != null) {
      // if we are merging with another SparseFixedBitSet, a quick win is
      // to clear up some blocks by only looking at their index. Then the set
      // is sparser and the leap-frog approach of the parent class is more
      // efficient. Since SparseFixedBitSet is supposed to be used for sparse
      // sets, the intersection of two SparseFixedBitSet is likely very sparse
      final int numCommonBlocks = Math.min(indices.length, other.indices.length);
      for (int i = 0; i < numCommonBlocks; ++i) {
        if ((indices[i] & other.indices[i]) == 0) {
          this.nonZeroLongCount -= Long.bitCount(this.indices[i]);
          this.indices[i] = 0;
          this.bits[i] = null;
        }
      }
    }
    super.and(it);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public String toString() {
    return "SparseFixedBitSet(size=" + length + ",cardinality=~" + approximateCardinality();
  }
}
