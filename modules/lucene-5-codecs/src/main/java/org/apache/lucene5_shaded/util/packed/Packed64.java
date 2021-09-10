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
package org.apache.lucene5_shaded.util.packed;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Space optimized random access capable array of values with a fixed number of
 * bits/value. Values are packed contiguously.
 * <p>
 * The implementation strives to perform af fast as possible under the
 * constraint of contiguous bits, by avoiding expensive operations. This comes
 * at the cost of code clarity.
 * <p>
 * Technical details: This implementation is a refinement of a non-branching
 * version. The non-branching get and set methods meant that 2 or 4 atomics in
 * the underlying array were always accessed, even for the cases where only
 * 1 or 2 were needed. Even with caching, this had a detrimental effect on
 * performance.
 * Related to this issue, the old implementation used lookup tables for shifts
 * and masks, which also proved to be a bit slower than calculating the shifts
 * and masks on the fly.
 * See https://issues.apache.org/jira/browse/LUCENE-4062 for details.
 *
 */
class Packed64 extends PackedInts.MutableImpl {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
  static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  /**
   * Values are stores contiguously in the blocks array.
   */
  private final long[] blocks;
  /**
   * A right-aligned mask of width BitsPerValue used by {@link #get(int)}.
   */
  private final long maskRight;
  /**
   * Optimization: Saves one lookup in {@link #get(int)}.
   */
  private final int bpvMinusBlockSize;

  /**
   * Creates an array with the internal structures adjusted for the given
   * limits and initialized to 0.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public Packed64(int valueCount, int bitsPerValue) {
    super(valueCount, bitsPerValue);
    final PackedInts.Format format = PackedInts.Format.PACKED;
    final int longCount = format.longCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue);
    this.blocks = new long[longCount];
    maskRight = ~0L << (BLOCK_SIZE-bitsPerValue) >>> (BLOCK_SIZE-bitsPerValue);
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  /**
   * Creates an array with content retrieved from the given DataInput.
   * @param in       a DataInput, positioned at the start of Packed64-content.
   * @param valueCount  the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @throws IOException if the values for the backing array could not
   *                             be retrieved.
   */
  public Packed64(int packedIntsVersion, DataInput in, int valueCount, int bitsPerValue)
                                                            throws IOException {
    super(valueCount, bitsPerValue);
    final PackedInts.Format format = PackedInts.Format.PACKED;
    final long byteCount = format.byteCount(packedIntsVersion, valueCount, bitsPerValue); // to know how much to read
    final int longCount = format.longCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue); // to size the array
    blocks = new long[longCount];
    // read as many longs as we can
    for (int i = 0; i < byteCount / 8; ++i) {
      blocks[i] = in.readLong();
    }
    final int remaining = (int) (byteCount % 8);
    if (remaining != 0) {
      // read the last bytes
      long lastLong = 0;
      for (int i = 0; i < remaining; ++i) {
        lastLong |= (in.readByte() & 0xFFL) << (56 - i * 8);
      }
      blocks[blocks.length - 1] = lastLong;
    }
    maskRight = ~0L << (BLOCK_SIZE-bitsPerValue) >>> (BLOCK_SIZE-bitsPerValue);
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  /**
   * @param index the position of the value.
   * @return the value at the given index.
   */
  @Override
  public long get(final int index) {
    // The abstract index in a bit stream
    final long majorBitPos = (long)index * bitsPerValue;
    // The index in the backing long-array
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS);
    // The number of value-bits in the second long
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    if (endBits <= 0) { // Single block
      return (blocks[elementPos] >>> -endBits) & maskRight;
    }
    // Two blocks
    return ((blocks[elementPos] << endBits)
        | (blocks[elementPos+1] >>> (BLOCK_SIZE - endBits)))
        & maskRight;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;
    final PackedInts.Decoder decoder = BulkOperation.of(PackedInts.Format.PACKED, bitsPerValue);

    // go to the next block where the value does not span across two blocks
    final int offsetInBlocks = index % decoder.longValueCount();
    if (offsetInBlocks != 0) {
      for (int i = offsetInBlocks; i < decoder.longValueCount() && len > 0; ++i) {
        arr[off++] = get(index++);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk get
    assert index % decoder.longValueCount() == 0;
    int blockIndex = (int) (((long) index * bitsPerValue) >>> BLOCK_BITS);
    assert (((long)index * bitsPerValue) & MOD_MASK) == 0;
    final int iterations = len / decoder.longValueCount();
    decoder.decode(blocks, blockIndex, arr, off, iterations);
    final int gotValues = iterations * decoder.longValueCount();
    index += gotValues;
    len -= gotValues;
    assert len >= 0;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to get
      assert index == originalIndex;
      return super.get(index, arr, off, len);
    }
  }

  @Override
  public void set(final int index, final long value) {
    // The abstract index in a contiguous bit stream
    final long majorBitPos = (long)index * bitsPerValue;
    // The index in the backing long-array
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    // The number of value-bits in the second long
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    if (endBits <= 0) { // Single block
      blocks[elementPos] = blocks[elementPos] &  ~(maskRight << -endBits)
         | (value << -endBits);
      return;
    }
    // Two blocks
    blocks[elementPos] = blocks[elementPos] &  ~(maskRight >>> endBits)
        | (value >>> endBits);
    blocks[elementPos+1] = blocks[elementPos+1] &  (~0L >>> endBits)
        | (value << (BLOCK_SIZE - endBits));
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;
    final PackedInts.Encoder encoder = BulkOperation.of(PackedInts.Format.PACKED, bitsPerValue);

    // go to the next block where the value does not span across two blocks
    final int offsetInBlocks = index % encoder.longValueCount();
    if (offsetInBlocks != 0) {
      for (int i = offsetInBlocks; i < encoder.longValueCount() && len > 0; ++i) {
        set(index++, arr[off++]);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk set
    assert index % encoder.longValueCount() == 0;
    int blockIndex = (int) (((long) index * bitsPerValue) >>> BLOCK_BITS);
    assert (((long)index * bitsPerValue) & MOD_MASK) == 0;
    final int iterations = len / encoder.longValueCount();
    encoder.encode(arr, off, blocks, blockIndex, iterations);
    final int setValues = iterations * encoder.longValueCount();
    index += setValues;
    len -= setValues;
    assert len >= 0;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to get
      assert index == originalIndex;
      return super.set(index, arr, off, len);
    }
  }

  @Override
  public String toString() {
    return "Packed64(bitsPerValue=" + bitsPerValue + ",size="
            + size() + ",blocks=" + blocks.length + ")";
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 3 * RamUsageEstimator.NUM_BYTES_INT     // bpvMinusBlockSize,valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_LONG        // maskRight
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert PackedInts.unsignedBitsRequired(val) <= getBitsPerValue();
    assert fromIndex <= toIndex;

    // minimum number of values that use an exact number of full blocks
    final int nAlignedValues = 64 / gcd(64, bitsPerValue);
    final int span = toIndex - fromIndex;
    if (span <= 3 * nAlignedValues) {
      // there needs be at least 2 * nAlignedValues aligned values for the
      // block approach to be worth trying
      super.fill(fromIndex, toIndex, val);
      return;
    }

    // fill the first values naively until the next block start
    final int fromIndexModNAlignedValues = fromIndex % nAlignedValues;
    if (fromIndexModNAlignedValues != 0) {
      for (int i = fromIndexModNAlignedValues; i < nAlignedValues; ++i) {
        set(fromIndex++, val);
      }
    }
    assert fromIndex % nAlignedValues == 0;

    // compute the long[] blocks for nAlignedValues consecutive values and
    // use them to set as many values as possible without applying any mask
    // or shift
    final int nAlignedBlocks = (nAlignedValues * bitsPerValue) >> 6;
    final long[] nAlignedValuesBlocks;
    {
      Packed64 values = new Packed64(nAlignedValues, bitsPerValue);
      for (int i = 0; i < nAlignedValues; ++i) {
        values.set(i, val);
      }
      nAlignedValuesBlocks = values.blocks;
      assert nAlignedBlocks <= nAlignedValuesBlocks.length;
    }
    final int startBlock = (int) (((long) fromIndex * bitsPerValue) >>> 6);
    final int endBlock = (int) (((long) toIndex * bitsPerValue) >>> 6);
    for (int  block = startBlock; block < endBlock; ++block) {
      final long blockValue = nAlignedValuesBlocks[block % nAlignedBlocks];
      blocks[block] = blockValue;
    }

    // fill the gap
    for (int i = (int) (((long) endBlock << 6) / bitsPerValue); i < toIndex; ++i) {
      set(i, val);
    }
  }

  private static int gcd(int a, int b) {
    if (a < b) {
      return gcd(b, a);
    } else if (b == 0) {
      return a;
    } else {
      return gcd(b, a % b);
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, 0L);
  }
}
