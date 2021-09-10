// This file has been automatically generated, DO NOT EDIT

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.lucene5_shaded.util.packed;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * This class is similar to {@link Packed64} except that it trades space for
 * speed by ensuring that a single block needs to be read/written in order to
 * read/write a value.
 */
abstract class Packed64SingleBlock extends PackedInts.MutableImpl {

  public static final int MAX_SUPPORTED_BITS_PER_VALUE = 32;
  private static final int[] SUPPORTED_BITS_PER_VALUE = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32};

  public static boolean isSupported(int bitsPerValue) {
    return Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) >= 0;
  }

  private static int requiredCapacity(int valueCount, int valuesPerBlock) {
    return valueCount / valuesPerBlock
        + (valueCount % valuesPerBlock == 0 ? 0 : 1);
  }

  final long[] blocks;

  Packed64SingleBlock(int valueCount, int bitsPerValue) {
    super(valueCount, bitsPerValue);
    assert isSupported(bitsPerValue);
    final int valuesPerBlock = 64 / bitsPerValue;
    blocks = new long[requiredCapacity(valueCount, valuesPerBlock)];
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, 0L);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * RamUsageEstimator.NUM_BYTES_INT     // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;

    // go to the next block boundary
    final int valuesPerBlock = 64 / bitsPerValue;
    final int offsetInBlock = index % valuesPerBlock;
    if (offsetInBlock != 0) {
      for (int i = offsetInBlock; i < valuesPerBlock && len > 0; ++i) {
        arr[off++] = get(index++);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk get
    assert index % valuesPerBlock == 0;
    final PackedInts.Decoder decoder = BulkOperation.of(PackedInts.Format.PACKED_SINGLE_BLOCK, bitsPerValue);
    assert decoder.longBlockCount() == 1;
    assert decoder.longValueCount() == valuesPerBlock;
    final int blockIndex = index / valuesPerBlock;
    final int nblocks = (index + len) / valuesPerBlock - blockIndex;
    decoder.decode(blocks, blockIndex, arr, off, nblocks);
    final int diff = nblocks * valuesPerBlock;
    index += diff; len -= diff;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to
      // get
      assert index == originalIndex;
      return super.get(index, arr, off, len);
    }
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;

    // go to the next block boundary
    final int valuesPerBlock = 64 / bitsPerValue;
    final int offsetInBlock = index % valuesPerBlock;
    if (offsetInBlock != 0) {
      for (int i = offsetInBlock; i < valuesPerBlock && len > 0; ++i) {
        set(index++, arr[off++]);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk set
    assert index % valuesPerBlock == 0;
    final BulkOperation op = BulkOperation.of(PackedInts.Format.PACKED_SINGLE_BLOCK, bitsPerValue);
    assert op.longBlockCount() == 1;
    assert op.longValueCount() == valuesPerBlock;
    final int blockIndex = index / valuesPerBlock;
    final int nblocks = (index + len) / valuesPerBlock - blockIndex;
    op.encode(arr, off, blocks, blockIndex, nblocks);
    final int diff = nblocks * valuesPerBlock;
    index += diff; len -= diff;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to
      // set
      assert index == originalIndex;
      return super.set(index, arr, off, len);
    }
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert fromIndex >= 0;
    assert fromIndex <= toIndex;
    assert PackedInts.unsignedBitsRequired(val) <= bitsPerValue;

    final int valuesPerBlock = 64 / bitsPerValue;
    if (toIndex - fromIndex <= valuesPerBlock << 1) {
      // there needs to be at least one full block to set for the block
      // approach to be worth trying
      super.fill(fromIndex, toIndex, val);
      return;
    }

    // set values naively until the next block start
    int fromOffsetInBlock = fromIndex % valuesPerBlock;
    if (fromOffsetInBlock != 0) {
      for (int i = fromOffsetInBlock; i < valuesPerBlock; ++i) {
        set(fromIndex++, val);
      }
      assert fromIndex % valuesPerBlock == 0;
    }

    // bulk set of the inner blocks
    final int fromBlock = fromIndex / valuesPerBlock;
    final int toBlock = toIndex / valuesPerBlock;
    assert fromBlock * valuesPerBlock == fromIndex;

    long blockValue = 0L;
    for (int i = 0; i < valuesPerBlock; ++i) {
      blockValue = blockValue | (val << (i * bitsPerValue));
    }
    Arrays.fill(blocks, fromBlock, toBlock, blockValue);

    // fill the gap
    for (int i = valuesPerBlock * toBlock; i < toIndex; ++i) {
      set(i, val);
    }
  }

  @Override
  protected PackedInts.Format getFormat() {
    return PackedInts.Format.PACKED_SINGLE_BLOCK;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(bitsPerValue=" + bitsPerValue
        + ",size=" + size() + ",blocks=" + blocks.length + ")";
  }

  public static Packed64SingleBlock create(DataInput in,
      int valueCount, int bitsPerValue) throws IOException {
    Packed64SingleBlock reader = create(valueCount, bitsPerValue);
    for (int i = 0; i < reader.blocks.length; ++i) {
      reader.blocks[i] = in.readLong();
    }
    return reader;
  }

  public static Packed64SingleBlock create(int valueCount, int bitsPerValue) {
    switch (bitsPerValue) {
      case 1:
        return new Packed64SingleBlock1(valueCount);
      case 2:
        return new Packed64SingleBlock2(valueCount);
      case 3:
        return new Packed64SingleBlock3(valueCount);
      case 4:
        return new Packed64SingleBlock4(valueCount);
      case 5:
        return new Packed64SingleBlock5(valueCount);
      case 6:
        return new Packed64SingleBlock6(valueCount);
      case 7:
        return new Packed64SingleBlock7(valueCount);
      case 8:
        return new Packed64SingleBlock8(valueCount);
      case 9:
        return new Packed64SingleBlock9(valueCount);
      case 10:
        return new Packed64SingleBlock10(valueCount);
      case 12:
        return new Packed64SingleBlock12(valueCount);
      case 16:
        return new Packed64SingleBlock16(valueCount);
      case 21:
        return new Packed64SingleBlock21(valueCount);
      case 32:
        return new Packed64SingleBlock32(valueCount);
      default:
        throw new IllegalArgumentException("Unsupported number of bits per value: " + 32);
    }
  }

  static class Packed64SingleBlock1 extends Packed64SingleBlock {

    Packed64SingleBlock1(int valueCount) {
      super(valueCount, 1);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 6;
      final int b = index & 63;
      final int shift = b << 0;
      return (blocks[o] >>> shift) & 1L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 6;
      final int b = index & 63;
      final int shift = b << 0;
      blocks[o] = (blocks[o] & ~(1L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock2 extends Packed64SingleBlock {

    Packed64SingleBlock2(int valueCount) {
      super(valueCount, 2);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 5;
      final int b = index & 31;
      final int shift = b << 1;
      return (blocks[o] >>> shift) & 3L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 5;
      final int b = index & 31;
      final int shift = b << 1;
      blocks[o] = (blocks[o] & ~(3L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock3 extends Packed64SingleBlock {

    Packed64SingleBlock3(int valueCount) {
      super(valueCount, 3);
    }

    @Override
    public long get(int index) {
      final int o = index / 21;
      final int b = index % 21;
      final int shift = b * 3;
      return (blocks[o] >>> shift) & 7L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 21;
      final int b = index % 21;
      final int shift = b * 3;
      blocks[o] = (blocks[o] & ~(7L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock4 extends Packed64SingleBlock {

    Packed64SingleBlock4(int valueCount) {
      super(valueCount, 4);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 4;
      final int b = index & 15;
      final int shift = b << 2;
      return (blocks[o] >>> shift) & 15L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 4;
      final int b = index & 15;
      final int shift = b << 2;
      blocks[o] = (blocks[o] & ~(15L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock5 extends Packed64SingleBlock {

    Packed64SingleBlock5(int valueCount) {
      super(valueCount, 5);
    }

    @Override
    public long get(int index) {
      final int o = index / 12;
      final int b = index % 12;
      final int shift = b * 5;
      return (blocks[o] >>> shift) & 31L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 12;
      final int b = index % 12;
      final int shift = b * 5;
      blocks[o] = (blocks[o] & ~(31L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock6 extends Packed64SingleBlock {

    Packed64SingleBlock6(int valueCount) {
      super(valueCount, 6);
    }

    @Override
    public long get(int index) {
      final int o = index / 10;
      final int b = index % 10;
      final int shift = b * 6;
      return (blocks[o] >>> shift) & 63L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 10;
      final int b = index % 10;
      final int shift = b * 6;
      blocks[o] = (blocks[o] & ~(63L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock7 extends Packed64SingleBlock {

    Packed64SingleBlock7(int valueCount) {
      super(valueCount, 7);
    }

    @Override
    public long get(int index) {
      final int o = index / 9;
      final int b = index % 9;
      final int shift = b * 7;
      return (blocks[o] >>> shift) & 127L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 9;
      final int b = index % 9;
      final int shift = b * 7;
      blocks[o] = (blocks[o] & ~(127L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock8 extends Packed64SingleBlock {

    Packed64SingleBlock8(int valueCount) {
      super(valueCount, 8);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 3;
      final int b = index & 7;
      final int shift = b << 3;
      return (blocks[o] >>> shift) & 255L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 3;
      final int b = index & 7;
      final int shift = b << 3;
      blocks[o] = (blocks[o] & ~(255L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock9 extends Packed64SingleBlock {

    Packed64SingleBlock9(int valueCount) {
      super(valueCount, 9);
    }

    @Override
    public long get(int index) {
      final int o = index / 7;
      final int b = index % 7;
      final int shift = b * 9;
      return (blocks[o] >>> shift) & 511L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 7;
      final int b = index % 7;
      final int shift = b * 9;
      blocks[o] = (blocks[o] & ~(511L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock10 extends Packed64SingleBlock {

    Packed64SingleBlock10(int valueCount) {
      super(valueCount, 10);
    }

    @Override
    public long get(int index) {
      final int o = index / 6;
      final int b = index % 6;
      final int shift = b * 10;
      return (blocks[o] >>> shift) & 1023L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 6;
      final int b = index % 6;
      final int shift = b * 10;
      blocks[o] = (blocks[o] & ~(1023L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock12 extends Packed64SingleBlock {

    Packed64SingleBlock12(int valueCount) {
      super(valueCount, 12);
    }

    @Override
    public long get(int index) {
      final int o = index / 5;
      final int b = index % 5;
      final int shift = b * 12;
      return (blocks[o] >>> shift) & 4095L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 5;
      final int b = index % 5;
      final int shift = b * 12;
      blocks[o] = (blocks[o] & ~(4095L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock16 extends Packed64SingleBlock {

    Packed64SingleBlock16(int valueCount) {
      super(valueCount, 16);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 2;
      final int b = index & 3;
      final int shift = b << 4;
      return (blocks[o] >>> shift) & 65535L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 2;
      final int b = index & 3;
      final int shift = b << 4;
      blocks[o] = (blocks[o] & ~(65535L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock21 extends Packed64SingleBlock {

    Packed64SingleBlock21(int valueCount) {
      super(valueCount, 21);
    }

    @Override
    public long get(int index) {
      final int o = index / 3;
      final int b = index % 3;
      final int shift = b * 21;
      return (blocks[o] >>> shift) & 2097151L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 3;
      final int b = index % 3;
      final int shift = b * 21;
      blocks[o] = (blocks[o] & ~(2097151L << shift)) | (value << shift);
    }

  }

  static class Packed64SingleBlock32 extends Packed64SingleBlock {

    Packed64SingleBlock32(int valueCount) {
      super(valueCount, 32);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 1;
      final int b = index & 1;
      final int shift = b << 5;
      return (blocks[o] >>> shift) & 4294967295L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 1;
      final int b = index & 1;
      final int shift = b << 5;
      blocks[o] = (blocks[o] & ~(4294967295L << shift)) | (value << shift);
    }

  }

}
