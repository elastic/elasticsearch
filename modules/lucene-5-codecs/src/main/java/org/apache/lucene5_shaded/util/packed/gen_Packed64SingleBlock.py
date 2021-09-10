#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SUPPORTED_BITS_PER_VALUE = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]

HEADER = """// This file has been automatically generated, DO NOT EDIT

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

  public static final int MAX_SUPPORTED_BITS_PER_VALUE = %d;
  private static final int[] SUPPORTED_BITS_PER_VALUE = new int[] {%s};

  public static boolean isSupported(int bitsPerValue) {
    return Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) >= 0;
  }

  private static int requiredCapacity(int valueCount, int valuesPerBlock) {
    return valueCount / valuesPerBlock
        + (valueCount %% valuesPerBlock == 0 ? 0 : 1);
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
    final int offsetInBlock = index %% valuesPerBlock;
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
    assert index %% valuesPerBlock == 0;
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
    final int offsetInBlock = index %% valuesPerBlock;
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
    assert index %% valuesPerBlock == 0;
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
    int fromOffsetInBlock = fromIndex %% valuesPerBlock;
    if (fromOffsetInBlock != 0) {
      for (int i = fromOffsetInBlock; i < valuesPerBlock; ++i) {
        set(fromIndex++, val);
      }
      assert fromIndex %% valuesPerBlock == 0;
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

""" % (SUPPORTED_BITS_PER_VALUE[-1], ", ".join(map(str, SUPPORTED_BITS_PER_VALUE)))

FOOTER = "}"

if __name__ == '__main__':

  f = open("Packed64SingleBlock.java", 'w')
  f.write(HEADER)
  f.write("  public static Packed64SingleBlock create(int valueCount, int bitsPerValue) {\n")
  f.write("    switch (bitsPerValue) {\n")
  for bpv in SUPPORTED_BITS_PER_VALUE:
    f.write("      case %d:\n" % bpv)
    f.write("        return new Packed64SingleBlock%d(valueCount);\n" % bpv)
  f.write("      default:\n")
  f.write("        throw new IllegalArgumentException(\"Unsupported number of bits per value: \" + %d);\n" % bpv)
  f.write("    }\n")
  f.write("  }\n\n")

  for bpv in SUPPORTED_BITS_PER_VALUE:
    log_2 = 0
    while (1 << log_2) < bpv:
      log_2 = log_2 + 1
    if (1 << log_2) != bpv:
      log_2 = None

    f.write("  static class Packed64SingleBlock%d extends Packed64SingleBlock {\n\n" % bpv)

    f.write("    Packed64SingleBlock%d(int valueCount) {\n" % bpv)
    f.write("      super(valueCount, %d);\n" % bpv)
    f.write("    }\n\n")

    f.write("    @Override\n")
    f.write("    public long get(int index) {\n")
    if log_2 is not None:
      f.write("      final int o = index >>> %d;\n" % (6 - log_2))
      f.write("      final int b = index & %d;\n" % ((1 << (6 - log_2)) - 1))
      f.write("      final int shift = b << %d;\n" % log_2)
    else:
      f.write("      final int o = index / %d;\n" % (64 / bpv))
      f.write("      final int b = index %% %d;\n" % (64 / bpv))
      f.write("      final int shift = b * %d;\n" % bpv)
    f.write("      return (blocks[o] >>> shift) & %dL;\n" % ((1 << bpv) - 1))
    f.write("    }\n\n")

    f.write("    @Override\n")
    f.write("    public void set(int index, long value) {\n")
    if log_2 is not None:
      f.write("      final int o = index >>> %d;\n" % (6 - log_2))
      f.write("      final int b = index & %d;\n" % ((1 << (6 - log_2)) - 1))
      f.write("      final int shift = b << %d;\n" % log_2)
    else:
      f.write("      final int o = index / %d;\n" % (64 / bpv))
      f.write("      final int b = index %% %d;\n" % (64 / bpv))
      f.write("      final int shift = b * %d;\n" % bpv)
    f.write("      blocks[o] = (blocks[o] & ~(%dL << shift)) | (value << shift);\n" % ((1 << bpv) - 1))
    f.write("    }\n\n")
    f.write("  }\n\n")

  f.write(FOOTER)
  f.close()
