// This file has been automatically generated, DO NOT EDIT

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

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Packs integers into 3 bytes (24 bits per value).
 * @lucene.internal
 */
final class Packed8ThreeBlocks extends PackedInts.MutableImpl {
  final byte[] blocks;

  public static final int MAX_SIZE = Integer.MAX_VALUE / 3;

  Packed8ThreeBlocks(int valueCount) {
    super(valueCount, 24);
    if (valueCount > MAX_SIZE) {
      throw new ArrayIndexOutOfBoundsException("MAX_SIZE exceeded");
    }
    blocks = new byte[valueCount * 3];
  }

  Packed8ThreeBlocks(int packedIntsVersion, DataInput in, int valueCount) throws IOException {
    this(valueCount);
    in.readBytes(blocks, 0, 3 * valueCount);
    // because packed ints have not always been byte-aligned
    final int remaining = (int) (PackedInts.Format.PACKED.byteCount(packedIntsVersion, valueCount, 24) - 3L * valueCount * 1);
    for (int i = 0; i < remaining; ++i) {
       in.readByte();
    }
  }

  @Override
  public long get(int index) {
    final int o = index * 3;
    return (blocks[o] & 0xFFL) << 16 | (blocks[o+1] & 0xFFL) << 8 | (blocks[o+2] & 0xFFL);
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index * 3, end = (index + gets) * 3; i < end; i+=3) {
      arr[off++] = (blocks[i] & 0xFFL) << 16 | (blocks[i+1] & 0xFFL) << 8 | (blocks[i+2] & 0xFFL);
    }
    return gets;
  }

  @Override
  public void set(int index, long value) {
    final int o = index * 3;
    blocks[o] = (byte) (value >>> 16);
    blocks[o+1] = (byte) (value >>> 8);
    blocks[o+2] = (byte) value;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = off, o = index * 3, end = off + sets; i < end; ++i) {
      final long value = arr[i];
      blocks[o++] = (byte) (value >>> 16);
      blocks[o++] = (byte) (value >>> 8);
      blocks[o++] = (byte) value;
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    final byte block1 = (byte) (val >>> 16);
    final byte block2 = (byte) (val >>> 8);
    final byte block3 = (byte) val;
    for (int i = fromIndex * 3, end = toIndex * 3; i < end; i += 3) {
      blocks[i] = block1;
      blocks[i+1] = block2;
      blocks[i+2] = block3;
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, (byte) 0);
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
  public String toString() {
    return getClass().getSimpleName() + "(bitsPerValue=" + bitsPerValue
        + ",size=" + size() + ",blocks=" + blocks.length + ")";
  }
}
