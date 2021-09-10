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


/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED_SINGLE_BLOCK}.
 */
final class BulkOperationPackedSingleBlock extends BulkOperation {

  private static final int BLOCK_COUNT = 1;

  private final int bitsPerValue;
  private final int valueCount;
  private final long mask;

  public BulkOperationPackedSingleBlock(int bitsPerValue) {
    this.bitsPerValue = bitsPerValue;
    this.valueCount = 64 / bitsPerValue;
    this.mask = (1L << bitsPerValue) - 1;
  }

  @Override
  public final int longBlockCount() {
    return BLOCK_COUNT;
  }

  @Override
  public final int byteBlockCount() {
    return BLOCK_COUNT * 8;
  }

  @Override
  public int longValueCount() {
    return valueCount;
  }

  @Override
  public final int byteValueCount() {
    return valueCount;
  }

  private static long readLong(byte[] blocks, int blocksOffset) {
    return (blocks[blocksOffset++] & 0xFFL) << 56
        | (blocks[blocksOffset++] & 0xFFL) << 48
        | (blocks[blocksOffset++] & 0xFFL) << 40
        | (blocks[blocksOffset++] & 0xFFL) << 32
        | (blocks[blocksOffset++] & 0xFFL) << 24
        | (blocks[blocksOffset++] & 0xFFL) << 16
        | (blocks[blocksOffset++] & 0xFFL) << 8
        | blocks[blocksOffset++] & 0xFFL;
  }

  private int decode(long block, long[] values, int valuesOffset) {
    values[valuesOffset++] = block & mask;
    for (int j = 1; j < valueCount; ++j) {
      block >>>= bitsPerValue;
      values[valuesOffset++] = block & mask;
    }
    return valuesOffset;
  }

  private int decode(long block, int[] values, int valuesOffset) {
    values[valuesOffset++] = (int) (block & mask);
    for (int j = 1; j < valueCount; ++j) {
      block >>>= bitsPerValue;
      values[valuesOffset++] = (int) (block & mask);
    }
    return valuesOffset;
  }

  private long encode(long[] values, int valuesOffset) {
    long block = values[valuesOffset++];
    for (int j = 1; j < valueCount; ++j) {
      block |= values[valuesOffset++] << (j * bitsPerValue);
    }
    return block;
  }

  private long encode(int[] values, int valuesOffset) {
    long block = values[valuesOffset++] & 0xFFFFFFFFL;
    for (int j = 1; j < valueCount; ++j) {
      block |= (values[valuesOffset++] & 0xFFFFFFFFL) << (j * bitsPerValue);
    }
    return block;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = blocks[blocksOffset++];
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = readLong(blocks, blocksOffset);
      blocksOffset += 8;
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    for (int i = 0; i < iterations; ++i) {
      final long block = blocks[blocksOffset++];
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    for (int i = 0; i < iterations; ++i) {
      final long block = readLong(blocks, blocksOffset);
      blocksOffset += 8;
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      blocks[blocksOffset++] = encode(values, valuesOffset);
      valuesOffset += valueCount;
    }
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      blocks[blocksOffset++] = encode(values, valuesOffset);
      valuesOffset += valueCount;
    }
  }

  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = encode(values, valuesOffset);
      valuesOffset += valueCount;
      blocksOffset = writeLong(block, blocks, blocksOffset);
    }
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = encode(values, valuesOffset);
      valuesOffset += valueCount;
      blocksOffset = writeLong(block, blocks, blocksOffset);
    }
  }

}
