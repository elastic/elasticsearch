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

/**
 * Efficient sequential read/write of packed integers.
 */
final class BulkOperationPacked12 extends BulkOperationPacked {

  public BulkOperationPacked12() {
    super(12);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 52);
      values[valuesOffset++] = (int) ((block0 >>> 40) & 4095L);
      values[valuesOffset++] = (int) ((block0 >>> 28) & 4095L);
      values[valuesOffset++] = (int) ((block0 >>> 16) & 4095L);
      values[valuesOffset++] = (int) ((block0 >>> 4) & 4095L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 15L) << 8) | (block1 >>> 56));
      values[valuesOffset++] = (int) ((block1 >>> 44) & 4095L);
      values[valuesOffset++] = (int) ((block1 >>> 32) & 4095L);
      values[valuesOffset++] = (int) ((block1 >>> 20) & 4095L);
      values[valuesOffset++] = (int) ((block1 >>> 8) & 4095L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 255L) << 4) | (block2 >>> 60));
      values[valuesOffset++] = (int) ((block2 >>> 48) & 4095L);
      values[valuesOffset++] = (int) ((block2 >>> 36) & 4095L);
      values[valuesOffset++] = (int) ((block2 >>> 24) & 4095L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 4095L);
      values[valuesOffset++] = (int) (block2 & 4095L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 4) | (byte1 >>> 4);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 15) << 8) | byte2;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 52;
      values[valuesOffset++] = (block0 >>> 40) & 4095L;
      values[valuesOffset++] = (block0 >>> 28) & 4095L;
      values[valuesOffset++] = (block0 >>> 16) & 4095L;
      values[valuesOffset++] = (block0 >>> 4) & 4095L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 15L) << 8) | (block1 >>> 56);
      values[valuesOffset++] = (block1 >>> 44) & 4095L;
      values[valuesOffset++] = (block1 >>> 32) & 4095L;
      values[valuesOffset++] = (block1 >>> 20) & 4095L;
      values[valuesOffset++] = (block1 >>> 8) & 4095L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 255L) << 4) | (block2 >>> 60);
      values[valuesOffset++] = (block2 >>> 48) & 4095L;
      values[valuesOffset++] = (block2 >>> 36) & 4095L;
      values[valuesOffset++] = (block2 >>> 24) & 4095L;
      values[valuesOffset++] = (block2 >>> 12) & 4095L;
      values[valuesOffset++] = block2 & 4095L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 4) | (byte1 >>> 4);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 15) << 8) | byte2;
    }
  }

}
