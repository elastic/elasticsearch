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
final class BulkOperationPacked14 extends BulkOperationPacked {

  public BulkOperationPacked14() {
    super(14);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 50);
      values[valuesOffset++] = (int) ((block0 >>> 36) & 16383L);
      values[valuesOffset++] = (int) ((block0 >>> 22) & 16383L);
      values[valuesOffset++] = (int) ((block0 >>> 8) & 16383L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 255L) << 6) | (block1 >>> 58));
      values[valuesOffset++] = (int) ((block1 >>> 44) & 16383L);
      values[valuesOffset++] = (int) ((block1 >>> 30) & 16383L);
      values[valuesOffset++] = (int) ((block1 >>> 16) & 16383L);
      values[valuesOffset++] = (int) ((block1 >>> 2) & 16383L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 3L) << 12) | (block2 >>> 52));
      values[valuesOffset++] = (int) ((block2 >>> 38) & 16383L);
      values[valuesOffset++] = (int) ((block2 >>> 24) & 16383L);
      values[valuesOffset++] = (int) ((block2 >>> 10) & 16383L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 1023L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = (int) ((block3 >>> 46) & 16383L);
      values[valuesOffset++] = (int) ((block3 >>> 32) & 16383L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 16383L);
      values[valuesOffset++] = (int) ((block3 >>> 4) & 16383L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 10) | (block4 >>> 54));
      values[valuesOffset++] = (int) ((block4 >>> 40) & 16383L);
      values[valuesOffset++] = (int) ((block4 >>> 26) & 16383L);
      values[valuesOffset++] = (int) ((block4 >>> 12) & 16383L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 4095L) << 2) | (block5 >>> 62));
      values[valuesOffset++] = (int) ((block5 >>> 48) & 16383L);
      values[valuesOffset++] = (int) ((block5 >>> 34) & 16383L);
      values[valuesOffset++] = (int) ((block5 >>> 20) & 16383L);
      values[valuesOffset++] = (int) ((block5 >>> 6) & 16383L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 63L) << 8) | (block6 >>> 56));
      values[valuesOffset++] = (int) ((block6 >>> 42) & 16383L);
      values[valuesOffset++] = (int) ((block6 >>> 28) & 16383L);
      values[valuesOffset++] = (int) ((block6 >>> 14) & 16383L);
      values[valuesOffset++] = (int) (block6 & 16383L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 6) | (byte1 >>> 2);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 12) | (byte2 << 4) | (byte3 >>> 4);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 10) | (byte4 << 2) | (byte5 >>> 6);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 8) | byte6;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 50;
      values[valuesOffset++] = (block0 >>> 36) & 16383L;
      values[valuesOffset++] = (block0 >>> 22) & 16383L;
      values[valuesOffset++] = (block0 >>> 8) & 16383L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 255L) << 6) | (block1 >>> 58);
      values[valuesOffset++] = (block1 >>> 44) & 16383L;
      values[valuesOffset++] = (block1 >>> 30) & 16383L;
      values[valuesOffset++] = (block1 >>> 16) & 16383L;
      values[valuesOffset++] = (block1 >>> 2) & 16383L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 3L) << 12) | (block2 >>> 52);
      values[valuesOffset++] = (block2 >>> 38) & 16383L;
      values[valuesOffset++] = (block2 >>> 24) & 16383L;
      values[valuesOffset++] = (block2 >>> 10) & 16383L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 1023L) << 4) | (block3 >>> 60);
      values[valuesOffset++] = (block3 >>> 46) & 16383L;
      values[valuesOffset++] = (block3 >>> 32) & 16383L;
      values[valuesOffset++] = (block3 >>> 18) & 16383L;
      values[valuesOffset++] = (block3 >>> 4) & 16383L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 10) | (block4 >>> 54);
      values[valuesOffset++] = (block4 >>> 40) & 16383L;
      values[valuesOffset++] = (block4 >>> 26) & 16383L;
      values[valuesOffset++] = (block4 >>> 12) & 16383L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 4095L) << 2) | (block5 >>> 62);
      values[valuesOffset++] = (block5 >>> 48) & 16383L;
      values[valuesOffset++] = (block5 >>> 34) & 16383L;
      values[valuesOffset++] = (block5 >>> 20) & 16383L;
      values[valuesOffset++] = (block5 >>> 6) & 16383L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 63L) << 8) | (block6 >>> 56);
      values[valuesOffset++] = (block6 >>> 42) & 16383L;
      values[valuesOffset++] = (block6 >>> 28) & 16383L;
      values[valuesOffset++] = (block6 >>> 14) & 16383L;
      values[valuesOffset++] = block6 & 16383L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 6) | (byte1 >>> 2);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 12) | (byte2 << 4) | (byte3 >>> 4);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 10) | (byte4 << 2) | (byte5 >>> 6);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 8) | byte6;
    }
  }

}
