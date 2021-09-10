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
final class BulkOperationPacked7 extends BulkOperationPacked {

  public BulkOperationPacked7() {
    super(7);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 57);
      values[valuesOffset++] = (int) ((block0 >>> 50) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 43) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 36) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 29) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 22) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 15) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 8) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 1) & 127L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 1L) << 6) | (block1 >>> 58));
      values[valuesOffset++] = (int) ((block1 >>> 51) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 44) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 37) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 30) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 23) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 16) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 9) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 2) & 127L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 3L) << 5) | (block2 >>> 59));
      values[valuesOffset++] = (int) ((block2 >>> 52) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 45) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 38) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 31) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 24) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 17) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 10) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 3) & 127L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 7L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = (int) ((block3 >>> 53) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 46) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 39) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 32) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 25) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 11) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 4) & 127L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 3) | (block4 >>> 61));
      values[valuesOffset++] = (int) ((block4 >>> 54) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 47) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 40) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 33) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 26) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 19) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 12) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 5) & 127L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 31L) << 2) | (block5 >>> 62));
      values[valuesOffset++] = (int) ((block5 >>> 55) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 48) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 41) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 34) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 27) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 20) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 13) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 6) & 127L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 63L) << 1) | (block6 >>> 63));
      values[valuesOffset++] = (int) ((block6 >>> 56) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 49) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 42) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 35) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 28) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 21) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 14) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 7) & 127L);
      values[valuesOffset++] = (int) (block6 & 127L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 >>> 1;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte0 & 1) << 6) | (byte1 >>> 2);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 5) | (byte2 >>> 3);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 7) << 4) | (byte3 >>> 4);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 3) | (byte4 >>> 5);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 31) << 2) | (byte5 >>> 6);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 1) | (byte6 >>> 7);
      values[valuesOffset++] = byte6 & 127;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 57;
      values[valuesOffset++] = (block0 >>> 50) & 127L;
      values[valuesOffset++] = (block0 >>> 43) & 127L;
      values[valuesOffset++] = (block0 >>> 36) & 127L;
      values[valuesOffset++] = (block0 >>> 29) & 127L;
      values[valuesOffset++] = (block0 >>> 22) & 127L;
      values[valuesOffset++] = (block0 >>> 15) & 127L;
      values[valuesOffset++] = (block0 >>> 8) & 127L;
      values[valuesOffset++] = (block0 >>> 1) & 127L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 1L) << 6) | (block1 >>> 58);
      values[valuesOffset++] = (block1 >>> 51) & 127L;
      values[valuesOffset++] = (block1 >>> 44) & 127L;
      values[valuesOffset++] = (block1 >>> 37) & 127L;
      values[valuesOffset++] = (block1 >>> 30) & 127L;
      values[valuesOffset++] = (block1 >>> 23) & 127L;
      values[valuesOffset++] = (block1 >>> 16) & 127L;
      values[valuesOffset++] = (block1 >>> 9) & 127L;
      values[valuesOffset++] = (block1 >>> 2) & 127L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 3L) << 5) | (block2 >>> 59);
      values[valuesOffset++] = (block2 >>> 52) & 127L;
      values[valuesOffset++] = (block2 >>> 45) & 127L;
      values[valuesOffset++] = (block2 >>> 38) & 127L;
      values[valuesOffset++] = (block2 >>> 31) & 127L;
      values[valuesOffset++] = (block2 >>> 24) & 127L;
      values[valuesOffset++] = (block2 >>> 17) & 127L;
      values[valuesOffset++] = (block2 >>> 10) & 127L;
      values[valuesOffset++] = (block2 >>> 3) & 127L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 7L) << 4) | (block3 >>> 60);
      values[valuesOffset++] = (block3 >>> 53) & 127L;
      values[valuesOffset++] = (block3 >>> 46) & 127L;
      values[valuesOffset++] = (block3 >>> 39) & 127L;
      values[valuesOffset++] = (block3 >>> 32) & 127L;
      values[valuesOffset++] = (block3 >>> 25) & 127L;
      values[valuesOffset++] = (block3 >>> 18) & 127L;
      values[valuesOffset++] = (block3 >>> 11) & 127L;
      values[valuesOffset++] = (block3 >>> 4) & 127L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 3) | (block4 >>> 61);
      values[valuesOffset++] = (block4 >>> 54) & 127L;
      values[valuesOffset++] = (block4 >>> 47) & 127L;
      values[valuesOffset++] = (block4 >>> 40) & 127L;
      values[valuesOffset++] = (block4 >>> 33) & 127L;
      values[valuesOffset++] = (block4 >>> 26) & 127L;
      values[valuesOffset++] = (block4 >>> 19) & 127L;
      values[valuesOffset++] = (block4 >>> 12) & 127L;
      values[valuesOffset++] = (block4 >>> 5) & 127L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 31L) << 2) | (block5 >>> 62);
      values[valuesOffset++] = (block5 >>> 55) & 127L;
      values[valuesOffset++] = (block5 >>> 48) & 127L;
      values[valuesOffset++] = (block5 >>> 41) & 127L;
      values[valuesOffset++] = (block5 >>> 34) & 127L;
      values[valuesOffset++] = (block5 >>> 27) & 127L;
      values[valuesOffset++] = (block5 >>> 20) & 127L;
      values[valuesOffset++] = (block5 >>> 13) & 127L;
      values[valuesOffset++] = (block5 >>> 6) & 127L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 63L) << 1) | (block6 >>> 63);
      values[valuesOffset++] = (block6 >>> 56) & 127L;
      values[valuesOffset++] = (block6 >>> 49) & 127L;
      values[valuesOffset++] = (block6 >>> 42) & 127L;
      values[valuesOffset++] = (block6 >>> 35) & 127L;
      values[valuesOffset++] = (block6 >>> 28) & 127L;
      values[valuesOffset++] = (block6 >>> 21) & 127L;
      values[valuesOffset++] = (block6 >>> 14) & 127L;
      values[valuesOffset++] = (block6 >>> 7) & 127L;
      values[valuesOffset++] = block6 & 127L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 >>> 1;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte0 & 1) << 6) | (byte1 >>> 2);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 5) | (byte2 >>> 3);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 7) << 4) | (byte3 >>> 4);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 3) | (byte4 >>> 5);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 31) << 2) | (byte5 >>> 6);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 1) | (byte6 >>> 7);
      values[valuesOffset++] = byte6 & 127;
    }
  }

}
