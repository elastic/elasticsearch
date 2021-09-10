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
final class BulkOperationPacked9 extends BulkOperationPacked {

  public BulkOperationPacked9() {
    super(9);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 55);
      values[valuesOffset++] = (int) ((block0 >>> 46) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 37) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 28) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 19) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 10) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 1) & 511L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 1L) << 8) | (block1 >>> 56));
      values[valuesOffset++] = (int) ((block1 >>> 47) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 38) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 29) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 20) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 11) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 2) & 511L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 3L) << 7) | (block2 >>> 57));
      values[valuesOffset++] = (int) ((block2 >>> 48) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 39) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 30) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 21) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 3) & 511L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 7L) << 6) | (block3 >>> 58));
      values[valuesOffset++] = (int) ((block3 >>> 49) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 40) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 31) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 22) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 13) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 4) & 511L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 5) | (block4 >>> 59));
      values[valuesOffset++] = (int) ((block4 >>> 50) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 41) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 32) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 23) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 14) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 5) & 511L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 31L) << 4) | (block5 >>> 60));
      values[valuesOffset++] = (int) ((block5 >>> 51) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 42) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 33) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 24) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 15) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 6) & 511L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 63L) << 3) | (block6 >>> 61));
      values[valuesOffset++] = (int) ((block6 >>> 52) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 43) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 34) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 25) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 16) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 7) & 511L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 127L) << 2) | (block7 >>> 62));
      values[valuesOffset++] = (int) ((block7 >>> 53) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 44) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 35) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 26) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 17) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 8) & 511L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 255L) << 1) | (block8 >>> 63));
      values[valuesOffset++] = (int) ((block8 >>> 54) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 45) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 36) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 27) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 18) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 9) & 511L);
      values[valuesOffset++] = (int) (block8 & 511L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 1) | (byte1 >>> 7);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 127) << 2) | (byte2 >>> 6);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 63) << 3) | (byte3 >>> 5);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 31) << 4) | (byte4 >>> 4);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 15) << 5) | (byte5 >>> 3);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 7) << 6) | (byte6 >>> 2);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 3) << 7) | (byte7 >>> 1);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 1) << 8) | byte8;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 55;
      values[valuesOffset++] = (block0 >>> 46) & 511L;
      values[valuesOffset++] = (block0 >>> 37) & 511L;
      values[valuesOffset++] = (block0 >>> 28) & 511L;
      values[valuesOffset++] = (block0 >>> 19) & 511L;
      values[valuesOffset++] = (block0 >>> 10) & 511L;
      values[valuesOffset++] = (block0 >>> 1) & 511L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 1L) << 8) | (block1 >>> 56);
      values[valuesOffset++] = (block1 >>> 47) & 511L;
      values[valuesOffset++] = (block1 >>> 38) & 511L;
      values[valuesOffset++] = (block1 >>> 29) & 511L;
      values[valuesOffset++] = (block1 >>> 20) & 511L;
      values[valuesOffset++] = (block1 >>> 11) & 511L;
      values[valuesOffset++] = (block1 >>> 2) & 511L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 3L) << 7) | (block2 >>> 57);
      values[valuesOffset++] = (block2 >>> 48) & 511L;
      values[valuesOffset++] = (block2 >>> 39) & 511L;
      values[valuesOffset++] = (block2 >>> 30) & 511L;
      values[valuesOffset++] = (block2 >>> 21) & 511L;
      values[valuesOffset++] = (block2 >>> 12) & 511L;
      values[valuesOffset++] = (block2 >>> 3) & 511L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 7L) << 6) | (block3 >>> 58);
      values[valuesOffset++] = (block3 >>> 49) & 511L;
      values[valuesOffset++] = (block3 >>> 40) & 511L;
      values[valuesOffset++] = (block3 >>> 31) & 511L;
      values[valuesOffset++] = (block3 >>> 22) & 511L;
      values[valuesOffset++] = (block3 >>> 13) & 511L;
      values[valuesOffset++] = (block3 >>> 4) & 511L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 5) | (block4 >>> 59);
      values[valuesOffset++] = (block4 >>> 50) & 511L;
      values[valuesOffset++] = (block4 >>> 41) & 511L;
      values[valuesOffset++] = (block4 >>> 32) & 511L;
      values[valuesOffset++] = (block4 >>> 23) & 511L;
      values[valuesOffset++] = (block4 >>> 14) & 511L;
      values[valuesOffset++] = (block4 >>> 5) & 511L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 31L) << 4) | (block5 >>> 60);
      values[valuesOffset++] = (block5 >>> 51) & 511L;
      values[valuesOffset++] = (block5 >>> 42) & 511L;
      values[valuesOffset++] = (block5 >>> 33) & 511L;
      values[valuesOffset++] = (block5 >>> 24) & 511L;
      values[valuesOffset++] = (block5 >>> 15) & 511L;
      values[valuesOffset++] = (block5 >>> 6) & 511L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 63L) << 3) | (block6 >>> 61);
      values[valuesOffset++] = (block6 >>> 52) & 511L;
      values[valuesOffset++] = (block6 >>> 43) & 511L;
      values[valuesOffset++] = (block6 >>> 34) & 511L;
      values[valuesOffset++] = (block6 >>> 25) & 511L;
      values[valuesOffset++] = (block6 >>> 16) & 511L;
      values[valuesOffset++] = (block6 >>> 7) & 511L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 127L) << 2) | (block7 >>> 62);
      values[valuesOffset++] = (block7 >>> 53) & 511L;
      values[valuesOffset++] = (block7 >>> 44) & 511L;
      values[valuesOffset++] = (block7 >>> 35) & 511L;
      values[valuesOffset++] = (block7 >>> 26) & 511L;
      values[valuesOffset++] = (block7 >>> 17) & 511L;
      values[valuesOffset++] = (block7 >>> 8) & 511L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 255L) << 1) | (block8 >>> 63);
      values[valuesOffset++] = (block8 >>> 54) & 511L;
      values[valuesOffset++] = (block8 >>> 45) & 511L;
      values[valuesOffset++] = (block8 >>> 36) & 511L;
      values[valuesOffset++] = (block8 >>> 27) & 511L;
      values[valuesOffset++] = (block8 >>> 18) & 511L;
      values[valuesOffset++] = (block8 >>> 9) & 511L;
      values[valuesOffset++] = block8 & 511L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 1) | (byte1 >>> 7);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 127) << 2) | (byte2 >>> 6);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 63) << 3) | (byte3 >>> 5);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 31) << 4) | (byte4 >>> 4);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 15) << 5) | (byte5 >>> 3);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 7) << 6) | (byte6 >>> 2);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 3) << 7) | (byte7 >>> 1);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 1) << 8) | byte8;
    }
  }

}
