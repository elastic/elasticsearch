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
final class BulkOperationPacked11 extends BulkOperationPacked {

  public BulkOperationPacked11() {
    super(11);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 53);
      values[valuesOffset++] = (int) ((block0 >>> 42) & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 31) & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 20) & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 9) & 2047L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 511L) << 2) | (block1 >>> 62));
      values[valuesOffset++] = (int) ((block1 >>> 51) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 40) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 29) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 18) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 7) & 2047L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 127L) << 4) | (block2 >>> 60));
      values[valuesOffset++] = (int) ((block2 >>> 49) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 38) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 27) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 16) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 5) & 2047L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 31L) << 6) | (block3 >>> 58));
      values[valuesOffset++] = (int) ((block3 >>> 47) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 36) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 25) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 14) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 3) & 2047L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 7L) << 8) | (block4 >>> 56));
      values[valuesOffset++] = (int) ((block4 >>> 45) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 34) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 23) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 12) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 1) & 2047L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 1L) << 10) | (block5 >>> 54));
      values[valuesOffset++] = (int) ((block5 >>> 43) & 2047L);
      values[valuesOffset++] = (int) ((block5 >>> 32) & 2047L);
      values[valuesOffset++] = (int) ((block5 >>> 21) & 2047L);
      values[valuesOffset++] = (int) ((block5 >>> 10) & 2047L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 1023L) << 1) | (block6 >>> 63));
      values[valuesOffset++] = (int) ((block6 >>> 52) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 41) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 30) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 19) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 8) & 2047L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 255L) << 3) | (block7 >>> 61));
      values[valuesOffset++] = (int) ((block7 >>> 50) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 39) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 28) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 17) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 6) & 2047L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 63L) << 5) | (block8 >>> 59));
      values[valuesOffset++] = (int) ((block8 >>> 48) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 37) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 26) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 15) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 4) & 2047L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 15L) << 7) | (block9 >>> 57));
      values[valuesOffset++] = (int) ((block9 >>> 46) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 35) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 24) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 13) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 2) & 2047L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 3L) << 9) | (block10 >>> 55));
      values[valuesOffset++] = (int) ((block10 >>> 44) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 33) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 22) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 11) & 2047L);
      values[valuesOffset++] = (int) (block10 & 2047L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 3) | (byte1 >>> 5);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 31) << 6) | (byte2 >>> 2);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 3) << 9) | (byte3 << 1) | (byte4 >>> 7);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 127) << 4) | (byte5 >>> 4);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 15) << 7) | (byte6 >>> 1);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 1) << 10) | (byte7 << 2) | (byte8 >>> 6);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 63) << 5) | (byte9 >>> 3);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 7) << 8) | byte10;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 53;
      values[valuesOffset++] = (block0 >>> 42) & 2047L;
      values[valuesOffset++] = (block0 >>> 31) & 2047L;
      values[valuesOffset++] = (block0 >>> 20) & 2047L;
      values[valuesOffset++] = (block0 >>> 9) & 2047L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 511L) << 2) | (block1 >>> 62);
      values[valuesOffset++] = (block1 >>> 51) & 2047L;
      values[valuesOffset++] = (block1 >>> 40) & 2047L;
      values[valuesOffset++] = (block1 >>> 29) & 2047L;
      values[valuesOffset++] = (block1 >>> 18) & 2047L;
      values[valuesOffset++] = (block1 >>> 7) & 2047L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 127L) << 4) | (block2 >>> 60);
      values[valuesOffset++] = (block2 >>> 49) & 2047L;
      values[valuesOffset++] = (block2 >>> 38) & 2047L;
      values[valuesOffset++] = (block2 >>> 27) & 2047L;
      values[valuesOffset++] = (block2 >>> 16) & 2047L;
      values[valuesOffset++] = (block2 >>> 5) & 2047L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 31L) << 6) | (block3 >>> 58);
      values[valuesOffset++] = (block3 >>> 47) & 2047L;
      values[valuesOffset++] = (block3 >>> 36) & 2047L;
      values[valuesOffset++] = (block3 >>> 25) & 2047L;
      values[valuesOffset++] = (block3 >>> 14) & 2047L;
      values[valuesOffset++] = (block3 >>> 3) & 2047L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 7L) << 8) | (block4 >>> 56);
      values[valuesOffset++] = (block4 >>> 45) & 2047L;
      values[valuesOffset++] = (block4 >>> 34) & 2047L;
      values[valuesOffset++] = (block4 >>> 23) & 2047L;
      values[valuesOffset++] = (block4 >>> 12) & 2047L;
      values[valuesOffset++] = (block4 >>> 1) & 2047L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 1L) << 10) | (block5 >>> 54);
      values[valuesOffset++] = (block5 >>> 43) & 2047L;
      values[valuesOffset++] = (block5 >>> 32) & 2047L;
      values[valuesOffset++] = (block5 >>> 21) & 2047L;
      values[valuesOffset++] = (block5 >>> 10) & 2047L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 1023L) << 1) | (block6 >>> 63);
      values[valuesOffset++] = (block6 >>> 52) & 2047L;
      values[valuesOffset++] = (block6 >>> 41) & 2047L;
      values[valuesOffset++] = (block6 >>> 30) & 2047L;
      values[valuesOffset++] = (block6 >>> 19) & 2047L;
      values[valuesOffset++] = (block6 >>> 8) & 2047L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 255L) << 3) | (block7 >>> 61);
      values[valuesOffset++] = (block7 >>> 50) & 2047L;
      values[valuesOffset++] = (block7 >>> 39) & 2047L;
      values[valuesOffset++] = (block7 >>> 28) & 2047L;
      values[valuesOffset++] = (block7 >>> 17) & 2047L;
      values[valuesOffset++] = (block7 >>> 6) & 2047L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 63L) << 5) | (block8 >>> 59);
      values[valuesOffset++] = (block8 >>> 48) & 2047L;
      values[valuesOffset++] = (block8 >>> 37) & 2047L;
      values[valuesOffset++] = (block8 >>> 26) & 2047L;
      values[valuesOffset++] = (block8 >>> 15) & 2047L;
      values[valuesOffset++] = (block8 >>> 4) & 2047L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 15L) << 7) | (block9 >>> 57);
      values[valuesOffset++] = (block9 >>> 46) & 2047L;
      values[valuesOffset++] = (block9 >>> 35) & 2047L;
      values[valuesOffset++] = (block9 >>> 24) & 2047L;
      values[valuesOffset++] = (block9 >>> 13) & 2047L;
      values[valuesOffset++] = (block9 >>> 2) & 2047L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 3L) << 9) | (block10 >>> 55);
      values[valuesOffset++] = (block10 >>> 44) & 2047L;
      values[valuesOffset++] = (block10 >>> 33) & 2047L;
      values[valuesOffset++] = (block10 >>> 22) & 2047L;
      values[valuesOffset++] = (block10 >>> 11) & 2047L;
      values[valuesOffset++] = block10 & 2047L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 3) | (byte1 >>> 5);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 31) << 6) | (byte2 >>> 2);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 3) << 9) | (byte3 << 1) | (byte4 >>> 7);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 127) << 4) | (byte5 >>> 4);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 15) << 7) | (byte6 >>> 1);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 1) << 10) | (byte7 << 2) | (byte8 >>> 6);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 63) << 5) | (byte9 >>> 3);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 7) << 8) | byte10;
    }
  }

}
