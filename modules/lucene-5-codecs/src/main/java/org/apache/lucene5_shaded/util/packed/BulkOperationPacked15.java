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
final class BulkOperationPacked15 extends BulkOperationPacked {

  public BulkOperationPacked15() {
    super(15);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 49);
      values[valuesOffset++] = (int) ((block0 >>> 34) & 32767L);
      values[valuesOffset++] = (int) ((block0 >>> 19) & 32767L);
      values[valuesOffset++] = (int) ((block0 >>> 4) & 32767L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 15L) << 11) | (block1 >>> 53));
      values[valuesOffset++] = (int) ((block1 >>> 38) & 32767L);
      values[valuesOffset++] = (int) ((block1 >>> 23) & 32767L);
      values[valuesOffset++] = (int) ((block1 >>> 8) & 32767L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 255L) << 7) | (block2 >>> 57));
      values[valuesOffset++] = (int) ((block2 >>> 42) & 32767L);
      values[valuesOffset++] = (int) ((block2 >>> 27) & 32767L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 32767L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 4095L) << 3) | (block3 >>> 61));
      values[valuesOffset++] = (int) ((block3 >>> 46) & 32767L);
      values[valuesOffset++] = (int) ((block3 >>> 31) & 32767L);
      values[valuesOffset++] = (int) ((block3 >>> 16) & 32767L);
      values[valuesOffset++] = (int) ((block3 >>> 1) & 32767L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 1L) << 14) | (block4 >>> 50));
      values[valuesOffset++] = (int) ((block4 >>> 35) & 32767L);
      values[valuesOffset++] = (int) ((block4 >>> 20) & 32767L);
      values[valuesOffset++] = (int) ((block4 >>> 5) & 32767L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 31L) << 10) | (block5 >>> 54));
      values[valuesOffset++] = (int) ((block5 >>> 39) & 32767L);
      values[valuesOffset++] = (int) ((block5 >>> 24) & 32767L);
      values[valuesOffset++] = (int) ((block5 >>> 9) & 32767L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 511L) << 6) | (block6 >>> 58));
      values[valuesOffset++] = (int) ((block6 >>> 43) & 32767L);
      values[valuesOffset++] = (int) ((block6 >>> 28) & 32767L);
      values[valuesOffset++] = (int) ((block6 >>> 13) & 32767L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 8191L) << 2) | (block7 >>> 62));
      values[valuesOffset++] = (int) ((block7 >>> 47) & 32767L);
      values[valuesOffset++] = (int) ((block7 >>> 32) & 32767L);
      values[valuesOffset++] = (int) ((block7 >>> 17) & 32767L);
      values[valuesOffset++] = (int) ((block7 >>> 2) & 32767L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 3L) << 13) | (block8 >>> 51));
      values[valuesOffset++] = (int) ((block8 >>> 36) & 32767L);
      values[valuesOffset++] = (int) ((block8 >>> 21) & 32767L);
      values[valuesOffset++] = (int) ((block8 >>> 6) & 32767L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 63L) << 9) | (block9 >>> 55));
      values[valuesOffset++] = (int) ((block9 >>> 40) & 32767L);
      values[valuesOffset++] = (int) ((block9 >>> 25) & 32767L);
      values[valuesOffset++] = (int) ((block9 >>> 10) & 32767L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 1023L) << 5) | (block10 >>> 59));
      values[valuesOffset++] = (int) ((block10 >>> 44) & 32767L);
      values[valuesOffset++] = (int) ((block10 >>> 29) & 32767L);
      values[valuesOffset++] = (int) ((block10 >>> 14) & 32767L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 16383L) << 1) | (block11 >>> 63));
      values[valuesOffset++] = (int) ((block11 >>> 48) & 32767L);
      values[valuesOffset++] = (int) ((block11 >>> 33) & 32767L);
      values[valuesOffset++] = (int) ((block11 >>> 18) & 32767L);
      values[valuesOffset++] = (int) ((block11 >>> 3) & 32767L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 7L) << 12) | (block12 >>> 52));
      values[valuesOffset++] = (int) ((block12 >>> 37) & 32767L);
      values[valuesOffset++] = (int) ((block12 >>> 22) & 32767L);
      values[valuesOffset++] = (int) ((block12 >>> 7) & 32767L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 127L) << 8) | (block13 >>> 56));
      values[valuesOffset++] = (int) ((block13 >>> 41) & 32767L);
      values[valuesOffset++] = (int) ((block13 >>> 26) & 32767L);
      values[valuesOffset++] = (int) ((block13 >>> 11) & 32767L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 2047L) << 4) | (block14 >>> 60));
      values[valuesOffset++] = (int) ((block14 >>> 45) & 32767L);
      values[valuesOffset++] = (int) ((block14 >>> 30) & 32767L);
      values[valuesOffset++] = (int) ((block14 >>> 15) & 32767L);
      values[valuesOffset++] = (int) (block14 & 32767L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 7) | (byte1 >>> 1);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 1) << 14) | (byte2 << 6) | (byte3 >>> 2);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 3) << 13) | (byte4 << 5) | (byte5 >>> 3);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 7) << 12) | (byte6 << 4) | (byte7 >>> 4);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 15) << 11) | (byte8 << 3) | (byte9 >>> 5);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 31) << 10) | (byte10 << 2) | (byte11 >>> 6);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 63) << 9) | (byte12 << 1) | (byte13 >>> 7);
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 127) << 8) | byte14;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 49;
      values[valuesOffset++] = (block0 >>> 34) & 32767L;
      values[valuesOffset++] = (block0 >>> 19) & 32767L;
      values[valuesOffset++] = (block0 >>> 4) & 32767L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 15L) << 11) | (block1 >>> 53);
      values[valuesOffset++] = (block1 >>> 38) & 32767L;
      values[valuesOffset++] = (block1 >>> 23) & 32767L;
      values[valuesOffset++] = (block1 >>> 8) & 32767L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 255L) << 7) | (block2 >>> 57);
      values[valuesOffset++] = (block2 >>> 42) & 32767L;
      values[valuesOffset++] = (block2 >>> 27) & 32767L;
      values[valuesOffset++] = (block2 >>> 12) & 32767L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 4095L) << 3) | (block3 >>> 61);
      values[valuesOffset++] = (block3 >>> 46) & 32767L;
      values[valuesOffset++] = (block3 >>> 31) & 32767L;
      values[valuesOffset++] = (block3 >>> 16) & 32767L;
      values[valuesOffset++] = (block3 >>> 1) & 32767L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 1L) << 14) | (block4 >>> 50);
      values[valuesOffset++] = (block4 >>> 35) & 32767L;
      values[valuesOffset++] = (block4 >>> 20) & 32767L;
      values[valuesOffset++] = (block4 >>> 5) & 32767L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 31L) << 10) | (block5 >>> 54);
      values[valuesOffset++] = (block5 >>> 39) & 32767L;
      values[valuesOffset++] = (block5 >>> 24) & 32767L;
      values[valuesOffset++] = (block5 >>> 9) & 32767L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 511L) << 6) | (block6 >>> 58);
      values[valuesOffset++] = (block6 >>> 43) & 32767L;
      values[valuesOffset++] = (block6 >>> 28) & 32767L;
      values[valuesOffset++] = (block6 >>> 13) & 32767L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 8191L) << 2) | (block7 >>> 62);
      values[valuesOffset++] = (block7 >>> 47) & 32767L;
      values[valuesOffset++] = (block7 >>> 32) & 32767L;
      values[valuesOffset++] = (block7 >>> 17) & 32767L;
      values[valuesOffset++] = (block7 >>> 2) & 32767L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 3L) << 13) | (block8 >>> 51);
      values[valuesOffset++] = (block8 >>> 36) & 32767L;
      values[valuesOffset++] = (block8 >>> 21) & 32767L;
      values[valuesOffset++] = (block8 >>> 6) & 32767L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 63L) << 9) | (block9 >>> 55);
      values[valuesOffset++] = (block9 >>> 40) & 32767L;
      values[valuesOffset++] = (block9 >>> 25) & 32767L;
      values[valuesOffset++] = (block9 >>> 10) & 32767L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 1023L) << 5) | (block10 >>> 59);
      values[valuesOffset++] = (block10 >>> 44) & 32767L;
      values[valuesOffset++] = (block10 >>> 29) & 32767L;
      values[valuesOffset++] = (block10 >>> 14) & 32767L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 16383L) << 1) | (block11 >>> 63);
      values[valuesOffset++] = (block11 >>> 48) & 32767L;
      values[valuesOffset++] = (block11 >>> 33) & 32767L;
      values[valuesOffset++] = (block11 >>> 18) & 32767L;
      values[valuesOffset++] = (block11 >>> 3) & 32767L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 7L) << 12) | (block12 >>> 52);
      values[valuesOffset++] = (block12 >>> 37) & 32767L;
      values[valuesOffset++] = (block12 >>> 22) & 32767L;
      values[valuesOffset++] = (block12 >>> 7) & 32767L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 127L) << 8) | (block13 >>> 56);
      values[valuesOffset++] = (block13 >>> 41) & 32767L;
      values[valuesOffset++] = (block13 >>> 26) & 32767L;
      values[valuesOffset++] = (block13 >>> 11) & 32767L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 2047L) << 4) | (block14 >>> 60);
      values[valuesOffset++] = (block14 >>> 45) & 32767L;
      values[valuesOffset++] = (block14 >>> 30) & 32767L;
      values[valuesOffset++] = (block14 >>> 15) & 32767L;
      values[valuesOffset++] = block14 & 32767L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 7) | (byte1 >>> 1);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 1) << 14) | (byte2 << 6) | (byte3 >>> 2);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 3) << 13) | (byte4 << 5) | (byte5 >>> 3);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 7) << 12) | (byte6 << 4) | (byte7 >>> 4);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 15) << 11) | (byte8 << 3) | (byte9 >>> 5);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 31) << 10) | (byte10 << 2) | (byte11 >>> 6);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 63) << 9) | (byte12 << 1) | (byte13 >>> 7);
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 127) << 8) | byte14;
    }
  }

}
