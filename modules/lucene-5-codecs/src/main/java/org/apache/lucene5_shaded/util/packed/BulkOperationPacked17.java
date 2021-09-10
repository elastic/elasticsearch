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
final class BulkOperationPacked17 extends BulkOperationPacked {

  public BulkOperationPacked17() {
    super(17);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 47);
      values[valuesOffset++] = (int) ((block0 >>> 30) & 131071L);
      values[valuesOffset++] = (int) ((block0 >>> 13) & 131071L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 8191L) << 4) | (block1 >>> 60));
      values[valuesOffset++] = (int) ((block1 >>> 43) & 131071L);
      values[valuesOffset++] = (int) ((block1 >>> 26) & 131071L);
      values[valuesOffset++] = (int) ((block1 >>> 9) & 131071L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 511L) << 8) | (block2 >>> 56));
      values[valuesOffset++] = (int) ((block2 >>> 39) & 131071L);
      values[valuesOffset++] = (int) ((block2 >>> 22) & 131071L);
      values[valuesOffset++] = (int) ((block2 >>> 5) & 131071L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 31L) << 12) | (block3 >>> 52));
      values[valuesOffset++] = (int) ((block3 >>> 35) & 131071L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 131071L);
      values[valuesOffset++] = (int) ((block3 >>> 1) & 131071L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 1L) << 16) | (block4 >>> 48));
      values[valuesOffset++] = (int) ((block4 >>> 31) & 131071L);
      values[valuesOffset++] = (int) ((block4 >>> 14) & 131071L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 16383L) << 3) | (block5 >>> 61));
      values[valuesOffset++] = (int) ((block5 >>> 44) & 131071L);
      values[valuesOffset++] = (int) ((block5 >>> 27) & 131071L);
      values[valuesOffset++] = (int) ((block5 >>> 10) & 131071L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 1023L) << 7) | (block6 >>> 57));
      values[valuesOffset++] = (int) ((block6 >>> 40) & 131071L);
      values[valuesOffset++] = (int) ((block6 >>> 23) & 131071L);
      values[valuesOffset++] = (int) ((block6 >>> 6) & 131071L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 63L) << 11) | (block7 >>> 53));
      values[valuesOffset++] = (int) ((block7 >>> 36) & 131071L);
      values[valuesOffset++] = (int) ((block7 >>> 19) & 131071L);
      values[valuesOffset++] = (int) ((block7 >>> 2) & 131071L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 3L) << 15) | (block8 >>> 49));
      values[valuesOffset++] = (int) ((block8 >>> 32) & 131071L);
      values[valuesOffset++] = (int) ((block8 >>> 15) & 131071L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 32767L) << 2) | (block9 >>> 62));
      values[valuesOffset++] = (int) ((block9 >>> 45) & 131071L);
      values[valuesOffset++] = (int) ((block9 >>> 28) & 131071L);
      values[valuesOffset++] = (int) ((block9 >>> 11) & 131071L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 2047L) << 6) | (block10 >>> 58));
      values[valuesOffset++] = (int) ((block10 >>> 41) & 131071L);
      values[valuesOffset++] = (int) ((block10 >>> 24) & 131071L);
      values[valuesOffset++] = (int) ((block10 >>> 7) & 131071L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 127L) << 10) | (block11 >>> 54));
      values[valuesOffset++] = (int) ((block11 >>> 37) & 131071L);
      values[valuesOffset++] = (int) ((block11 >>> 20) & 131071L);
      values[valuesOffset++] = (int) ((block11 >>> 3) & 131071L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 7L) << 14) | (block12 >>> 50));
      values[valuesOffset++] = (int) ((block12 >>> 33) & 131071L);
      values[valuesOffset++] = (int) ((block12 >>> 16) & 131071L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 65535L) << 1) | (block13 >>> 63));
      values[valuesOffset++] = (int) ((block13 >>> 46) & 131071L);
      values[valuesOffset++] = (int) ((block13 >>> 29) & 131071L);
      values[valuesOffset++] = (int) ((block13 >>> 12) & 131071L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 4095L) << 5) | (block14 >>> 59));
      values[valuesOffset++] = (int) ((block14 >>> 42) & 131071L);
      values[valuesOffset++] = (int) ((block14 >>> 25) & 131071L);
      values[valuesOffset++] = (int) ((block14 >>> 8) & 131071L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 255L) << 9) | (block15 >>> 55));
      values[valuesOffset++] = (int) ((block15 >>> 38) & 131071L);
      values[valuesOffset++] = (int) ((block15 >>> 21) & 131071L);
      values[valuesOffset++] = (int) ((block15 >>> 4) & 131071L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 15L) << 13) | (block16 >>> 51));
      values[valuesOffset++] = (int) ((block16 >>> 34) & 131071L);
      values[valuesOffset++] = (int) ((block16 >>> 17) & 131071L);
      values[valuesOffset++] = (int) (block16 & 131071L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 9) | (byte1 << 1) | (byte2 >>> 7);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 127) << 10) | (byte3 << 2) | (byte4 >>> 6);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 63) << 11) | (byte5 << 3) | (byte6 >>> 5);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 31) << 12) | (byte7 << 4) | (byte8 >>> 4);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 15) << 13) | (byte9 << 5) | (byte10 >>> 3);
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 7) << 14) | (byte11 << 6) | (byte12 >>> 2);
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte12 & 3) << 15) | (byte13 << 7) | (byte14 >>> 1);
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 1) << 16) | (byte15 << 8) | byte16;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 47;
      values[valuesOffset++] = (block0 >>> 30) & 131071L;
      values[valuesOffset++] = (block0 >>> 13) & 131071L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 8191L) << 4) | (block1 >>> 60);
      values[valuesOffset++] = (block1 >>> 43) & 131071L;
      values[valuesOffset++] = (block1 >>> 26) & 131071L;
      values[valuesOffset++] = (block1 >>> 9) & 131071L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 511L) << 8) | (block2 >>> 56);
      values[valuesOffset++] = (block2 >>> 39) & 131071L;
      values[valuesOffset++] = (block2 >>> 22) & 131071L;
      values[valuesOffset++] = (block2 >>> 5) & 131071L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 31L) << 12) | (block3 >>> 52);
      values[valuesOffset++] = (block3 >>> 35) & 131071L;
      values[valuesOffset++] = (block3 >>> 18) & 131071L;
      values[valuesOffset++] = (block3 >>> 1) & 131071L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 1L) << 16) | (block4 >>> 48);
      values[valuesOffset++] = (block4 >>> 31) & 131071L;
      values[valuesOffset++] = (block4 >>> 14) & 131071L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 16383L) << 3) | (block5 >>> 61);
      values[valuesOffset++] = (block5 >>> 44) & 131071L;
      values[valuesOffset++] = (block5 >>> 27) & 131071L;
      values[valuesOffset++] = (block5 >>> 10) & 131071L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 1023L) << 7) | (block6 >>> 57);
      values[valuesOffset++] = (block6 >>> 40) & 131071L;
      values[valuesOffset++] = (block6 >>> 23) & 131071L;
      values[valuesOffset++] = (block6 >>> 6) & 131071L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 63L) << 11) | (block7 >>> 53);
      values[valuesOffset++] = (block7 >>> 36) & 131071L;
      values[valuesOffset++] = (block7 >>> 19) & 131071L;
      values[valuesOffset++] = (block7 >>> 2) & 131071L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 3L) << 15) | (block8 >>> 49);
      values[valuesOffset++] = (block8 >>> 32) & 131071L;
      values[valuesOffset++] = (block8 >>> 15) & 131071L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 32767L) << 2) | (block9 >>> 62);
      values[valuesOffset++] = (block9 >>> 45) & 131071L;
      values[valuesOffset++] = (block9 >>> 28) & 131071L;
      values[valuesOffset++] = (block9 >>> 11) & 131071L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 2047L) << 6) | (block10 >>> 58);
      values[valuesOffset++] = (block10 >>> 41) & 131071L;
      values[valuesOffset++] = (block10 >>> 24) & 131071L;
      values[valuesOffset++] = (block10 >>> 7) & 131071L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 127L) << 10) | (block11 >>> 54);
      values[valuesOffset++] = (block11 >>> 37) & 131071L;
      values[valuesOffset++] = (block11 >>> 20) & 131071L;
      values[valuesOffset++] = (block11 >>> 3) & 131071L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 7L) << 14) | (block12 >>> 50);
      values[valuesOffset++] = (block12 >>> 33) & 131071L;
      values[valuesOffset++] = (block12 >>> 16) & 131071L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 65535L) << 1) | (block13 >>> 63);
      values[valuesOffset++] = (block13 >>> 46) & 131071L;
      values[valuesOffset++] = (block13 >>> 29) & 131071L;
      values[valuesOffset++] = (block13 >>> 12) & 131071L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 4095L) << 5) | (block14 >>> 59);
      values[valuesOffset++] = (block14 >>> 42) & 131071L;
      values[valuesOffset++] = (block14 >>> 25) & 131071L;
      values[valuesOffset++] = (block14 >>> 8) & 131071L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 255L) << 9) | (block15 >>> 55);
      values[valuesOffset++] = (block15 >>> 38) & 131071L;
      values[valuesOffset++] = (block15 >>> 21) & 131071L;
      values[valuesOffset++] = (block15 >>> 4) & 131071L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 15L) << 13) | (block16 >>> 51);
      values[valuesOffset++] = (block16 >>> 34) & 131071L;
      values[valuesOffset++] = (block16 >>> 17) & 131071L;
      values[valuesOffset++] = block16 & 131071L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 9) | (byte1 << 1) | (byte2 >>> 7);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 127) << 10) | (byte3 << 2) | (byte4 >>> 6);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 63) << 11) | (byte5 << 3) | (byte6 >>> 5);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 31) << 12) | (byte7 << 4) | (byte8 >>> 4);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 15) << 13) | (byte9 << 5) | (byte10 >>> 3);
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 7) << 14) | (byte11 << 6) | (byte12 >>> 2);
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte12 & 3) << 15) | (byte13 << 7) | (byte14 >>> 1);
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 1) << 16) | (byte15 << 8) | byte16;
    }
  }

}
