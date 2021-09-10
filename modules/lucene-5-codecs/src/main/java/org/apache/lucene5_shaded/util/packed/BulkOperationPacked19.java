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
final class BulkOperationPacked19 extends BulkOperationPacked {

  public BulkOperationPacked19() {
    super(19);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 45);
      values[valuesOffset++] = (int) ((block0 >>> 26) & 524287L);
      values[valuesOffset++] = (int) ((block0 >>> 7) & 524287L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 127L) << 12) | (block1 >>> 52));
      values[valuesOffset++] = (int) ((block1 >>> 33) & 524287L);
      values[valuesOffset++] = (int) ((block1 >>> 14) & 524287L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 16383L) << 5) | (block2 >>> 59));
      values[valuesOffset++] = (int) ((block2 >>> 40) & 524287L);
      values[valuesOffset++] = (int) ((block2 >>> 21) & 524287L);
      values[valuesOffset++] = (int) ((block2 >>> 2) & 524287L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 3L) << 17) | (block3 >>> 47));
      values[valuesOffset++] = (int) ((block3 >>> 28) & 524287L);
      values[valuesOffset++] = (int) ((block3 >>> 9) & 524287L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 511L) << 10) | (block4 >>> 54));
      values[valuesOffset++] = (int) ((block4 >>> 35) & 524287L);
      values[valuesOffset++] = (int) ((block4 >>> 16) & 524287L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 65535L) << 3) | (block5 >>> 61));
      values[valuesOffset++] = (int) ((block5 >>> 42) & 524287L);
      values[valuesOffset++] = (int) ((block5 >>> 23) & 524287L);
      values[valuesOffset++] = (int) ((block5 >>> 4) & 524287L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 15L) << 15) | (block6 >>> 49));
      values[valuesOffset++] = (int) ((block6 >>> 30) & 524287L);
      values[valuesOffset++] = (int) ((block6 >>> 11) & 524287L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 2047L) << 8) | (block7 >>> 56));
      values[valuesOffset++] = (int) ((block7 >>> 37) & 524287L);
      values[valuesOffset++] = (int) ((block7 >>> 18) & 524287L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 262143L) << 1) | (block8 >>> 63));
      values[valuesOffset++] = (int) ((block8 >>> 44) & 524287L);
      values[valuesOffset++] = (int) ((block8 >>> 25) & 524287L);
      values[valuesOffset++] = (int) ((block8 >>> 6) & 524287L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 63L) << 13) | (block9 >>> 51));
      values[valuesOffset++] = (int) ((block9 >>> 32) & 524287L);
      values[valuesOffset++] = (int) ((block9 >>> 13) & 524287L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 8191L) << 6) | (block10 >>> 58));
      values[valuesOffset++] = (int) ((block10 >>> 39) & 524287L);
      values[valuesOffset++] = (int) ((block10 >>> 20) & 524287L);
      values[valuesOffset++] = (int) ((block10 >>> 1) & 524287L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 1L) << 18) | (block11 >>> 46));
      values[valuesOffset++] = (int) ((block11 >>> 27) & 524287L);
      values[valuesOffset++] = (int) ((block11 >>> 8) & 524287L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 255L) << 11) | (block12 >>> 53));
      values[valuesOffset++] = (int) ((block12 >>> 34) & 524287L);
      values[valuesOffset++] = (int) ((block12 >>> 15) & 524287L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 32767L) << 4) | (block13 >>> 60));
      values[valuesOffset++] = (int) ((block13 >>> 41) & 524287L);
      values[valuesOffset++] = (int) ((block13 >>> 22) & 524287L);
      values[valuesOffset++] = (int) ((block13 >>> 3) & 524287L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 7L) << 16) | (block14 >>> 48));
      values[valuesOffset++] = (int) ((block14 >>> 29) & 524287L);
      values[valuesOffset++] = (int) ((block14 >>> 10) & 524287L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 1023L) << 9) | (block15 >>> 55));
      values[valuesOffset++] = (int) ((block15 >>> 36) & 524287L);
      values[valuesOffset++] = (int) ((block15 >>> 17) & 524287L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 131071L) << 2) | (block16 >>> 62));
      values[valuesOffset++] = (int) ((block16 >>> 43) & 524287L);
      values[valuesOffset++] = (int) ((block16 >>> 24) & 524287L);
      values[valuesOffset++] = (int) ((block16 >>> 5) & 524287L);
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 31L) << 14) | (block17 >>> 50));
      values[valuesOffset++] = (int) ((block17 >>> 31) & 524287L);
      values[valuesOffset++] = (int) ((block17 >>> 12) & 524287L);
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block17 & 4095L) << 7) | (block18 >>> 57));
      values[valuesOffset++] = (int) ((block18 >>> 38) & 524287L);
      values[valuesOffset++] = (int) ((block18 >>> 19) & 524287L);
      values[valuesOffset++] = (int) (block18 & 524287L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 11) | (byte1 << 3) | (byte2 >>> 5);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 31) << 14) | (byte3 << 6) | (byte4 >>> 2);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 3) << 17) | (byte5 << 9) | (byte6 << 1) | (byte7 >>> 7);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 127) << 12) | (byte8 << 4) | (byte9 >>> 4);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 15) << 15) | (byte10 << 7) | (byte11 >>> 1);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 1) << 18) | (byte12 << 10) | (byte13 << 2) | (byte14 >>> 6);
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 63) << 13) | (byte15 << 5) | (byte16 >>> 3);
      final int byte17 = blocks[blocksOffset++] & 0xFF;
      final int byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte16 & 7) << 16) | (byte17 << 8) | byte18;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 45;
      values[valuesOffset++] = (block0 >>> 26) & 524287L;
      values[valuesOffset++] = (block0 >>> 7) & 524287L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 127L) << 12) | (block1 >>> 52);
      values[valuesOffset++] = (block1 >>> 33) & 524287L;
      values[valuesOffset++] = (block1 >>> 14) & 524287L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 16383L) << 5) | (block2 >>> 59);
      values[valuesOffset++] = (block2 >>> 40) & 524287L;
      values[valuesOffset++] = (block2 >>> 21) & 524287L;
      values[valuesOffset++] = (block2 >>> 2) & 524287L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 3L) << 17) | (block3 >>> 47);
      values[valuesOffset++] = (block3 >>> 28) & 524287L;
      values[valuesOffset++] = (block3 >>> 9) & 524287L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 511L) << 10) | (block4 >>> 54);
      values[valuesOffset++] = (block4 >>> 35) & 524287L;
      values[valuesOffset++] = (block4 >>> 16) & 524287L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 65535L) << 3) | (block5 >>> 61);
      values[valuesOffset++] = (block5 >>> 42) & 524287L;
      values[valuesOffset++] = (block5 >>> 23) & 524287L;
      values[valuesOffset++] = (block5 >>> 4) & 524287L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 15L) << 15) | (block6 >>> 49);
      values[valuesOffset++] = (block6 >>> 30) & 524287L;
      values[valuesOffset++] = (block6 >>> 11) & 524287L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 2047L) << 8) | (block7 >>> 56);
      values[valuesOffset++] = (block7 >>> 37) & 524287L;
      values[valuesOffset++] = (block7 >>> 18) & 524287L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 262143L) << 1) | (block8 >>> 63);
      values[valuesOffset++] = (block8 >>> 44) & 524287L;
      values[valuesOffset++] = (block8 >>> 25) & 524287L;
      values[valuesOffset++] = (block8 >>> 6) & 524287L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 63L) << 13) | (block9 >>> 51);
      values[valuesOffset++] = (block9 >>> 32) & 524287L;
      values[valuesOffset++] = (block9 >>> 13) & 524287L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 8191L) << 6) | (block10 >>> 58);
      values[valuesOffset++] = (block10 >>> 39) & 524287L;
      values[valuesOffset++] = (block10 >>> 20) & 524287L;
      values[valuesOffset++] = (block10 >>> 1) & 524287L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 1L) << 18) | (block11 >>> 46);
      values[valuesOffset++] = (block11 >>> 27) & 524287L;
      values[valuesOffset++] = (block11 >>> 8) & 524287L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 255L) << 11) | (block12 >>> 53);
      values[valuesOffset++] = (block12 >>> 34) & 524287L;
      values[valuesOffset++] = (block12 >>> 15) & 524287L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 32767L) << 4) | (block13 >>> 60);
      values[valuesOffset++] = (block13 >>> 41) & 524287L;
      values[valuesOffset++] = (block13 >>> 22) & 524287L;
      values[valuesOffset++] = (block13 >>> 3) & 524287L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 7L) << 16) | (block14 >>> 48);
      values[valuesOffset++] = (block14 >>> 29) & 524287L;
      values[valuesOffset++] = (block14 >>> 10) & 524287L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 1023L) << 9) | (block15 >>> 55);
      values[valuesOffset++] = (block15 >>> 36) & 524287L;
      values[valuesOffset++] = (block15 >>> 17) & 524287L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 131071L) << 2) | (block16 >>> 62);
      values[valuesOffset++] = (block16 >>> 43) & 524287L;
      values[valuesOffset++] = (block16 >>> 24) & 524287L;
      values[valuesOffset++] = (block16 >>> 5) & 524287L;
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 31L) << 14) | (block17 >>> 50);
      values[valuesOffset++] = (block17 >>> 31) & 524287L;
      values[valuesOffset++] = (block17 >>> 12) & 524287L;
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block17 & 4095L) << 7) | (block18 >>> 57);
      values[valuesOffset++] = (block18 >>> 38) & 524287L;
      values[valuesOffset++] = (block18 >>> 19) & 524287L;
      values[valuesOffset++] = block18 & 524287L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 11) | (byte1 << 3) | (byte2 >>> 5);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 31) << 14) | (byte3 << 6) | (byte4 >>> 2);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 3) << 17) | (byte5 << 9) | (byte6 << 1) | (byte7 >>> 7);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 127) << 12) | (byte8 << 4) | (byte9 >>> 4);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 15) << 15) | (byte10 << 7) | (byte11 >>> 1);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 1) << 18) | (byte12 << 10) | (byte13 << 2) | (byte14 >>> 6);
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 63) << 13) | (byte15 << 5) | (byte16 >>> 3);
      final long byte17 = blocks[blocksOffset++] & 0xFF;
      final long byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte16 & 7) << 16) | (byte17 << 8) | byte18;
    }
  }

}
