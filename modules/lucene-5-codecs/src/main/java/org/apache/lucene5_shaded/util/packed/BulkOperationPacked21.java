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
final class BulkOperationPacked21 extends BulkOperationPacked {

  public BulkOperationPacked21() {
    super(21);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 43);
      values[valuesOffset++] = (int) ((block0 >>> 22) & 2097151L);
      values[valuesOffset++] = (int) ((block0 >>> 1) & 2097151L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 1L) << 20) | (block1 >>> 44));
      values[valuesOffset++] = (int) ((block1 >>> 23) & 2097151L);
      values[valuesOffset++] = (int) ((block1 >>> 2) & 2097151L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 3L) << 19) | (block2 >>> 45));
      values[valuesOffset++] = (int) ((block2 >>> 24) & 2097151L);
      values[valuesOffset++] = (int) ((block2 >>> 3) & 2097151L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 7L) << 18) | (block3 >>> 46));
      values[valuesOffset++] = (int) ((block3 >>> 25) & 2097151L);
      values[valuesOffset++] = (int) ((block3 >>> 4) & 2097151L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 17) | (block4 >>> 47));
      values[valuesOffset++] = (int) ((block4 >>> 26) & 2097151L);
      values[valuesOffset++] = (int) ((block4 >>> 5) & 2097151L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 31L) << 16) | (block5 >>> 48));
      values[valuesOffset++] = (int) ((block5 >>> 27) & 2097151L);
      values[valuesOffset++] = (int) ((block5 >>> 6) & 2097151L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 63L) << 15) | (block6 >>> 49));
      values[valuesOffset++] = (int) ((block6 >>> 28) & 2097151L);
      values[valuesOffset++] = (int) ((block6 >>> 7) & 2097151L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 127L) << 14) | (block7 >>> 50));
      values[valuesOffset++] = (int) ((block7 >>> 29) & 2097151L);
      values[valuesOffset++] = (int) ((block7 >>> 8) & 2097151L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 255L) << 13) | (block8 >>> 51));
      values[valuesOffset++] = (int) ((block8 >>> 30) & 2097151L);
      values[valuesOffset++] = (int) ((block8 >>> 9) & 2097151L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 511L) << 12) | (block9 >>> 52));
      values[valuesOffset++] = (int) ((block9 >>> 31) & 2097151L);
      values[valuesOffset++] = (int) ((block9 >>> 10) & 2097151L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 1023L) << 11) | (block10 >>> 53));
      values[valuesOffset++] = (int) ((block10 >>> 32) & 2097151L);
      values[valuesOffset++] = (int) ((block10 >>> 11) & 2097151L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 2047L) << 10) | (block11 >>> 54));
      values[valuesOffset++] = (int) ((block11 >>> 33) & 2097151L);
      values[valuesOffset++] = (int) ((block11 >>> 12) & 2097151L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 4095L) << 9) | (block12 >>> 55));
      values[valuesOffset++] = (int) ((block12 >>> 34) & 2097151L);
      values[valuesOffset++] = (int) ((block12 >>> 13) & 2097151L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 8191L) << 8) | (block13 >>> 56));
      values[valuesOffset++] = (int) ((block13 >>> 35) & 2097151L);
      values[valuesOffset++] = (int) ((block13 >>> 14) & 2097151L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 16383L) << 7) | (block14 >>> 57));
      values[valuesOffset++] = (int) ((block14 >>> 36) & 2097151L);
      values[valuesOffset++] = (int) ((block14 >>> 15) & 2097151L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 32767L) << 6) | (block15 >>> 58));
      values[valuesOffset++] = (int) ((block15 >>> 37) & 2097151L);
      values[valuesOffset++] = (int) ((block15 >>> 16) & 2097151L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 65535L) << 5) | (block16 >>> 59));
      values[valuesOffset++] = (int) ((block16 >>> 38) & 2097151L);
      values[valuesOffset++] = (int) ((block16 >>> 17) & 2097151L);
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 131071L) << 4) | (block17 >>> 60));
      values[valuesOffset++] = (int) ((block17 >>> 39) & 2097151L);
      values[valuesOffset++] = (int) ((block17 >>> 18) & 2097151L);
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block17 & 262143L) << 3) | (block18 >>> 61));
      values[valuesOffset++] = (int) ((block18 >>> 40) & 2097151L);
      values[valuesOffset++] = (int) ((block18 >>> 19) & 2097151L);
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block18 & 524287L) << 2) | (block19 >>> 62));
      values[valuesOffset++] = (int) ((block19 >>> 41) & 2097151L);
      values[valuesOffset++] = (int) ((block19 >>> 20) & 2097151L);
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block19 & 1048575L) << 1) | (block20 >>> 63));
      values[valuesOffset++] = (int) ((block20 >>> 42) & 2097151L);
      values[valuesOffset++] = (int) ((block20 >>> 21) & 2097151L);
      values[valuesOffset++] = (int) (block20 & 2097151L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 13) | (byte1 << 5) | (byte2 >>> 3);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 7) << 18) | (byte3 << 10) | (byte4 << 2) | (byte5 >>> 6);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 15) | (byte6 << 7) | (byte7 >>> 1);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 1) << 20) | (byte8 << 12) | (byte9 << 4) | (byte10 >>> 4);
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 15) << 17) | (byte11 << 9) | (byte12 << 1) | (byte13 >>> 7);
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 127) << 14) | (byte14 << 6) | (byte15 >>> 2);
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      final int byte17 = blocks[blocksOffset++] & 0xFF;
      final int byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte15 & 3) << 19) | (byte16 << 11) | (byte17 << 3) | (byte18 >>> 5);
      final int byte19 = blocks[blocksOffset++] & 0xFF;
      final int byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte18 & 31) << 16) | (byte19 << 8) | byte20;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 43;
      values[valuesOffset++] = (block0 >>> 22) & 2097151L;
      values[valuesOffset++] = (block0 >>> 1) & 2097151L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 1L) << 20) | (block1 >>> 44);
      values[valuesOffset++] = (block1 >>> 23) & 2097151L;
      values[valuesOffset++] = (block1 >>> 2) & 2097151L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 3L) << 19) | (block2 >>> 45);
      values[valuesOffset++] = (block2 >>> 24) & 2097151L;
      values[valuesOffset++] = (block2 >>> 3) & 2097151L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 7L) << 18) | (block3 >>> 46);
      values[valuesOffset++] = (block3 >>> 25) & 2097151L;
      values[valuesOffset++] = (block3 >>> 4) & 2097151L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 17) | (block4 >>> 47);
      values[valuesOffset++] = (block4 >>> 26) & 2097151L;
      values[valuesOffset++] = (block4 >>> 5) & 2097151L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 31L) << 16) | (block5 >>> 48);
      values[valuesOffset++] = (block5 >>> 27) & 2097151L;
      values[valuesOffset++] = (block5 >>> 6) & 2097151L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 63L) << 15) | (block6 >>> 49);
      values[valuesOffset++] = (block6 >>> 28) & 2097151L;
      values[valuesOffset++] = (block6 >>> 7) & 2097151L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 127L) << 14) | (block7 >>> 50);
      values[valuesOffset++] = (block7 >>> 29) & 2097151L;
      values[valuesOffset++] = (block7 >>> 8) & 2097151L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 255L) << 13) | (block8 >>> 51);
      values[valuesOffset++] = (block8 >>> 30) & 2097151L;
      values[valuesOffset++] = (block8 >>> 9) & 2097151L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 511L) << 12) | (block9 >>> 52);
      values[valuesOffset++] = (block9 >>> 31) & 2097151L;
      values[valuesOffset++] = (block9 >>> 10) & 2097151L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 1023L) << 11) | (block10 >>> 53);
      values[valuesOffset++] = (block10 >>> 32) & 2097151L;
      values[valuesOffset++] = (block10 >>> 11) & 2097151L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 2047L) << 10) | (block11 >>> 54);
      values[valuesOffset++] = (block11 >>> 33) & 2097151L;
      values[valuesOffset++] = (block11 >>> 12) & 2097151L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 4095L) << 9) | (block12 >>> 55);
      values[valuesOffset++] = (block12 >>> 34) & 2097151L;
      values[valuesOffset++] = (block12 >>> 13) & 2097151L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 8191L) << 8) | (block13 >>> 56);
      values[valuesOffset++] = (block13 >>> 35) & 2097151L;
      values[valuesOffset++] = (block13 >>> 14) & 2097151L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 16383L) << 7) | (block14 >>> 57);
      values[valuesOffset++] = (block14 >>> 36) & 2097151L;
      values[valuesOffset++] = (block14 >>> 15) & 2097151L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 32767L) << 6) | (block15 >>> 58);
      values[valuesOffset++] = (block15 >>> 37) & 2097151L;
      values[valuesOffset++] = (block15 >>> 16) & 2097151L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 65535L) << 5) | (block16 >>> 59);
      values[valuesOffset++] = (block16 >>> 38) & 2097151L;
      values[valuesOffset++] = (block16 >>> 17) & 2097151L;
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 131071L) << 4) | (block17 >>> 60);
      values[valuesOffset++] = (block17 >>> 39) & 2097151L;
      values[valuesOffset++] = (block17 >>> 18) & 2097151L;
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block17 & 262143L) << 3) | (block18 >>> 61);
      values[valuesOffset++] = (block18 >>> 40) & 2097151L;
      values[valuesOffset++] = (block18 >>> 19) & 2097151L;
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block18 & 524287L) << 2) | (block19 >>> 62);
      values[valuesOffset++] = (block19 >>> 41) & 2097151L;
      values[valuesOffset++] = (block19 >>> 20) & 2097151L;
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block19 & 1048575L) << 1) | (block20 >>> 63);
      values[valuesOffset++] = (block20 >>> 42) & 2097151L;
      values[valuesOffset++] = (block20 >>> 21) & 2097151L;
      values[valuesOffset++] = block20 & 2097151L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 13) | (byte1 << 5) | (byte2 >>> 3);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 7) << 18) | (byte3 << 10) | (byte4 << 2) | (byte5 >>> 6);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 15) | (byte6 << 7) | (byte7 >>> 1);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 1) << 20) | (byte8 << 12) | (byte9 << 4) | (byte10 >>> 4);
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 15) << 17) | (byte11 << 9) | (byte12 << 1) | (byte13 >>> 7);
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 127) << 14) | (byte14 << 6) | (byte15 >>> 2);
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      final long byte17 = blocks[blocksOffset++] & 0xFF;
      final long byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte15 & 3) << 19) | (byte16 << 11) | (byte17 << 3) | (byte18 >>> 5);
      final long byte19 = blocks[blocksOffset++] & 0xFF;
      final long byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte18 & 31) << 16) | (byte19 << 8) | byte20;
    }
  }

}
