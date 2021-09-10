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
final class BulkOperationPacked23 extends BulkOperationPacked {

  public BulkOperationPacked23() {
    super(23);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 41);
      values[valuesOffset++] = (int) ((block0 >>> 18) & 8388607L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 262143L) << 5) | (block1 >>> 59));
      values[valuesOffset++] = (int) ((block1 >>> 36) & 8388607L);
      values[valuesOffset++] = (int) ((block1 >>> 13) & 8388607L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 8191L) << 10) | (block2 >>> 54));
      values[valuesOffset++] = (int) ((block2 >>> 31) & 8388607L);
      values[valuesOffset++] = (int) ((block2 >>> 8) & 8388607L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 255L) << 15) | (block3 >>> 49));
      values[valuesOffset++] = (int) ((block3 >>> 26) & 8388607L);
      values[valuesOffset++] = (int) ((block3 >>> 3) & 8388607L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 7L) << 20) | (block4 >>> 44));
      values[valuesOffset++] = (int) ((block4 >>> 21) & 8388607L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 2097151L) << 2) | (block5 >>> 62));
      values[valuesOffset++] = (int) ((block5 >>> 39) & 8388607L);
      values[valuesOffset++] = (int) ((block5 >>> 16) & 8388607L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 65535L) << 7) | (block6 >>> 57));
      values[valuesOffset++] = (int) ((block6 >>> 34) & 8388607L);
      values[valuesOffset++] = (int) ((block6 >>> 11) & 8388607L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 2047L) << 12) | (block7 >>> 52));
      values[valuesOffset++] = (int) ((block7 >>> 29) & 8388607L);
      values[valuesOffset++] = (int) ((block7 >>> 6) & 8388607L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 63L) << 17) | (block8 >>> 47));
      values[valuesOffset++] = (int) ((block8 >>> 24) & 8388607L);
      values[valuesOffset++] = (int) ((block8 >>> 1) & 8388607L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 1L) << 22) | (block9 >>> 42));
      values[valuesOffset++] = (int) ((block9 >>> 19) & 8388607L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 524287L) << 4) | (block10 >>> 60));
      values[valuesOffset++] = (int) ((block10 >>> 37) & 8388607L);
      values[valuesOffset++] = (int) ((block10 >>> 14) & 8388607L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 16383L) << 9) | (block11 >>> 55));
      values[valuesOffset++] = (int) ((block11 >>> 32) & 8388607L);
      values[valuesOffset++] = (int) ((block11 >>> 9) & 8388607L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 511L) << 14) | (block12 >>> 50));
      values[valuesOffset++] = (int) ((block12 >>> 27) & 8388607L);
      values[valuesOffset++] = (int) ((block12 >>> 4) & 8388607L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 15L) << 19) | (block13 >>> 45));
      values[valuesOffset++] = (int) ((block13 >>> 22) & 8388607L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 4194303L) << 1) | (block14 >>> 63));
      values[valuesOffset++] = (int) ((block14 >>> 40) & 8388607L);
      values[valuesOffset++] = (int) ((block14 >>> 17) & 8388607L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 131071L) << 6) | (block15 >>> 58));
      values[valuesOffset++] = (int) ((block15 >>> 35) & 8388607L);
      values[valuesOffset++] = (int) ((block15 >>> 12) & 8388607L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 4095L) << 11) | (block16 >>> 53));
      values[valuesOffset++] = (int) ((block16 >>> 30) & 8388607L);
      values[valuesOffset++] = (int) ((block16 >>> 7) & 8388607L);
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 127L) << 16) | (block17 >>> 48));
      values[valuesOffset++] = (int) ((block17 >>> 25) & 8388607L);
      values[valuesOffset++] = (int) ((block17 >>> 2) & 8388607L);
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block17 & 3L) << 21) | (block18 >>> 43));
      values[valuesOffset++] = (int) ((block18 >>> 20) & 8388607L);
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block18 & 1048575L) << 3) | (block19 >>> 61));
      values[valuesOffset++] = (int) ((block19 >>> 38) & 8388607L);
      values[valuesOffset++] = (int) ((block19 >>> 15) & 8388607L);
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block19 & 32767L) << 8) | (block20 >>> 56));
      values[valuesOffset++] = (int) ((block20 >>> 33) & 8388607L);
      values[valuesOffset++] = (int) ((block20 >>> 10) & 8388607L);
      final long block21 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block20 & 1023L) << 13) | (block21 >>> 51));
      values[valuesOffset++] = (int) ((block21 >>> 28) & 8388607L);
      values[valuesOffset++] = (int) ((block21 >>> 5) & 8388607L);
      final long block22 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block21 & 31L) << 18) | (block22 >>> 46));
      values[valuesOffset++] = (int) ((block22 >>> 23) & 8388607L);
      values[valuesOffset++] = (int) (block22 & 8388607L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 15) | (byte1 << 7) | (byte2 >>> 1);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 1) << 22) | (byte3 << 14) | (byte4 << 6) | (byte5 >>> 2);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 21) | (byte6 << 13) | (byte7 << 5) | (byte8 >>> 3);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 7) << 20) | (byte9 << 12) | (byte10 << 4) | (byte11 >>> 4);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 15) << 19) | (byte12 << 11) | (byte13 << 3) | (byte14 >>> 5);
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      final int byte17 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 31) << 18) | (byte15 << 10) | (byte16 << 2) | (byte17 >>> 6);
      final int byte18 = blocks[blocksOffset++] & 0xFF;
      final int byte19 = blocks[blocksOffset++] & 0xFF;
      final int byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte17 & 63) << 17) | (byte18 << 9) | (byte19 << 1) | (byte20 >>> 7);
      final int byte21 = blocks[blocksOffset++] & 0xFF;
      final int byte22 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte20 & 127) << 16) | (byte21 << 8) | byte22;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 41;
      values[valuesOffset++] = (block0 >>> 18) & 8388607L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 262143L) << 5) | (block1 >>> 59);
      values[valuesOffset++] = (block1 >>> 36) & 8388607L;
      values[valuesOffset++] = (block1 >>> 13) & 8388607L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 8191L) << 10) | (block2 >>> 54);
      values[valuesOffset++] = (block2 >>> 31) & 8388607L;
      values[valuesOffset++] = (block2 >>> 8) & 8388607L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 255L) << 15) | (block3 >>> 49);
      values[valuesOffset++] = (block3 >>> 26) & 8388607L;
      values[valuesOffset++] = (block3 >>> 3) & 8388607L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 7L) << 20) | (block4 >>> 44);
      values[valuesOffset++] = (block4 >>> 21) & 8388607L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 2097151L) << 2) | (block5 >>> 62);
      values[valuesOffset++] = (block5 >>> 39) & 8388607L;
      values[valuesOffset++] = (block5 >>> 16) & 8388607L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 65535L) << 7) | (block6 >>> 57);
      values[valuesOffset++] = (block6 >>> 34) & 8388607L;
      values[valuesOffset++] = (block6 >>> 11) & 8388607L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 2047L) << 12) | (block7 >>> 52);
      values[valuesOffset++] = (block7 >>> 29) & 8388607L;
      values[valuesOffset++] = (block7 >>> 6) & 8388607L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 63L) << 17) | (block8 >>> 47);
      values[valuesOffset++] = (block8 >>> 24) & 8388607L;
      values[valuesOffset++] = (block8 >>> 1) & 8388607L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 1L) << 22) | (block9 >>> 42);
      values[valuesOffset++] = (block9 >>> 19) & 8388607L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 524287L) << 4) | (block10 >>> 60);
      values[valuesOffset++] = (block10 >>> 37) & 8388607L;
      values[valuesOffset++] = (block10 >>> 14) & 8388607L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 16383L) << 9) | (block11 >>> 55);
      values[valuesOffset++] = (block11 >>> 32) & 8388607L;
      values[valuesOffset++] = (block11 >>> 9) & 8388607L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 511L) << 14) | (block12 >>> 50);
      values[valuesOffset++] = (block12 >>> 27) & 8388607L;
      values[valuesOffset++] = (block12 >>> 4) & 8388607L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 15L) << 19) | (block13 >>> 45);
      values[valuesOffset++] = (block13 >>> 22) & 8388607L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 4194303L) << 1) | (block14 >>> 63);
      values[valuesOffset++] = (block14 >>> 40) & 8388607L;
      values[valuesOffset++] = (block14 >>> 17) & 8388607L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 131071L) << 6) | (block15 >>> 58);
      values[valuesOffset++] = (block15 >>> 35) & 8388607L;
      values[valuesOffset++] = (block15 >>> 12) & 8388607L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 4095L) << 11) | (block16 >>> 53);
      values[valuesOffset++] = (block16 >>> 30) & 8388607L;
      values[valuesOffset++] = (block16 >>> 7) & 8388607L;
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 127L) << 16) | (block17 >>> 48);
      values[valuesOffset++] = (block17 >>> 25) & 8388607L;
      values[valuesOffset++] = (block17 >>> 2) & 8388607L;
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block17 & 3L) << 21) | (block18 >>> 43);
      values[valuesOffset++] = (block18 >>> 20) & 8388607L;
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block18 & 1048575L) << 3) | (block19 >>> 61);
      values[valuesOffset++] = (block19 >>> 38) & 8388607L;
      values[valuesOffset++] = (block19 >>> 15) & 8388607L;
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block19 & 32767L) << 8) | (block20 >>> 56);
      values[valuesOffset++] = (block20 >>> 33) & 8388607L;
      values[valuesOffset++] = (block20 >>> 10) & 8388607L;
      final long block21 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block20 & 1023L) << 13) | (block21 >>> 51);
      values[valuesOffset++] = (block21 >>> 28) & 8388607L;
      values[valuesOffset++] = (block21 >>> 5) & 8388607L;
      final long block22 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block21 & 31L) << 18) | (block22 >>> 46);
      values[valuesOffset++] = (block22 >>> 23) & 8388607L;
      values[valuesOffset++] = block22 & 8388607L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 15) | (byte1 << 7) | (byte2 >>> 1);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 1) << 22) | (byte3 << 14) | (byte4 << 6) | (byte5 >>> 2);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 21) | (byte6 << 13) | (byte7 << 5) | (byte8 >>> 3);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 7) << 20) | (byte9 << 12) | (byte10 << 4) | (byte11 >>> 4);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 15) << 19) | (byte12 << 11) | (byte13 << 3) | (byte14 >>> 5);
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      final long byte17 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 31) << 18) | (byte15 << 10) | (byte16 << 2) | (byte17 >>> 6);
      final long byte18 = blocks[blocksOffset++] & 0xFF;
      final long byte19 = blocks[blocksOffset++] & 0xFF;
      final long byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte17 & 63) << 17) | (byte18 << 9) | (byte19 << 1) | (byte20 >>> 7);
      final long byte21 = blocks[blocksOffset++] & 0xFF;
      final long byte22 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte20 & 127) << 16) | (byte21 << 8) | byte22;
    }
  }

}
