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
final class BulkOperationPacked18 extends BulkOperationPacked {

  public BulkOperationPacked18() {
    super(18);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 46);
      values[valuesOffset++] = (int) ((block0 >>> 28) & 262143L);
      values[valuesOffset++] = (int) ((block0 >>> 10) & 262143L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 1023L) << 8) | (block1 >>> 56));
      values[valuesOffset++] = (int) ((block1 >>> 38) & 262143L);
      values[valuesOffset++] = (int) ((block1 >>> 20) & 262143L);
      values[valuesOffset++] = (int) ((block1 >>> 2) & 262143L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 3L) << 16) | (block2 >>> 48));
      values[valuesOffset++] = (int) ((block2 >>> 30) & 262143L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 262143L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 4095L) << 6) | (block3 >>> 58));
      values[valuesOffset++] = (int) ((block3 >>> 40) & 262143L);
      values[valuesOffset++] = (int) ((block3 >>> 22) & 262143L);
      values[valuesOffset++] = (int) ((block3 >>> 4) & 262143L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 14) | (block4 >>> 50));
      values[valuesOffset++] = (int) ((block4 >>> 32) & 262143L);
      values[valuesOffset++] = (int) ((block4 >>> 14) & 262143L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 16383L) << 4) | (block5 >>> 60));
      values[valuesOffset++] = (int) ((block5 >>> 42) & 262143L);
      values[valuesOffset++] = (int) ((block5 >>> 24) & 262143L);
      values[valuesOffset++] = (int) ((block5 >>> 6) & 262143L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 63L) << 12) | (block6 >>> 52));
      values[valuesOffset++] = (int) ((block6 >>> 34) & 262143L);
      values[valuesOffset++] = (int) ((block6 >>> 16) & 262143L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 65535L) << 2) | (block7 >>> 62));
      values[valuesOffset++] = (int) ((block7 >>> 44) & 262143L);
      values[valuesOffset++] = (int) ((block7 >>> 26) & 262143L);
      values[valuesOffset++] = (int) ((block7 >>> 8) & 262143L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 255L) << 10) | (block8 >>> 54));
      values[valuesOffset++] = (int) ((block8 >>> 36) & 262143L);
      values[valuesOffset++] = (int) ((block8 >>> 18) & 262143L);
      values[valuesOffset++] = (int) (block8 & 262143L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 10) | (byte1 << 2) | (byte2 >>> 6);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 63) << 12) | (byte3 << 4) | (byte4 >>> 4);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 15) << 14) | (byte5 << 6) | (byte6 >>> 2);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 3) << 16) | (byte7 << 8) | byte8;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 46;
      values[valuesOffset++] = (block0 >>> 28) & 262143L;
      values[valuesOffset++] = (block0 >>> 10) & 262143L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 1023L) << 8) | (block1 >>> 56);
      values[valuesOffset++] = (block1 >>> 38) & 262143L;
      values[valuesOffset++] = (block1 >>> 20) & 262143L;
      values[valuesOffset++] = (block1 >>> 2) & 262143L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 3L) << 16) | (block2 >>> 48);
      values[valuesOffset++] = (block2 >>> 30) & 262143L;
      values[valuesOffset++] = (block2 >>> 12) & 262143L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 4095L) << 6) | (block3 >>> 58);
      values[valuesOffset++] = (block3 >>> 40) & 262143L;
      values[valuesOffset++] = (block3 >>> 22) & 262143L;
      values[valuesOffset++] = (block3 >>> 4) & 262143L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 14) | (block4 >>> 50);
      values[valuesOffset++] = (block4 >>> 32) & 262143L;
      values[valuesOffset++] = (block4 >>> 14) & 262143L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 16383L) << 4) | (block5 >>> 60);
      values[valuesOffset++] = (block5 >>> 42) & 262143L;
      values[valuesOffset++] = (block5 >>> 24) & 262143L;
      values[valuesOffset++] = (block5 >>> 6) & 262143L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 63L) << 12) | (block6 >>> 52);
      values[valuesOffset++] = (block6 >>> 34) & 262143L;
      values[valuesOffset++] = (block6 >>> 16) & 262143L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 65535L) << 2) | (block7 >>> 62);
      values[valuesOffset++] = (block7 >>> 44) & 262143L;
      values[valuesOffset++] = (block7 >>> 26) & 262143L;
      values[valuesOffset++] = (block7 >>> 8) & 262143L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 255L) << 10) | (block8 >>> 54);
      values[valuesOffset++] = (block8 >>> 36) & 262143L;
      values[valuesOffset++] = (block8 >>> 18) & 262143L;
      values[valuesOffset++] = block8 & 262143L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 10) | (byte1 << 2) | (byte2 >>> 6);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 63) << 12) | (byte3 << 4) | (byte4 >>> 4);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 15) << 14) | (byte5 << 6) | (byte6 >>> 2);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 3) << 16) | (byte7 << 8) | byte8;
    }
  }

}
