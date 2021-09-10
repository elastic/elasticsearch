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
final class BulkOperationPacked5 extends BulkOperationPacked {

  public BulkOperationPacked5() {
    super(5);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 59);
      values[valuesOffset++] = (int) ((block0 >>> 54) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 49) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 44) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 39) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 34) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 29) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 24) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 19) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 14) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 9) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 4) & 31L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 15L) << 1) | (block1 >>> 63));
      values[valuesOffset++] = (int) ((block1 >>> 58) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 53) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 48) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 43) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 38) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 33) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 28) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 23) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 18) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 13) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 8) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 3) & 31L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 7L) << 2) | (block2 >>> 62));
      values[valuesOffset++] = (int) ((block2 >>> 57) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 52) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 47) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 42) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 37) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 32) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 27) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 22) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 17) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 7) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 2) & 31L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 3L) << 3) | (block3 >>> 61));
      values[valuesOffset++] = (int) ((block3 >>> 56) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 51) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 46) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 41) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 36) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 31) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 26) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 21) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 16) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 11) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 6) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 1) & 31L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 1L) << 4) | (block4 >>> 60));
      values[valuesOffset++] = (int) ((block4 >>> 55) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 50) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 45) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 40) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 35) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 30) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 25) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 20) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 15) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 10) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 5) & 31L);
      values[valuesOffset++] = (int) (block4 & 31L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 >>> 3;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte0 & 7) << 2) | (byte1 >>> 6);
      values[valuesOffset++] = (byte1 >>> 1) & 31;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 1) << 4) | (byte2 >>> 4);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 1) | (byte3 >>> 7);
      values[valuesOffset++] = (byte3 >>> 2) & 31;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 3) << 3) | (byte4 >>> 5);
      values[valuesOffset++] = byte4 & 31;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 59;
      values[valuesOffset++] = (block0 >>> 54) & 31L;
      values[valuesOffset++] = (block0 >>> 49) & 31L;
      values[valuesOffset++] = (block0 >>> 44) & 31L;
      values[valuesOffset++] = (block0 >>> 39) & 31L;
      values[valuesOffset++] = (block0 >>> 34) & 31L;
      values[valuesOffset++] = (block0 >>> 29) & 31L;
      values[valuesOffset++] = (block0 >>> 24) & 31L;
      values[valuesOffset++] = (block0 >>> 19) & 31L;
      values[valuesOffset++] = (block0 >>> 14) & 31L;
      values[valuesOffset++] = (block0 >>> 9) & 31L;
      values[valuesOffset++] = (block0 >>> 4) & 31L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 15L) << 1) | (block1 >>> 63);
      values[valuesOffset++] = (block1 >>> 58) & 31L;
      values[valuesOffset++] = (block1 >>> 53) & 31L;
      values[valuesOffset++] = (block1 >>> 48) & 31L;
      values[valuesOffset++] = (block1 >>> 43) & 31L;
      values[valuesOffset++] = (block1 >>> 38) & 31L;
      values[valuesOffset++] = (block1 >>> 33) & 31L;
      values[valuesOffset++] = (block1 >>> 28) & 31L;
      values[valuesOffset++] = (block1 >>> 23) & 31L;
      values[valuesOffset++] = (block1 >>> 18) & 31L;
      values[valuesOffset++] = (block1 >>> 13) & 31L;
      values[valuesOffset++] = (block1 >>> 8) & 31L;
      values[valuesOffset++] = (block1 >>> 3) & 31L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 7L) << 2) | (block2 >>> 62);
      values[valuesOffset++] = (block2 >>> 57) & 31L;
      values[valuesOffset++] = (block2 >>> 52) & 31L;
      values[valuesOffset++] = (block2 >>> 47) & 31L;
      values[valuesOffset++] = (block2 >>> 42) & 31L;
      values[valuesOffset++] = (block2 >>> 37) & 31L;
      values[valuesOffset++] = (block2 >>> 32) & 31L;
      values[valuesOffset++] = (block2 >>> 27) & 31L;
      values[valuesOffset++] = (block2 >>> 22) & 31L;
      values[valuesOffset++] = (block2 >>> 17) & 31L;
      values[valuesOffset++] = (block2 >>> 12) & 31L;
      values[valuesOffset++] = (block2 >>> 7) & 31L;
      values[valuesOffset++] = (block2 >>> 2) & 31L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 3L) << 3) | (block3 >>> 61);
      values[valuesOffset++] = (block3 >>> 56) & 31L;
      values[valuesOffset++] = (block3 >>> 51) & 31L;
      values[valuesOffset++] = (block3 >>> 46) & 31L;
      values[valuesOffset++] = (block3 >>> 41) & 31L;
      values[valuesOffset++] = (block3 >>> 36) & 31L;
      values[valuesOffset++] = (block3 >>> 31) & 31L;
      values[valuesOffset++] = (block3 >>> 26) & 31L;
      values[valuesOffset++] = (block3 >>> 21) & 31L;
      values[valuesOffset++] = (block3 >>> 16) & 31L;
      values[valuesOffset++] = (block3 >>> 11) & 31L;
      values[valuesOffset++] = (block3 >>> 6) & 31L;
      values[valuesOffset++] = (block3 >>> 1) & 31L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 1L) << 4) | (block4 >>> 60);
      values[valuesOffset++] = (block4 >>> 55) & 31L;
      values[valuesOffset++] = (block4 >>> 50) & 31L;
      values[valuesOffset++] = (block4 >>> 45) & 31L;
      values[valuesOffset++] = (block4 >>> 40) & 31L;
      values[valuesOffset++] = (block4 >>> 35) & 31L;
      values[valuesOffset++] = (block4 >>> 30) & 31L;
      values[valuesOffset++] = (block4 >>> 25) & 31L;
      values[valuesOffset++] = (block4 >>> 20) & 31L;
      values[valuesOffset++] = (block4 >>> 15) & 31L;
      values[valuesOffset++] = (block4 >>> 10) & 31L;
      values[valuesOffset++] = (block4 >>> 5) & 31L;
      values[valuesOffset++] = block4 & 31L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 >>> 3;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte0 & 7) << 2) | (byte1 >>> 6);
      values[valuesOffset++] = (byte1 >>> 1) & 31;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 1) << 4) | (byte2 >>> 4);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 1) | (byte3 >>> 7);
      values[valuesOffset++] = (byte3 >>> 2) & 31;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 3) << 3) | (byte4 >>> 5);
      values[valuesOffset++] = byte4 & 31;
    }
  }

}
