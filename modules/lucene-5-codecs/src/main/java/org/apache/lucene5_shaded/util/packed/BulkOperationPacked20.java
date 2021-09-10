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
final class BulkOperationPacked20 extends BulkOperationPacked {

  public BulkOperationPacked20() {
    super(20);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 >>> 44);
      values[valuesOffset++] = (int) ((block0 >>> 24) & 1048575L);
      values[valuesOffset++] = (int) ((block0 >>> 4) & 1048575L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block0 & 15L) << 16) | (block1 >>> 48));
      values[valuesOffset++] = (int) ((block1 >>> 28) & 1048575L);
      values[valuesOffset++] = (int) ((block1 >>> 8) & 1048575L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 255L) << 12) | (block2 >>> 52));
      values[valuesOffset++] = (int) ((block2 >>> 32) & 1048575L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 1048575L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 4095L) << 8) | (block3 >>> 56));
      values[valuesOffset++] = (int) ((block3 >>> 36) & 1048575L);
      values[valuesOffset++] = (int) ((block3 >>> 16) & 1048575L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 65535L) << 4) | (block4 >>> 60));
      values[valuesOffset++] = (int) ((block4 >>> 40) & 1048575L);
      values[valuesOffset++] = (int) ((block4 >>> 20) & 1048575L);
      values[valuesOffset++] = (int) (block4 & 1048575L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 12) | (byte1 << 4) | (byte2 >>> 4);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 16) | (byte3 << 8) | byte4;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 >>> 44;
      values[valuesOffset++] = (block0 >>> 24) & 1048575L;
      values[valuesOffset++] = (block0 >>> 4) & 1048575L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block0 & 15L) << 16) | (block1 >>> 48);
      values[valuesOffset++] = (block1 >>> 28) & 1048575L;
      values[valuesOffset++] = (block1 >>> 8) & 1048575L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 255L) << 12) | (block2 >>> 52);
      values[valuesOffset++] = (block2 >>> 32) & 1048575L;
      values[valuesOffset++] = (block2 >>> 12) & 1048575L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 4095L) << 8) | (block3 >>> 56);
      values[valuesOffset++] = (block3 >>> 36) & 1048575L;
      values[valuesOffset++] = (block3 >>> 16) & 1048575L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 65535L) << 4) | (block4 >>> 60);
      values[valuesOffset++] = (block4 >>> 40) & 1048575L;
      values[valuesOffset++] = (block4 >>> 20) & 1048575L;
      values[valuesOffset++] = block4 & 1048575L;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 << 12) | (byte1 << 4) | (byte2 >>> 4);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 16) | (byte3 << 8) | byte4;
    }
  }

}
