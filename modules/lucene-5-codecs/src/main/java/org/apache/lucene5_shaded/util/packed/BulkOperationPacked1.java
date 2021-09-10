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
final class BulkOperationPacked1 extends BulkOperationPacked {

  public BulkOperationPacked1() {
    super(1);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = blocks[blocksOffset++];
      for (int shift = 63; shift >= 0; shift -= 1) {
        values[valuesOffset++] = (int) ((block >>> shift) & 1);
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int j = 0; j < iterations; ++j) {
      final byte block = blocks[blocksOffset++];
      values[valuesOffset++] = (block >>> 7) & 1;
      values[valuesOffset++] = (block >>> 6) & 1;
      values[valuesOffset++] = (block >>> 5) & 1;
      values[valuesOffset++] = (block >>> 4) & 1;
      values[valuesOffset++] = (block >>> 3) & 1;
      values[valuesOffset++] = (block >>> 2) & 1;
      values[valuesOffset++] = (block >>> 1) & 1;
      values[valuesOffset++] = block & 1;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = blocks[blocksOffset++];
      for (int shift = 63; shift >= 0; shift -= 1) {
        values[valuesOffset++] = (block >>> shift) & 1;
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int j = 0; j < iterations; ++j) {
      final byte block = blocks[blocksOffset++];
      values[valuesOffset++] = (block >>> 7) & 1;
      values[valuesOffset++] = (block >>> 6) & 1;
      values[valuesOffset++] = (block >>> 5) & 1;
      values[valuesOffset++] = (block >>> 4) & 1;
      values[valuesOffset++] = (block >>> 3) & 1;
      values[valuesOffset++] = (block >>> 2) & 1;
      values[valuesOffset++] = (block >>> 1) & 1;
      values[valuesOffset++] = block & 1;
    }
  }

}
