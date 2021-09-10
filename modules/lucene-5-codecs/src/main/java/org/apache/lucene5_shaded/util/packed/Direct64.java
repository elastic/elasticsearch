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

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Direct wrapping of 64-bits values to a backing array.
 * @lucene.internal
 */
final class Direct64 extends PackedInts.MutableImpl {
  final long[] values;

  Direct64(int valueCount) {
    super(valueCount, 64);
    values = new long[valueCount];
  }

  Direct64(int packedIntsVersion, DataInput in, int valueCount) throws IOException {
    this(valueCount);
    for (int i = 0; i < valueCount; ++i) {
      values[i] = in.readLong();
    }
  }

  @Override
  public long get(final int index) {
    return values[index];
  }

  @Override
  public void set(final int index, final long value) {
    values[index] = (value);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * RamUsageEstimator.NUM_BYTES_INT     // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // values ref
        + RamUsageEstimator.sizeOf(values);
  }

  @Override
  public void clear() {
    Arrays.fill(values, 0L);
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    System.arraycopy(values, index, arr, off, gets);
    return gets;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    System.arraycopy(arr, off, values, index, sets);
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    Arrays.fill(values, fromIndex, toIndex, val);
  }
}
