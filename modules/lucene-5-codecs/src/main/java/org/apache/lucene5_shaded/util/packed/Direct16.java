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
 * Direct wrapping of 16-bits values to a backing array.
 * @lucene.internal
 */
final class Direct16 extends PackedInts.MutableImpl {
  final short[] values;

  Direct16(int valueCount) {
    super(valueCount, 16);
    values = new short[valueCount];
  }

  Direct16(int packedIntsVersion, DataInput in, int valueCount) throws IOException {
    this(valueCount);
    for (int i = 0; i < valueCount; ++i) {
      values[i] = in.readShort();
    }
    // because packed ints have not always been byte-aligned
    final int remaining = (int) (PackedInts.Format.PACKED.byteCount(packedIntsVersion, valueCount, 16) - 2L * valueCount);
    for (int i = 0; i < remaining; ++i) {
      in.readByte();
    }
  }

  @Override
  public long get(final int index) {
    return values[index] & 0xFFFFL;
  }

  @Override
  public void set(final int index, final long value) {
    values[index] = (short) (value);
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
    Arrays.fill(values, (short) 0L);
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
      arr[o] = values[i] & 0xFFFFL;
    }
    return gets;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + sets; i < end; ++i, ++o) {
      values[i] = (short) arr[o];
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert val == (val & 0xFFFFL);
    Arrays.fill(values, fromIndex, toIndex, (short) val);
  }
}
