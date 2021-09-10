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


import java.util.Arrays;

import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.PackedInts.Reader;

class DeltaPackedLongValues extends PackedLongValues {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DeltaPackedLongValues.class);

  final long[] mins;

  DeltaPackedLongValues(int pageShift, int pageMask, Reader[] values, long[] mins, long size, long ramBytesUsed) {
    super(pageShift, pageMask, values, size, ramBytesUsed);
    assert values.length == mins.length;
    this.mins = mins;
  }

  @Override
  long get(int block, int element) {
    return mins[block] + values[block].get(element);
  }

  @Override
  int decodeBlock(int block, long[] dest) {
    final int count = super.decodeBlock(block, dest);
    final long min = mins[block];
    for (int i = 0; i < count; ++i) {
      dest[i] += min;
    }
    return count;
  }

  static class Builder extends PackedLongValues.Builder {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);

    long[] mins;

    Builder(int pageSize, float acceptableOverheadRatio) {
      super(pageSize, acceptableOverheadRatio);
      mins = new long[values.length];
      ramBytesUsed += RamUsageEstimator.sizeOf(mins);
    }

    @Override
    long baseRamBytesUsed() {
      return BASE_RAM_BYTES_USED;
    }

    @Override
    public DeltaPackedLongValues build() {
      finish();
      pending = null;
      final Reader[] values = Arrays.copyOf(this.values, valuesOff);
      final long[] mins = Arrays.copyOf(this.mins, valuesOff);
      final long ramBytesUsed = DeltaPackedLongValues.BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(values) + RamUsageEstimator.sizeOf(mins);
      return new DeltaPackedLongValues(pageShift, pageMask, values, mins, size, ramBytesUsed);
    }

    @Override
    void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
      long min = values[0];
      for (int i = 1; i < numValues; ++i) {
        min = Math.min(min, values[i]);
      }
      for (int i = 0; i < numValues; ++i) {
        values[i] -= min;
      }
      super.pack(values, numValues, block, acceptableOverheadRatio);
      mins[block] = min;
    }

    @Override
    void grow(int newBlockCount) {
      super.grow(newBlockCount);
      ramBytesUsed -= RamUsageEstimator.sizeOf(mins);
      mins = Arrays.copyOf(mins, newBlockCount);
      ramBytesUsed += RamUsageEstimator.sizeOf(mins);
    }

  }

}
