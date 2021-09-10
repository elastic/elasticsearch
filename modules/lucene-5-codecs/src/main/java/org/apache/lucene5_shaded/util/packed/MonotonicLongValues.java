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

import static org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedReader.expected;

import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.PackedInts.Reader;

class MonotonicLongValues extends DeltaPackedLongValues {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MonotonicLongValues.class);

  final float[] averages;

  MonotonicLongValues(int pageShift, int pageMask, Reader[] values, long[] mins, float[] averages, long size, long ramBytesUsed) {
    super(pageShift, pageMask, values, mins, size, ramBytesUsed);
    assert values.length == averages.length;
    this.averages = averages;
  }

  @Override
  long get(int block, int element) {
    return expected(mins[block], averages[block], element) + values[block].get(element);
  }

  @Override
  int decodeBlock(int block, long[] dest) {
    final int count = super.decodeBlock(block, dest);
    final float average = averages[block];
    for (int i = 0; i < count; ++i) {
      dest[i] += expected(0, average, i);
    }
    return count;
  }

  static class Builder extends DeltaPackedLongValues.Builder {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);

    float[] averages;

    Builder(int pageSize, float acceptableOverheadRatio) {
      super(pageSize, acceptableOverheadRatio);
      averages = new float[values.length];
      ramBytesUsed += RamUsageEstimator.sizeOf(averages);
    }

    @Override
    long baseRamBytesUsed() {
      return BASE_RAM_BYTES_USED;
    }

    @Override
    public MonotonicLongValues build() {
      finish();
      pending = null;
      final Reader[] values = Arrays.copyOf(this.values, valuesOff);
      final long[] mins = Arrays.copyOf(this.mins, valuesOff);
      final float[] averages = Arrays.copyOf(this.averages, valuesOff);
      final long ramBytesUsed = MonotonicLongValues.BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(values) + RamUsageEstimator.sizeOf(mins)
          + RamUsageEstimator.sizeOf(averages);
      return new MonotonicLongValues(pageShift, pageMask, values, mins, averages, size, ramBytesUsed);
    }

    @Override
    void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
      final float average = numValues == 1 ? 0 : (float) (values[numValues - 1] - values[0]) / (numValues - 1);
      for (int i = 0; i < numValues; ++i) {
        values[i] -= expected(0, average, i);
      }
      super.pack(values, numValues, block, acceptableOverheadRatio);
      averages[block] = average;
    }

    @Override
    void grow(int newBlockCount) {
      super.grow(newBlockCount);
      ramBytesUsed -= RamUsageEstimator.sizeOf(averages);
      averages = Arrays.copyOf(averages, newBlockCount);
      ramBytesUsed += RamUsageEstimator.sizeOf(averages);
    }

  }

}
