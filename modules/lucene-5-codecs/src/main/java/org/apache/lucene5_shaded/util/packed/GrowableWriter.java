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


import java.io.IOException;

import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**     
 * Implements {@link PackedInts.Mutable}, but grows the
 * bit count of the underlying packed ints on-demand.
 * <p>Beware that this class will accept to set negative values but in order
 * to do this, it will grow the number of bits per value to 64.
 *
 * <p>@lucene5_shaded.internal</p>
 */
public class GrowableWriter extends PackedInts.Mutable {

  private long currentMask;
  private PackedInts.Mutable current;
  private final float acceptableOverheadRatio;

  /**
   * @param startBitsPerValue       the initial number of bits per value, may grow depending on the data
   * @param valueCount              the number of values
   * @param acceptableOverheadRatio an acceptable overhead ratio
   */
  public GrowableWriter(int startBitsPerValue, int valueCount, float acceptableOverheadRatio) {
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    current = PackedInts.getMutable(valueCount, startBitsPerValue, this.acceptableOverheadRatio);
    currentMask = mask(current.getBitsPerValue());
  }

  private static long mask(int bitsPerValue) {
    return bitsPerValue == 64 ? ~0L : PackedInts.maxValue(bitsPerValue);
  }

  @Override
  public long get(int index) {
    return current.get(index);
  }

  @Override
  public int size() {
    return current.size();
  }

  @Override
  public int getBitsPerValue() {
    return current.getBitsPerValue();
  }

  public PackedInts.Mutable getMutable() {
    return current;
  }

  private void ensureCapacity(long value) {
    if ((value & currentMask) == value) {
      return;
    }
    final int bitsRequired = PackedInts.unsignedBitsRequired(value);
    assert bitsRequired > current.getBitsPerValue();
    final int valueCount = size();
    PackedInts.Mutable next = PackedInts.getMutable(valueCount, bitsRequired, acceptableOverheadRatio);
    PackedInts.copy(current, 0, next, 0, valueCount, PackedInts.DEFAULT_BUFFER_SIZE);
    current = next;
    currentMask = mask(current.getBitsPerValue());
  }

  @Override
  public void set(int index, long value) {
    ensureCapacity(value);
    current.set(index, value);
  }

  @Override
  public void clear() {
    current.clear();
  }

  public GrowableWriter resize(int newSize) {
    GrowableWriter next = new GrowableWriter(getBitsPerValue(), newSize, acceptableOverheadRatio);
    final int limit = Math.min(size(), newSize);
    PackedInts.copy(current, 0, next, 0, limit, PackedInts.DEFAULT_BUFFER_SIZE);
    return next;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    return current.get(index, arr, off, len);
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    long max = 0;
    for (int i = off, end = off + len; i < end; ++i) {
      // bitwise or is nice because either all values are positive and the
      // or-ed result will require as many bits per value as the max of the
      // values, or one of them is negative and the result will be negative,
      // forcing GrowableWriter to use 64 bits per value
      max |= arr[i];
    }
    ensureCapacity(max);
    return current.set(index, arr, off, len);
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    ensureCapacity(val);
    current.fill(fromIndex, toIndex, val);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF
        + RamUsageEstimator.NUM_BYTES_LONG
        + RamUsageEstimator.NUM_BYTES_FLOAT)
        + current.ramBytesUsed();
  }

  @Override
  public void save(DataOutput out) throws IOException {
    current.save(out);
  }

}
