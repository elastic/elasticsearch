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


import static org.apache.lucene5_shaded.util.BitUtil.zigZagEncode;
import static org.apache.lucene5_shaded.util.packed.MonotonicBlockPackedReader.expected;

import java.io.IOException;

import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.util.BitUtil;

/**
 * A writer for large monotonically increasing sequences of positive longs.
 * <p>
 * The sequence is divided into fixed-size blocks and for each block, values
 * are modeled after a linear function f: x &rarr; A &times; x + B. The block
 * encodes deltas from the expected values computed from this function using as
 * few bits as possible.
 * <p>
 * Format:
 * <ul>
 * <li>&lt;BLock&gt;<sup>BlockCount</sup>
 * <li>BlockCount: &lceil; ValueCount / BlockSize &rceil;
 * <li>Block: &lt;Header, (Ints)&gt;
 * <li>Header: &lt;B, A, BitsPerValue&gt;
 * <li>B: the B from f: x &rarr; A &times; x + B using a
 *     {@link BitUtil#zigZagEncode(long) zig-zag encoded}
 *     {@link DataOutput#writeVLong(long) vLong}
 * <li>A: the A from f: x &rarr; A &times; x + B encoded using
 *     {@link Float#floatToIntBits(float)} on
 *     {@link DataOutput#writeInt(int) 4 bytes}
 * <li>BitsPerValue: a {@link DataOutput#writeVInt(int) variable-length int}
 * <li>Ints: if BitsPerValue is <tt>0</tt>, then there is nothing to read and
 *     all values perfectly match the result of the function. Otherwise, these
 *     are the {@link PackedInts packed} deltas from the expected value
 *     (computed from the function) using exaclty BitsPerValue bits per value.
 * </ul>
 * @see MonotonicBlockPackedReader
 * @lucene.internal
 */
public final class MonotonicBlockPackedWriter extends AbstractBlockPackedWriter {

  /**
   * Sole constructor.
   * @param blockSize the number of values of a single block, must be a power of 2
   */
  public MonotonicBlockPackedWriter(DataOutput out, int blockSize) {
    super(out, blockSize);
  }

  @Override
  public void add(long l) throws IOException {
    assert l >= 0;
    super.add(l);
  }

  protected void flush() throws IOException {
    assert off > 0;

    final float avg = off == 1 ? 0f : (float) (values[off - 1] - values[0]) / (off - 1);
    long min = values[0];
    // adjust min so that all deltas will be positive
    for (int i = 1; i < off; ++i) {
      final long actual = values[i];
      final long expected = expected(min, avg, i);
      if (expected > actual) {
        min -= (expected - actual);
      }
    }

    long maxDelta = 0;
    for (int i = 0; i < off; ++i) {
      values[i] = values[i] - expected(min, avg, i);
      maxDelta = Math.max(maxDelta, values[i]);
    }

    out.writeZLong(min);
    out.writeInt(Float.floatToIntBits(avg));
    if (maxDelta == 0) {
      out.writeVInt(0);
    } else {
      final int bitsRequired = PackedInts.bitsRequired(maxDelta);
      out.writeVInt(bitsRequired);
      writeValues(bitsRequired);
    }

    off = 0;
  }

}
