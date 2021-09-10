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


import static org.apache.lucene5_shaded.util.BitUtil.zigZagDecode;
import static org.apache.lucene5_shaded.util.packed.AbstractBlockPackedWriter.MAX_BLOCK_SIZE;
import static org.apache.lucene5_shaded.util.packed.AbstractBlockPackedWriter.MIN_BLOCK_SIZE;
import static org.apache.lucene5_shaded.util.packed.PackedInts.checkBlockSize;
import static org.apache.lucene5_shaded.util.packed.PackedInts.numBlocks;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.LongValues;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Provides random access to a stream written with
 * {@link MonotonicBlockPackedWriter}.
 * @lucene.internal
 */
public class MonotonicBlockPackedReader extends LongValues implements Accountable {

  static long expected(long origin, float average, int index) {
    return origin + (long) (average * (long) index);
  }

  final int blockShift, blockMask;
  final long valueCount;
  final long[] minValues;
  final float[] averages;
  final PackedInts.Reader[] subReaders;
  final long sumBPV;

  /** Sole constructor. */
  public static MonotonicBlockPackedReader of(IndexInput in, int packedIntsVersion, int blockSize, long valueCount, boolean direct) throws IOException {
    if (packedIntsVersion < PackedInts.VERSION_MONOTONIC_WITHOUT_ZIGZAG) {
      return new MonotonicBlockPackedReader(in, packedIntsVersion, blockSize, valueCount, direct) {
        @Override
        protected long decodeDelta(long delta) {
          return zigZagDecode(delta);
        }
      };
    }
    return new MonotonicBlockPackedReader(in, packedIntsVersion, blockSize, valueCount, direct);
  }

  private MonotonicBlockPackedReader(IndexInput in, int packedIntsVersion, int blockSize, long valueCount, boolean direct) throws IOException {
    this.valueCount = valueCount;
    blockShift = checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
    blockMask = blockSize - 1;
    final int numBlocks = numBlocks(valueCount, blockSize);
    minValues = new long[numBlocks];
    averages = new float[numBlocks];
    subReaders = new PackedInts.Reader[numBlocks];
    long sumBPV = 0;
    for (int i = 0; i < numBlocks; ++i) {
      if (packedIntsVersion < PackedInts.VERSION_MONOTONIC_WITHOUT_ZIGZAG) {
        minValues[i] = in.readVLong();
      } else {
        minValues[i] = in.readZLong();
      }
      averages[i] = Float.intBitsToFloat(in.readInt());
      final int bitsPerValue = in.readVInt();
      sumBPV += bitsPerValue;
      if (bitsPerValue > 64) {
        throw new IOException("Corrupted");
      }
      if (bitsPerValue == 0) {
        subReaders[i] = new PackedInts.NullReader(blockSize);
      } else {
        final int size = (int) Math.min(blockSize, valueCount - (long) i * blockSize);
        if (direct) {
          final long pointer = in.getFilePointer();
          subReaders[i] = PackedInts.getDirectReaderNoHeader(in, PackedInts.Format.PACKED, packedIntsVersion, size, bitsPerValue);
          in.seek(pointer + PackedInts.Format.PACKED.byteCount(packedIntsVersion, size, bitsPerValue));
        } else {
          subReaders[i] = PackedInts.getReaderNoHeader(in, PackedInts.Format.PACKED, packedIntsVersion, size, bitsPerValue);
        }
      }
    }
    this.sumBPV = sumBPV;
  }

  @Override
  public long get(long index) {
    assert index >= 0 && index < valueCount;
    final int block = (int) (index >>> blockShift);
    final int idx = (int) (index & blockMask);
    return expected(minValues[block], averages[block], idx) + decodeDelta(subReaders[block].get(idx));
  }

  protected long decodeDelta(long delta) {
    return delta;
  }

  /** Returns the number of values */
  public long size() {
    return valueCount;
  }
  
  @Override
  public long ramBytesUsed() {
    long sizeInBytes = 0;
    sizeInBytes += RamUsageEstimator.sizeOf(minValues);
    sizeInBytes += RamUsageEstimator.sizeOf(averages);
    for(PackedInts.Reader reader: subReaders) {
      sizeInBytes += reader.ramBytesUsed();
    }
    return sizeInBytes;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
  
  @Override
  public String toString() {
    long avgBPV = subReaders.length == 0 ? 0 : sumBPV / subReaders.length;
    return getClass().getSimpleName() + "(blocksize=" + (1<<blockShift) + ",size=" + valueCount + ",avgBPV=" + avgBPV + ")";
  }
}
