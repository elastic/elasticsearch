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
import static org.apache.lucene5_shaded.util.packed.AbstractBlockPackedWriter.BPV_SHIFT;
import static org.apache.lucene5_shaded.util.packed.AbstractBlockPackedWriter.MAX_BLOCK_SIZE;
import static org.apache.lucene5_shaded.util.packed.AbstractBlockPackedWriter.MIN_BLOCK_SIZE;
import static org.apache.lucene5_shaded.util.packed.AbstractBlockPackedWriter.MIN_VALUE_EQUALS_0;
import static org.apache.lucene5_shaded.util.packed.BlockPackedReaderIterator.readVLong;
import static org.apache.lucene5_shaded.util.packed.PackedInts.checkBlockSize;
import static org.apache.lucene5_shaded.util.packed.PackedInts.numBlocks;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.LongValues;

/**
 * Provides random access to a stream written with {@link BlockPackedWriter}.
 * @lucene.internal
 */
public final class BlockPackedReader extends LongValues implements Accountable {

  private final int blockShift, blockMask;
  private final long valueCount;
  private final long[] minValues;
  private final PackedInts.Reader[] subReaders;
  private final long sumBPV;

  /** Sole constructor. */
  public BlockPackedReader(IndexInput in, int packedIntsVersion, int blockSize, long valueCount, boolean direct) throws IOException {
    this.valueCount = valueCount;
    blockShift = checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
    blockMask = blockSize - 1;
    final int numBlocks = numBlocks(valueCount, blockSize);
    long[] minValues = null;
    subReaders = new PackedInts.Reader[numBlocks];
    long sumBPV = 0;
    for (int i = 0; i < numBlocks; ++i) {
      final int token = in.readByte() & 0xFF;
      final int bitsPerValue = token >>> BPV_SHIFT;
      sumBPV += bitsPerValue;
      if (bitsPerValue > 64) {
        throw new IOException("Corrupted");
      }
      if ((token & MIN_VALUE_EQUALS_0) == 0) {
        if (minValues == null) {
          minValues = new long[numBlocks];
        }
        minValues[i] = zigZagDecode(1L + readVLong(in));
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
    this.minValues = minValues;
    this.sumBPV = sumBPV;
  }

  @Override
  public long get(long index) {
    assert index >= 0 && index < valueCount;
    final int block = (int) (index >>> blockShift);
    final int idx = (int) (index & blockMask);
    return (minValues == null ? 0 : minValues[block]) + subReaders[block].get(idx);
  }

  @Override
  public long ramBytesUsed() {
    long size = 0;
    for (PackedInts.Reader reader : subReaders) {
      size += reader.ramBytesUsed();
    }
    return size;
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
