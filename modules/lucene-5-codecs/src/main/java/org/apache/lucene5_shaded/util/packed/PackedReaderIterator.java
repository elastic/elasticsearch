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


import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.LongsRef;

final class PackedReaderIterator extends PackedInts.ReaderIteratorImpl {

  final int packedIntsVersion;
  final PackedInts.Format format;
  final BulkOperation bulkOperation;
  final byte[] nextBlocks;
  final LongsRef nextValues;
  final int iterations;
  int position;

  PackedReaderIterator(PackedInts.Format format, int packedIntsVersion, int valueCount, int bitsPerValue, DataInput in, int mem) {
    super(valueCount, bitsPerValue, in);
    this.format = format;
    this.packedIntsVersion = packedIntsVersion;
    bulkOperation = BulkOperation.of(format, bitsPerValue);
    iterations = iterations(mem);
    assert valueCount == 0 || iterations > 0;
    nextBlocks = new byte[iterations * bulkOperation.byteBlockCount()];
    nextValues = new LongsRef(new long[iterations * bulkOperation.byteValueCount()], 0, 0);
    nextValues.offset = nextValues.longs.length;
    position = -1;
  }

  private int iterations(int mem) {
    int iterations = bulkOperation.computeIterations(valueCount, mem);
    if (packedIntsVersion < PackedInts.VERSION_BYTE_ALIGNED) {
      // make sure iterations is a multiple of 8
      iterations = (iterations + 7) & 0xFFFFFFF8;
    }
    return iterations;
  }

  @Override
  public LongsRef next(int count) throws IOException {
    assert nextValues.length >= 0;
    assert count > 0;
    assert nextValues.offset + nextValues.length <= nextValues.longs.length;
    
    nextValues.offset += nextValues.length;

    final int remaining = valueCount - position - 1;
    if (remaining <= 0) {
      throw new EOFException();
    }
    count = Math.min(remaining, count);

    if (nextValues.offset == nextValues.longs.length) {
      final long remainingBlocks = format.byteCount(packedIntsVersion, remaining, bitsPerValue);
      final int blocksToRead = (int) Math.min(remainingBlocks, nextBlocks.length);
      in.readBytes(nextBlocks, 0, blocksToRead);
      if (blocksToRead < nextBlocks.length) {
        Arrays.fill(nextBlocks, blocksToRead, nextBlocks.length, (byte) 0);
      }

      bulkOperation.decode(nextBlocks, 0, nextValues.longs, 0, iterations);
      nextValues.offset = 0;
    }

    nextValues.length = Math.min(nextValues.longs.length - nextValues.offset, count);
    position += nextValues.length;
    return nextValues;
  }

  @Override
  public int ord() {
    return position;
  }

}
