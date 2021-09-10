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
import static org.apache.lucene5_shaded.util.packed.PackedInts.checkBlockSize;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.LongsRef;

/**
 * Reader for sequences of longs written with {@link BlockPackedWriter}.
 * @see BlockPackedWriter
 * @lucene.internal
 */
public final class BlockPackedReaderIterator {

  // same as DataInput.readVLong but supports negative values
  static long readVLong(DataInput in) throws IOException {
    byte b = in.readByte();
    if (b >= 0) return b;
    long i = b & 0x7FL;
    b = in.readByte();
    i |= (b & 0x7FL) << 7;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0x7FL) << 14;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0x7FL) << 21;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0x7FL) << 28;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0x7FL) << 35;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0x7FL) << 42;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0x7FL) << 49;
    if (b >= 0) return i;
    b = in.readByte();
    i |= (b & 0xFFL) << 56;
    return i;
  }

  DataInput in;
  final int packedIntsVersion;
  long valueCount;
  final int blockSize;
  final long[] values;
  final LongsRef valuesRef;
  byte[] blocks;
  int off;
  long ord;

  /** Sole constructor.
   * @param blockSize the number of values of a block, must be equal to the
   *                  block size of the {@link BlockPackedWriter} which has
   *                  been used to write the stream
   */
  public BlockPackedReaderIterator(DataInput in, int packedIntsVersion, int blockSize, long valueCount) {
    checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
    this.packedIntsVersion = packedIntsVersion;
    this.blockSize = blockSize;
    this.values = new long[blockSize];
    this.valuesRef = new LongsRef(this.values, 0, 0);
    reset(in, valueCount);
  }

  /** Reset the current reader to wrap a stream of <code>valueCount</code>
   * values contained in <code>in</code>. The block size remains unchanged. */
  public void reset(DataInput in, long valueCount) {
    this.in = in;
    assert valueCount >= 0;
    this.valueCount = valueCount;
    off = blockSize;
    ord = 0;
  }

  /** Skip exactly <code>count</code> values. */
  public void skip(long count) throws IOException {
    assert count >= 0;
    if (ord + count > valueCount || ord + count < 0) {
      throw new EOFException();
    }

    // 1. skip buffered values
    final int skipBuffer = (int) Math.min(count, blockSize - off);
    off += skipBuffer;
    ord += skipBuffer;
    count -= skipBuffer;
    if (count == 0L) {
      return;
    }

    // 2. skip as many blocks as necessary
    assert off == blockSize;
    while (count >= blockSize) {
      final int token = in.readByte() & 0xFF;
      final int bitsPerValue = token >>> BPV_SHIFT;
      if (bitsPerValue > 64) {
        throw new IOException("Corrupted");
      }
      if ((token & MIN_VALUE_EQUALS_0) == 0) {
        readVLong(in);
      }
      final long blockBytes = PackedInts.Format.PACKED.byteCount(packedIntsVersion, blockSize, bitsPerValue);
      skipBytes(blockBytes);
      ord += blockSize;
      count -= blockSize;
    }
    if (count == 0L) {
      return;
    }

    // 3. skip last values
    assert count < blockSize;
    refill();
    ord += count;
    off += count;
  }

  private void skipBytes(long count) throws IOException {
    if (in instanceof IndexInput) {
      final IndexInput iin = (IndexInput) in;
      iin.seek(iin.getFilePointer() + count);
    } else {
      if (blocks == null) {
        blocks = new byte[blockSize];
      }
      long skipped = 0;
      while (skipped < count) {
        final int toSkip = (int) Math.min(blocks.length, count - skipped);
        in.readBytes(blocks, 0, toSkip);
        skipped += toSkip;
      }
    }
  }

  /** Read the next value. */
  public long next() throws IOException {
    if (ord == valueCount) {
      throw new EOFException();
    }
    if (off == blockSize) {
      refill();
    }
    final long value = values[off++];
    ++ord;
    return value;
  }

  /** Read between <tt>1</tt> and <code>count</code> values. */
  public LongsRef next(int count) throws IOException {
    assert count > 0;
    if (ord == valueCount) {
      throw new EOFException();
    }
    if (off == blockSize) {
      refill();
    }

    count = Math.min(count, blockSize - off);
    count = (int) Math.min(count, valueCount - ord);

    valuesRef.offset = off;
    valuesRef.length = count;
    off += count;
    ord += count;
    return valuesRef;
  }

  private void refill() throws IOException {
    final int token = in.readByte() & 0xFF;
    final boolean minEquals0 = (token & MIN_VALUE_EQUALS_0) != 0;
    final int bitsPerValue = token >>> BPV_SHIFT;
    if (bitsPerValue > 64) {
      throw new IOException("Corrupted");
    }
    final long minValue = minEquals0 ? 0L : zigZagDecode(1L + readVLong(in));
    assert minEquals0 || minValue != 0;

    if (bitsPerValue == 0) {
      Arrays.fill(values, minValue);
    } else {
      final PackedInts.Decoder decoder = PackedInts.getDecoder(PackedInts.Format.PACKED, packedIntsVersion, bitsPerValue);
      final int iterations = blockSize / decoder.byteValueCount();
      final int blocksSize = iterations * decoder.byteBlockCount();
      if (blocks == null || blocks.length < blocksSize) {
        blocks = new byte[blocksSize];
      }

      final int valueCount = (int) Math.min(this.valueCount - ord, blockSize);
      final int blocksCount = (int) PackedInts.Format.PACKED.byteCount(packedIntsVersion, valueCount, bitsPerValue);
      in.readBytes(blocks, 0, blocksCount);

      decoder.decode(blocks, 0, values, 0, iterations);

      if (minValue != 0) {
        for (int i = 0; i < valueCount; ++i) {
          values[i] += minValue;
        }
      }
    }
    off = 0;
  }

  /** Return the offset of the next value to read. */
  public long ord() {
    return ord;
  }

}
