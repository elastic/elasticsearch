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

import java.io.IOException;

import org.apache.lucene5_shaded.store.DataOutput;

/**
 * A writer for large sequences of longs.
 * <p>
 * The sequence is divided into fixed-size blocks and for each block, the
 * difference between each value and the minimum value of the block is encoded
 * using as few bits as possible. Memory usage of this class is proportional to
 * the block size. Each block has an overhead between 1 and 10 bytes to store
 * the minimum value and the number of bits per value of the block.
 * <p>
 * Format:
 * <ul>
 * <li>&lt;BLock&gt;<sup>BlockCount</sup>
 * <li>BlockCount: &lceil; ValueCount / BlockSize &rceil;
 * <li>Block: &lt;Header, (Ints)&gt;
 * <li>Header: &lt;Token, (MinValue)&gt;
 * <li>Token: a {@link DataOutput#writeByte(byte) byte}, first 7 bits are the
 *     number of bits per value (<tt>bitsPerValue</tt>). If the 8th bit is 1,
 *     then MinValue (see next) is <tt>0</tt>, otherwise MinValue and needs to
 *     be decoded
 * <li>MinValue: a
 *     <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">zigzag-encoded</a>
 *     {@link DataOutput#writeVLong(long) variable-length long} whose value
 *     should be added to every int from the block to restore the original
 *     values
 * <li>Ints: If the number of bits per value is <tt>0</tt>, then there is
 *     nothing to decode and all ints are equal to MinValue. Otherwise: BlockSize
 *     {@link PackedInts packed ints} encoded on exactly <tt>bitsPerValue</tt>
 *     bits per value. They are the subtraction of the original values and
 *     MinValue
 * </ul>
 * @see BlockPackedReaderIterator
 * @see BlockPackedReader
 * @lucene.internal
 */
public final class BlockPackedWriter extends AbstractBlockPackedWriter {
  
  /**
   * Sole constructor.
   * @param blockSize the number of values of a single block, must be a power of 2
   */
  public BlockPackedWriter(DataOutput out, int blockSize) {
    super(out, blockSize);
  }

  protected void flush() throws IOException {
    assert off > 0;
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    for (int i = 0; i < off; ++i) {
      min = Math.min(values[i], min);
      max = Math.max(values[i], max);
    }

    final long delta = max - min;
    int bitsRequired = delta == 0 ? 0 : PackedInts.unsignedBitsRequired(delta);
    if (bitsRequired == 64) {
      // no need to delta-encode
      min = 0L;
    } else if (min > 0L) {
      // make min as small as possible so that writeVLong requires fewer bytes
      min = Math.max(0L, max - PackedInts.maxValue(bitsRequired));
    }

    final int token = (bitsRequired << BPV_SHIFT) | (min == 0 ? MIN_VALUE_EQUALS_0 : 0);
    out.writeByte((byte) token);

    if (min != 0) {
      writeVLong(out, zigZagEncode(min) - 1);
    }

    if (bitsRequired > 0) {
      if (min != 0) {
        for (int i = 0; i < off; ++i) {
          values[i] -= min;
        }
      }
      writeValues(bitsRequired);
    }

    off = 0;
  }

}
