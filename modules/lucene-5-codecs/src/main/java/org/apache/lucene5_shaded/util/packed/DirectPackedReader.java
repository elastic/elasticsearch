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


import org.apache.lucene5_shaded.store.IndexInput;

import java.io.IOException;

/* Reads directly from disk on each get */
// just for back compat, use DirectReader/DirectWriter for more efficient impl
class DirectPackedReader extends PackedInts.ReaderImpl {
  final IndexInput in;
  final int bitsPerValue;
  final long startPointer;
  final long valueMask;

  DirectPackedReader(int bitsPerValue, int valueCount, IndexInput in) {
    super(valueCount);
    this.in = in;
    this.bitsPerValue = bitsPerValue;

    startPointer = in.getFilePointer();
    if (bitsPerValue == 64) {
      valueMask = -1L;
    } else {
      valueMask = (1L << bitsPerValue) - 1;
    }
  }

  @Override
  public long get(int index) {
    final long majorBitPos = (long)index * bitsPerValue;
    final long elementPos = majorBitPos >>> 3;
    try {
      in.seek(startPointer + elementPos);

      final int bitPos = (int) (majorBitPos & 7);
      // round up bits to a multiple of 8 to find total bytes needed to read
      final int roundedBits = ((bitPos + bitsPerValue + 7) & ~7);
      // the number of extra bits read at the end to shift out
      int shiftRightBits = roundedBits - bitPos - bitsPerValue;

      long rawValue;
      switch (roundedBits >>> 3) {
        case 1:
          rawValue = in.readByte();
          break;
        case 2:
          rawValue = in.readShort();
          break;
        case 3:
          rawValue = ((long)in.readShort() << 8) | (in.readByte() & 0xFFL);
          break;
        case 4:
          rawValue = in.readInt();
          break;
        case 5:
          rawValue = ((long)in.readInt() << 8) | (in.readByte() & 0xFFL);
          break;
        case 6:
          rawValue = ((long)in.readInt() << 16) | (in.readShort() & 0xFFFFL);
          break;
        case 7:
          rawValue = ((long)in.readInt() << 24) | ((in.readShort() & 0xFFFFL) << 8) | (in.readByte() & 0xFFL);
          break;
        case 8:
          rawValue = in.readLong();
          break;
        case 9:
          // We must be very careful not to shift out relevant bits. So we account for right shift
          // we would normally do on return here, and reset it.
          rawValue = (in.readLong() << (8 - shiftRightBits)) | ((in.readByte() & 0xFFL) >>> shiftRightBits);
          shiftRightBits = 0;
          break;
        default:
          throw new AssertionError("bitsPerValue too large: " + bitsPerValue);
      }
      return (rawValue >>> shiftRightBits) & valueMask;

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
