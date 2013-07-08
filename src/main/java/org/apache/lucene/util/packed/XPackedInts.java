package org.apache.lucene.util.packed;

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

import org.apache.lucene.util.Version;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.elasticsearch.common.lucene.Lucene;

/**
 * Simplistic compression for array of unsigned long values.
 * Each value is >= 0 and <= a specified maximum value.  The
 * values are stored as packed ints, with each value
 * consuming a fixed number of bits.
 *
 * @lucene.internal
 */
public class XPackedInts {

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1492640.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

  /** Same as {@link #copy(Reader, int, Mutable, int, int, int)} but using a pre-allocated buffer. */
  static void copy(Reader src, int srcPos, Mutable dest, int destPos, int len, long[] buf) {
    assert buf.length > 0;
    int remaining = 0;
    while (len > 0) {
      final int read = src.get(srcPos, buf, remaining, Math.min(len, buf.length - remaining));
      assert read > 0;
      srcPos += read;
      len -= read;
      remaining += read;
      final int written = dest.set(destPos, buf, 0, remaining);
      assert written > 0;
      destPos += written;
      if (written < remaining) {
        System.arraycopy(buf, written, buf, 0, remaining - written);
      }
      remaining -= written;
    }
    while (remaining > 0) {
      final int written = dest.set(destPos, buf, 0, remaining);
      destPos += written;
      remaining -= written;
      System.arraycopy(buf, written, buf, 0, remaining);
    }
  }

  /** Check that the block size is a power of 2, in the right bounds, and return
   *  its log in base 2. */
  static int checkBlockSize(int blockSize, int minBlockSize, int maxBlockSize) {
    if (blockSize < minBlockSize || blockSize > maxBlockSize) {
      throw new IllegalArgumentException("blockSize must be >= " + minBlockSize + " and <= " + maxBlockSize + ", got " + blockSize);
    }
    if ((blockSize & (blockSize - 1)) != 0) {
      throw new IllegalArgumentException("blockSize must be a power of two, got " + blockSize);
    }
    return Integer.numberOfTrailingZeros(blockSize);
  }

  /** Return the number of blocks required to store <code>size</code> values on
   *  <code>blockSize</code>. */
  static int numBlocks(long size, int blockSize) {
    final int numBlocks = (int) (size / blockSize) + (size % blockSize == 0 ? 0 : 1);
    if ((long) numBlocks * blockSize < size) {
      throw new IllegalArgumentException("size is too large for this block size");
    }
    return numBlocks;
  }

}
