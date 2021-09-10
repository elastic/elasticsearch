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
package org.apache.lucene5_shaded.util;


import java.io.IOException;

import org.apache.lucene5_shaded.search.DocIdSet;
import org.apache.lucene5_shaded.search.DocIdSetIterator;

/**
 * A builder of {@link DocIdSet}s.  At first it uses a sparse structure to gather
 * documents, and then upgrades to a non-sparse bit set once enough hits match.
 *
 * @lucene.internal
 */
public final class DocIdSetBuilder {

  private final int maxDoc;
  private final int threshold;

  private int[] buffer;
  private int bufferSize;

  private BitSet bitSet;

  /**
   * Create a builder that can contain doc IDs between {@code 0} and {@code maxDoc}.
   */
  public DocIdSetBuilder(int maxDoc) {
    this.maxDoc = maxDoc;
    // For ridiculously small sets, we'll just use a sorted int[]
    // maxDoc >>> 7 is a good value if you want to save memory, lower values
    // such as maxDoc >>> 11 should provide faster building but at the expense
    // of using a full bitset even for quite sparse data
    this.threshold = maxDoc >>> 7;

    this.buffer = new int[0];
    this.bufferSize = 0;
    this.bitSet = null;
  }

  private void upgradeToBitSet() {
    assert bitSet == null;
    bitSet = new FixedBitSet(maxDoc);
    for (int i = 0; i < bufferSize; ++i) {
      bitSet.set(buffer[i]);
    }
    this.buffer = null;
    this.bufferSize = 0;
  }

  /** Grows the buffer to at least minSize, but never larger than threshold. */
  private void growBuffer(int minSize) {
    assert minSize < threshold;
    if (buffer.length < minSize) {
      int nextSize = Math.min(threshold, ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_INT));
      int[] newBuffer = new int[nextSize];
      System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
      buffer = newBuffer;
    }
  }

  /**
   * Add the content of the provided {@link DocIdSetIterator} to this builder.
   * NOTE: if you need to build a {@link DocIdSet} out of a single
   * {@link DocIdSetIterator}, you should rather use {@link RoaringDocIdSet.Builder}.
   */
  public void add(DocIdSetIterator iter) throws IOException {
    grow((int) Math.min(Integer.MAX_VALUE, iter.cost()));

    if (bitSet != null) {
      bitSet.or(iter);
    } else {
      while (true) {  
        assert buffer.length <= threshold;
        final int end = buffer.length;
        for (int i = bufferSize; i < end; ++i) {
          final int doc = iter.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            bufferSize = i;
            return;
          }
          buffer[bufferSize++] = doc;
        }
        bufferSize = end;

        if (bufferSize + 1 >= threshold) {
          break;
        }

        growBuffer(bufferSize+1);
      }

      upgradeToBitSet();
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        bitSet.set(doc);
      }
    }
  }

  /**
   * Reserve space so that this builder can hold {@code numDocs} MORE documents.
   */
  public void grow(int numDocs) {
    if (bitSet == null) {
      final long newLength = bufferSize + numDocs;
      if (newLength < threshold) {
        growBuffer((int) newLength);
      } else {
        upgradeToBitSet();
      }
    }
  }

  /**
   * Add a document to this builder.
   * NOTE: doc IDs do not need to be provided in order.
   * NOTE: if you plan on adding several docs at once, look into using
   * {@link #grow(int)} to reserve space.
   */
  public void add(int doc) {
    if (bitSet != null) {
      bitSet.set(doc);
    } else {
      if (bufferSize + 1 > buffer.length) {
        if (bufferSize + 1 >= threshold) {
          upgradeToBitSet();
          bitSet.set(doc);
          return;
        }
        growBuffer(bufferSize+1);
      }
      buffer[bufferSize++] = doc;
    }
  }

  private static int dedup(int[] arr, int length) {
    if (length == 0) {
      return 0;
    }
    int l = 1;
    int previous = arr[0];
    for (int i = 1; i < length; ++i) {
      final int value = arr[i];
      assert value >= previous;
      if (value != previous) {
        arr[l++] = value;
        previous = value;
      }
    }
    return l;
  }

  /**
   * Build a {@link DocIdSet} from the accumulated doc IDs.
   */
  public DocIdSet build() {
    return build(-1);
  }

  /**
   * Expert: build a {@link DocIdSet} with a hint on the cost that the resulting
   * {@link DocIdSet} would have.
   */
  public DocIdSet build(long costHint) {
    try {
      if (bitSet != null) {
        if (costHint == -1) {
          return new BitDocIdSet(bitSet);
        } else {
          return new BitDocIdSet(bitSet, costHint);
        }
      } else {
        LSBRadixSorter sorter = new LSBRadixSorter();
        sorter.sort(buffer, 0, bufferSize);
        final int l = dedup(buffer, bufferSize);
        assert l <= bufferSize;
        buffer = ArrayUtil.grow(buffer, l + 1);
        buffer[l] = DocIdSetIterator.NO_MORE_DOCS;
        return new IntArrayDocIdSet(buffer, l);
      }
    } finally {
      this.buffer = null;
      this.bufferSize = 0;
      this.bitSet = null;
    }
  }

}
