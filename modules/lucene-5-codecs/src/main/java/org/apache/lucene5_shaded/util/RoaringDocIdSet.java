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
import java.util.Arrays;

import org.apache.lucene5_shaded.search.DocIdSet;
import org.apache.lucene5_shaded.search.DocIdSetIterator;

/**
 * {@link DocIdSet} implementation inspired from http://roaringbitmap.org/
 *
 * The space is divided into blocks of 2^16 bits and each block is encoded
 * independently. In each block, if less than 2^12 bits are set, then
 * documents are simply stored in a short[]. If more than 2^16-2^12 bits are
 * set, then the inverse of the set is encoded in a simple short[]. Otherwise
 * a {@link FixedBitSet} is used.
 *
 * @lucene.internal
 */
public class RoaringDocIdSet extends DocIdSet {

  // Number of documents in a block
  private static final int BLOCK_SIZE = 1 << 16;
  // The maximum length for an array, beyond that point we switch to a bitset
  private static final int MAX_ARRAY_LENGTH = 1 << 12;
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoaringDocIdSet.class);

  /** A builder of {@link RoaringDocIdSet}s. */
  public static class Builder {

    private final int maxDoc;
    private final DocIdSet[] sets;

    private int cardinality;
    private int lastDocId;
    private int currentBlock;
    private int currentBlockCardinality;

    // We start by filling the buffer and when it's full we copy the content of
    // the buffer to the FixedBitSet and put further documents in that bitset
    private final short[] buffer;
    private FixedBitSet denseBuffer;

    /** Sole constructor. */
    public Builder(int maxDoc) {
      this.maxDoc = maxDoc;
      sets = new DocIdSet[(maxDoc + (1 << 16) - 1) >>> 16];
      lastDocId = -1;
      currentBlock = -1;
      buffer = new short[MAX_ARRAY_LENGTH];
    }

    private void flush() {
      assert currentBlockCardinality <= BLOCK_SIZE;
      if (currentBlockCardinality <= MAX_ARRAY_LENGTH) {
        // Use sparse encoding
        assert denseBuffer == null;
        if (currentBlockCardinality > 0) {
          sets[currentBlock] = new ShortArrayDocIdSet(Arrays.copyOf(buffer, currentBlockCardinality));
        }
      } else {
        assert denseBuffer != null;
        assert denseBuffer.cardinality() == currentBlockCardinality;
        if (denseBuffer.length() == BLOCK_SIZE && BLOCK_SIZE - currentBlockCardinality < MAX_ARRAY_LENGTH) {
          // Doc ids are very dense, inverse the encoding
          final short[] excludedDocs = new short[BLOCK_SIZE - currentBlockCardinality];
          denseBuffer.flip(0, denseBuffer.length());
          int excludedDoc = -1;
          for (int i = 0; i < excludedDocs.length; ++i) {
            excludedDoc = denseBuffer.nextSetBit(excludedDoc + 1);
            assert excludedDoc != DocIdSetIterator.NO_MORE_DOCS;
            excludedDocs[i] = (short) excludedDoc;
          }
          assert excludedDoc + 1 == denseBuffer.length() || denseBuffer.nextSetBit(excludedDoc + 1) == DocIdSetIterator.NO_MORE_DOCS;
          sets[currentBlock] = new NotDocIdSet(BLOCK_SIZE, new ShortArrayDocIdSet(excludedDocs));
        } else {
          // Neither sparse nor super dense, use a fixed bit set
          sets[currentBlock] = new BitDocIdSet(denseBuffer, currentBlockCardinality);
        }
        denseBuffer = null;
      }

      cardinality += currentBlockCardinality;
      denseBuffer = null;
      currentBlockCardinality = 0;
    }

    /**
     * Add a new doc-id to this builder.
     * NOTE: doc ids must be added in order.
     */
    public Builder add(int docId) {
      if (docId <= lastDocId) {
        throw new IllegalArgumentException("Doc ids must be added in-order, got " + docId + " which is <= lastDocID=" + lastDocId);
      }
      final int block = docId >>> 16;
      if (block != currentBlock) {
        // we went to a different block, let's flush what we buffered and start from fresh
        flush();
        currentBlock = block;
      }

      if (currentBlockCardinality < MAX_ARRAY_LENGTH) {
        buffer[currentBlockCardinality] = (short) docId;
      } else {
        if (denseBuffer == null) {
          // the buffer is full, let's move to a fixed bit set
          final int numBits = Math.min(1 << 16, maxDoc - (block << 16));
          denseBuffer = new FixedBitSet(numBits);
          for (short doc : buffer) {
            denseBuffer.set(doc & 0xFFFF);
          }
        }
        denseBuffer.set(docId & 0xFFFF);
      }

      lastDocId = docId;
      currentBlockCardinality += 1;
      return this;
    }

    /** Add the content of the provided {@link DocIdSetIterator}. */
    public Builder add(DocIdSetIterator disi) throws IOException {
      for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
        add(doc);
      }
      return this;
    }

    /** Build an instance. */
    public RoaringDocIdSet build() {
      flush();
      return new RoaringDocIdSet(sets, cardinality);
    }

  }

  /**
   * {@link DocIdSet} implementation that can store documents up to 2^16-1 in a short[].
   */
  private static class ShortArrayDocIdSet extends DocIdSet {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ShortArrayDocIdSet.class);

    private final short[] docIDs;

    private ShortArrayDocIdSet(short[] docIDs) {
      this.docIDs = docIDs;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(docIDs);
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new DocIdSetIterator() {

        int i = -1; // this is the index of the current document in the array
        int doc = -1;

        private int docId(int i) {
          return docIDs[i] & 0xFFFF;
        }

        @Override
        public int nextDoc() throws IOException {
          if (++i >= docIDs.length) {
            return doc = NO_MORE_DOCS;
          }
          return doc = docId(i);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return docIDs.length;
        }

        @Override
        public int advance(int target) throws IOException {
          // binary search
          int lo = i + 1;
          int hi = docIDs.length - 1;
          while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int midDoc = docId(mid);
            if (midDoc < target) {
              lo = mid + 1;
            } else {
              hi = mid - 1;
            }
          }
          if (lo == docIDs.length) {
            i = docIDs.length;
            return doc = NO_MORE_DOCS;
          } else {
            i = lo;
            return doc = docId(i);
          }
        }
      };
    }

  }

  private final DocIdSet[] docIdSets;
  private final int cardinality;
  private final long ramBytesUsed;

  private RoaringDocIdSet(DocIdSet[] docIdSets, int cardinality) {
    this.docIdSets = docIdSets;
    long ramBytesUsed = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(docIdSets);
    for (DocIdSet set : this.docIdSets) {
      if (set != null) {
        ramBytesUsed += set.ramBytesUsed();
      }
    }
    this.ramBytesUsed = ramBytesUsed;
    this.cardinality = cardinality;
  }

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    if (cardinality == 0) {
      return null;
    }
    return new Iterator();
  }

  private class Iterator extends DocIdSetIterator {

    int block;
    DocIdSetIterator sub = null;
    int doc;

    Iterator() throws IOException {
      doc = -1;
      block = -1;
      sub = DocIdSetIterator.empty();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      final int subNext = sub.nextDoc();
      if (subNext == NO_MORE_DOCS) {
        return firstDocFromNextBlock();
      }
      return doc = (block << 16) | subNext;
    }

    @Override
    public int advance(int target) throws IOException {
      final int targetBlock = target >>> 16;
      if (targetBlock != block) {
        block = targetBlock;
        if (block >= docIdSets.length) {
          sub = null;
          return doc = NO_MORE_DOCS;
        }
        if (docIdSets[block] == null) {
          return firstDocFromNextBlock();
        }
        sub = docIdSets[block].iterator();
      }
      final int subNext = sub.advance(target & 0xFFFF);
      if (subNext == NO_MORE_DOCS) {
        return firstDocFromNextBlock();
      }
      return doc = (block << 16) | subNext;
    }

    private int firstDocFromNextBlock() throws IOException {
      while (true) {
        block += 1;
        if (block >= docIdSets.length) {
          sub = null;
          return doc = NO_MORE_DOCS;
        } else if (docIdSets[block] != null) {
          sub = docIdSets[block].iterator();
          final int subNext = sub.nextDoc();
          assert subNext != NO_MORE_DOCS;
          return doc = (block << 16) | subNext;
        }
      }
    }

    @Override
    public long cost() {
      return cardinality;
    }

  }

  /** Return the exact number of documents that are contained in this set. */
  public int cardinality() {
    return cardinality;
  }

  @Override
  public String toString() {
    return "RoaringDocIdSet(cardinality=" + cardinality + ")";
  }
}
