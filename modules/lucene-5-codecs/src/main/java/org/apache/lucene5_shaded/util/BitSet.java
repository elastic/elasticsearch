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
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene5_shaded.search.DocIdSetIterator;

/**
 * Base implementation for a bit set.
 * @lucene.internal
 */
public abstract class BitSet implements MutableBits, Accountable {

  /** Build a {@link BitSet} from the content of the provided {@link DocIdSetIterator}.
   *  NOTE: this will fully consume the {@link DocIdSetIterator}. */
  public static BitSet of(DocIdSetIterator it, int maxDoc) throws IOException {
    final long cost = it.cost();
    final int threshold = maxDoc >>> 7;
    BitSet set;
    if (cost < threshold) {
      set = new SparseFixedBitSet(maxDoc);
    } else {
      set = new FixedBitSet(maxDoc);
    }
    set.or(it);
    return set;
  }

  /** Set the bit at <code>i</code>. */
  public abstract void set(int i);

  /** Clears a range of bits.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public abstract void clear(int startIndex, int endIndex);

  /**
   * Return the number of bits that are set.
   * NOTE: this method is likely to run in linear time
   */
  public abstract int cardinality();

  /**
   * Return an approximation of the cardinality of this set. Some
   * implementations may trade accuracy for speed if they have the ability to
   * estimate the cardinality of the set without iterating over all the data.
   * The default implementation returns {@link #cardinality()}.
   */
  public int approximateCardinality() {
    return cardinality();
  }

  /** Returns the index of the last set bit before or on the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public abstract int prevSetBit(int index);

  /** Returns the index of the first set bit starting at the index specified.
   *  {@link DocIdSetIterator#NO_MORE_DOCS} is returned if there are no more set bits.
   */
  public abstract int nextSetBit(int index);

  /** Assert that the current doc is -1. */
  protected final void assertUnpositioned(DocIdSetIterator iter) {
    if (iter.docID() != -1) {
      throw new IllegalStateException("This operation only works with an unpositioned iterator, got current position = " + iter.docID());
    }
  }

  /** Does in-place OR of the bits provided by the iterator. The state of the
   *  iterator after this operation terminates is undefined. */
  public void or(DocIdSetIterator iter) throws IOException {
    assertUnpositioned(iter);
    for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
      set(doc);
    }
  }

  private static abstract class LeapFrogCallBack {
    abstract void onMatch(int doc);
    void finish() {}
  }

  /** Performs a leap frog between this and the provided iterator in order to find common documents. */
  private void leapFrog(DocIdSetIterator iter, LeapFrogCallBack callback) throws IOException {
    final int length = length();
    int bitSetDoc = -1;
    int disiDoc = iter.nextDoc();
    while (true) {
      // invariant: bitSetDoc <= disiDoc
      assert bitSetDoc <= disiDoc;
      if (disiDoc >= length) {
        callback.finish();
        return;
      }
      if (bitSetDoc < disiDoc) {
        bitSetDoc = nextSetBit(disiDoc);
      }
      if (bitSetDoc == disiDoc) {
        callback.onMatch(bitSetDoc);
        disiDoc = iter.nextDoc();
      } else {
        disiDoc = iter.advance(bitSetDoc);
      }
    }
  }

  /** Does in-place AND of the bits provided by the iterator. The state of the
   *  iterator after this operation terminates is undefined. */
  @Deprecated
  public void and(DocIdSetIterator iter) throws IOException {
    assertUnpositioned(iter);
    leapFrog(iter, new LeapFrogCallBack() {
      int previous = -1;

      @Override
      public void onMatch(int doc) {
        clear(previous + 1, doc);
        previous = doc;
      }

      @Override
      public void finish() {
        if (previous + 1 < length()) {
          clear(previous + 1, length());
        }
      }

    });
  }

  /** this = this AND NOT other. The state of the iterator after this operation
   *  terminates is undefined. */
  @Deprecated
  public void andNot(DocIdSetIterator iter) throws IOException {
    assertUnpositioned(iter);
    leapFrog(iter, new LeapFrogCallBack() {

      @Override
      public void onMatch(int doc) {
        clear(doc);
      }

    });
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
