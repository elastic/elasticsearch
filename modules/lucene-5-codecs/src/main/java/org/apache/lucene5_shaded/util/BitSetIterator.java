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


import org.apache.lucene5_shaded.search.DocIdSetIterator;

/**
 * A {@link DocIdSetIterator} which iterates over set bits in a
 * bit set.
 * @lucene.internal
 */
public class BitSetIterator extends DocIdSetIterator {

  private static <T extends BitSet> T getBitSet(DocIdSetIterator iterator, Class<? extends T> clazz) {
    if (iterator instanceof BitSetIterator) {
      BitSet bits = ((BitSetIterator) iterator).bits;
      assert bits != null;
      if (clazz.isInstance(bits)) {
        return clazz.cast(bits);
      }
    }
    return null;
  }

  /** If the provided iterator wraps a {@link FixedBitSet}, returns it, otherwise returns null. */
  public static FixedBitSet getFixedBitSetOrNull(DocIdSetIterator iterator) {
    return getBitSet(iterator, FixedBitSet.class);
  }

  /** If the provided iterator wraps a {@link SparseFixedBitSet}, returns it, otherwise returns null. */
  public static SparseFixedBitSet getSparseFixedBitSetOrNull(DocIdSetIterator iterator) {
    return getBitSet(iterator, SparseFixedBitSet.class);
  }

  private final BitSet bits;
  private final int length;
  private final long cost;
  private int doc = -1;

  /** Sole constructor. */
  public BitSetIterator(BitSet bits, long cost) {
    this.bits = bits;
    this.length = bits.length();
    this.cost = cost;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) {
    if (target >= length) {
      return doc = NO_MORE_DOCS;
    }
    return doc = bits.nextSetBit(target);
  }

  @Override
  public long cost() {
    return cost;
  }

}
