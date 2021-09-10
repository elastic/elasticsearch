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
 * Implementation of the {@link DocIdSet} interface on top of a {@link BitSet}.
 * @lucene.internal
 */
public class BitDocIdSet extends DocIdSet {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BitDocIdSet.class);

  private final BitSet set;
  private final long cost;

  /**
   * Wrap the given {@link BitSet} as a {@link DocIdSet}. The provided
   * {@link BitSet} must not be modified afterwards.
   */
  public BitDocIdSet(BitSet set, long cost) {
    this.set = set;
    this.cost = cost;
  }

  /**
   * Same as {@link #BitDocIdSet(BitSet, long)} but uses the set's
   * {@link BitSet#approximateCardinality() approximate cardinality} as a cost.
   */
  public BitDocIdSet(BitSet set) {
    this(set, set.approximateCardinality());
  }

  @Override
  public DocIdSetIterator iterator() {
    return new BitSetIterator(set, cost);
  }

  @Override
  public BitSet bits() {
    return set;
  }

  /** This DocIdSet implementation is cacheable. */
  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + set.ramBytesUsed();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(set=" + set + ",cost=" + cost + ")";
  }

  /**
   * A builder of {@link DocIdSet}s that supports random access. If you don't
   * need random access, you should rather use {@link DocIdSetBuilder}.
   * @lucene.internal
   */
  public static final class Builder {

    private final int maxDoc;
    private final int threshold;
    private SparseFixedBitSet sparseSet;
    private FixedBitSet denseSet;

    // we cache an upper bound of the cost of this builder so that we don't have
    // to re-compute approximateCardinality on the sparse set every time 
    private long costUpperBound;

    /** Create a new instance that can hold <code>maxDoc</code> documents and is optionally <code>full</code>. */
    public Builder(int maxDoc, boolean full) {
      this.maxDoc = maxDoc;
      threshold = maxDoc >>> 10;
      if (full) {
        denseSet = new FixedBitSet(maxDoc);
        denseSet.set(0, maxDoc);
      }
    }

    /** Create a new empty instance. */
    public Builder(int maxDoc) {
      this(maxDoc, false);
    }

    // pkg-private for testing
    boolean dense() {
      return denseSet != null;
    }

    /**
     * Is this builder definitely empty?  If so, {@link #build()} will return null.  This is usually the same as
     * simply being empty but if this builder was constructed with the {@code full} option or if an iterator was passed
     * that iterated over no documents, then we're not sure.
     */
    public boolean isDefinitelyEmpty() {
      return sparseSet == null && denseSet == null;
    }

    /**
     * Add the content of the provided {@link DocIdSetIterator} to this builder.
     */
    public void or(DocIdSetIterator it) throws IOException {
      if (denseSet != null) {
        // already upgraded
        denseSet.or(it);
        return;
      }

      final long itCost = it.cost();
      costUpperBound += itCost;
      if (costUpperBound >= threshold) {
        costUpperBound = (sparseSet == null ? 0 : sparseSet.approximateCardinality()) + itCost;

        if (costUpperBound >= threshold) {
          // upgrade
          denseSet = new FixedBitSet(maxDoc);
          denseSet.or(it);
          if (sparseSet != null) {
            denseSet.or(new BitSetIterator(sparseSet, 0L));
          }
          return;
        }
      }

      // we are still sparse
      if (sparseSet == null) {
        sparseSet = new SparseFixedBitSet(maxDoc);
      }
      sparseSet.or(it);
    }

    /**
     * Removes from this builder documents that are not contained in <code>it</code>.
     */
    @Deprecated
    public void and(DocIdSetIterator it) throws IOException {
      if (denseSet != null) {
        denseSet.and(it);
      } else if (sparseSet != null) {
        sparseSet.and(it);
      }
    }

    /**
     * Removes from this builder documents that are contained in <code>it</code>.
     */
    @Deprecated
    public void andNot(DocIdSetIterator it) throws IOException {
      if (denseSet != null) {
        denseSet.andNot(it);
      } else if (sparseSet != null) {
        sparseSet.andNot(it);
      }
    }

    /**
     * Build a {@link DocIdSet} that contains all doc ids that have been added.
     * This method may return <tt>null</tt> if no documents were addded to this
     * builder.
     * NOTE: this is a destructive operation, the builder should not be used
     * anymore after this method has been called.
     */
    public BitDocIdSet build() {
      final BitDocIdSet result;
      if (denseSet != null) {
        result = new BitDocIdSet(denseSet);
      } else if (sparseSet != null) {
        result = new BitDocIdSet(sparseSet);
      } else {
        result = null;
      }
      denseSet = null;
      sparseSet = null;
      costUpperBound = 0;
      return result;
    }

  }

}
