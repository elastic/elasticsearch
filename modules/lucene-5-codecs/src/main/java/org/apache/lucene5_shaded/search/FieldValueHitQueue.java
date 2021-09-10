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
package org.apache.lucene5_shaded.search;


import java.io.IOException;

import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.PriorityQueue;

/**
 * Expert: A hit queue for sorting by hits by terms in more than one field.
 * 
 * @lucene.experimental
 * @since 2.9
 * @see IndexSearcher#search(Query,int,Sort)
 */
public abstract class FieldValueHitQueue<T extends FieldValueHitQueue.Entry> extends PriorityQueue<T> {

  /**
   * Extension of ScoreDoc to also store the 
   * {@link FieldComparator} slot.
   */
  public static class Entry extends ScoreDoc {
    public int slot;

    public Entry(int slot, int doc, float score) {
      super(doc, score);
      this.slot = slot;
    }
    
    @Override
    public String toString() {
      return "slot:" + slot + " " + super.toString();
    }
  }

  /**
   * An implementation of {@link FieldValueHitQueue} which is optimized in case
   * there is just one comparator.
   */
  private static final class OneComparatorFieldValueHitQueue<T extends Entry> extends FieldValueHitQueue<T> {
    
    private final int oneReverseMul;
    private final FieldComparator<?> oneComparator;
    
    public OneComparatorFieldValueHitQueue(SortField[] fields, int size)
        throws IOException {
      super(fields, size);

      assert fields.length == 1;
      oneComparator = comparators[0];
      oneReverseMul = reverseMul[0];
    }

    /**
     * Returns whether <code>hitA</code> is less relevant than <code>hitB</code>.
     * @param hitA Entry
     * @param hitB Entry
     * @return <code>true</code> if document <code>hitA</code> should be sorted after document <code>hitB</code>.
     */
    @Override
    protected boolean lessThan(final Entry hitA, final Entry hitB) {

      assert hitA != hitB;
      assert hitA.slot != hitB.slot;

      final int c = oneReverseMul * oneComparator.compare(hitA.slot, hitB.slot);
      if (c != 0) {
        return c > 0;
      }

      // avoid random sort order that could lead to duplicates (bug #31241):
      return hitA.doc > hitB.doc;
    }

  }
  
  /**
   * An implementation of {@link FieldValueHitQueue} which is optimized in case
   * there is more than one comparator.
   */
  private static final class MultiComparatorsFieldValueHitQueue<T extends Entry> extends FieldValueHitQueue<T> {

    public MultiComparatorsFieldValueHitQueue(SortField[] fields, int size)
        throws IOException {
      super(fields, size);
    }
  
    @Override
    protected boolean lessThan(final Entry hitA, final Entry hitB) {

      assert hitA != hitB;
      assert hitA.slot != hitB.slot;

      int numComparators = comparators.length;
      for (int i = 0; i < numComparators; ++i) {
        final int c = reverseMul[i] * comparators[i].compare(hitA.slot, hitB.slot);
        if (c != 0) {
          // Short circuit
          return c > 0;
        }
      }

      // avoid random sort order that could lead to duplicates (bug #31241):
      return hitA.doc > hitB.doc;
    }
    
  }
  
  // prevent instantiation and extension.
  private FieldValueHitQueue(SortField[] fields, int size) throws IOException {
    super(size);
    // When we get here, fields.length is guaranteed to be > 0, therefore no
    // need to check it again.
    
    // All these are required by this class's API - need to return arrays.
    // Therefore even in the case of a single comparator, create an array
    // anyway.
    this.fields = fields;
    int numComparators = fields.length;
    comparators = new FieldComparator<?>[numComparators];
    reverseMul = new int[numComparators];
    for (int i = 0; i < numComparators; ++i) {
      SortField field = fields[i];

      reverseMul[i] = field.reverse ? -1 : 1;
      comparators[i] = field.getComparator(size, i);
    }
  }

  /**
   * Creates a hit queue sorted by the given list of fields.
   * 
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length <code>numHits</code>.
   * 
   * @param fields
   *          SortField array we are sorting by in priority order (highest
   *          priority first); cannot be <code>null</code> or empty
   * @param size
   *          The number of hits to retain. Must be greater than zero.
   * @throws IOException if there is a low-level IO error
   */
  public static <T extends Entry> FieldValueHitQueue<T> create(SortField[] fields, int size) throws IOException {

    if (fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (fields.length == 1) {
      return new OneComparatorFieldValueHitQueue<>(fields, size);
    } else {
      return new MultiComparatorsFieldValueHitQueue<>(fields, size);
    }
  }
  
  public FieldComparator<?>[] getComparators() {
    return comparators;
  }

  public int[] getReverseMul() {
    return reverseMul;
  }

  public LeafFieldComparator[] getComparators(LeafReaderContext context) throws IOException {
    LeafFieldComparator[] comparators = new LeafFieldComparator[this.comparators.length];
    for (int i = 0; i < comparators.length; ++i) {
      comparators[i] = this.comparators[i].getLeafComparator(context);
    }
    return comparators;
  }

  /** Stores the sort criteria being used. */
  protected final SortField[] fields;
  protected final FieldComparator<?>[] comparators;
  protected final int[] reverseMul;

  @Override
  protected abstract boolean lessThan (final Entry a, final Entry b);

  /**
   * Given a queue Entry, creates a corresponding FieldDoc
   * that contains the values used to sort the given document.
   * These values are not the raw values out of the index, but the internal
   * representation of them. This is so the given search hit can be collated by
   * a MultiSearcher with other search hits.
   * 
   * @param entry The Entry used to create a FieldDoc
   * @return The newly created FieldDoc
   * @see IndexSearcher#search(Query,int,Sort)
   */
  FieldDoc fillFields(final Entry entry) {
    final int n = comparators.length;
    final Object[] fields = new Object[n];
    for (int i = 0; i < n; ++i) {
      fields[i] = comparators[i].value(entry.slot);
    }
    //if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
    return new FieldDoc(entry.doc, entry.score, fields);
  }

  /** Returns the SortFields being used by this hit queue. */
  SortField[] getFields() {
    return fields;
  }
}
