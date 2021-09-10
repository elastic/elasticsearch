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
import java.util.Collection;
import java.util.Collections;

/**
 * Expert: Common scoring functionality for different types of queries.
 *
 * <p>
 * A <code>Scorer</code> exposes an {@link #iterator()} over documents
 * matching a query in increasing order of doc Id.
 * </p>
 * <p>
 * Document scores are computed using a given <code>Similarity</code>
 * implementation.
 * </p>
 *
 * <p><b>NOTE</b>: The values Float.Nan,
 * Float.NEGATIVE_INFINITY and Float.POSITIVE_INFINITY are
 * not valid scores.  Certain collectors (eg {@link
 * TopScoreDocCollector}) will not properly collect hits
 * with these scores.
 */
public abstract class Scorer {
  /** the Scorer's parent Weight. in some cases this may be null */
  // TODO can we clean this up?
  protected final Weight weight;

  /**
   * Constructs a Scorer
   * @param weight The scorers <code>Weight</code>.
   */
  protected Scorer(Weight weight) {
    this.weight = weight;
  }

  /**
   * Returns the doc ID that is currently being scored.
   * This will return {@code -1} if the {@link #iterator()} is not positioned
   * or {@link DocIdSetIterator#NO_MORE_DOCS} if it has been entirely consumed.
   * @see DocIdSetIterator#docID()
   */
  public abstract int docID();

  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link DocIdSetIterator#nextDoc()} or
   * {@link DocIdSetIterator#advance(int)} is called on the {@link #iterator()}
   * the first time, or when called from within {@link LeafCollector#collect}.
   */
  public abstract float score() throws IOException;

  /** Returns the freq of this Scorer on the current document */
  public abstract int freq() throws IOException;

  /** returns parent Weight
   * @lucene.experimental
   */
  public Weight getWeight() {
    return weight;
  }
  
  /** Returns child sub-scorers
   * @lucene.experimental */
  public Collection<ChildScorer> getChildren() {
    return Collections.emptyList();
  }
  
  /** A child Scorer and its relationship to its parent.
   * the meaning of the relationship depends upon the parent query. 
   * @lucene.experimental */
  public static class ChildScorer {
    /**
     * Child Scorer. (note this is typically a direct child, and may
     * itself also have children).
     */
    public final Scorer child;
    /**
     * An arbitrary string relating this scorer to the parent.
     */
    public final String relationship;
    
    /**
     * Creates a new ChildScorer node with the specified relationship.
     * <p>
     * The relationship can be any be any string that makes sense to 
     * the parent Scorer. 
     */
    public ChildScorer(Scorer child, String relationship) {
      this.child = child;
      this.relationship = relationship;
    }
  }

  /**
   * Return a {@link DocIdSetIterator} over matching documents.
   *
   * The returned iterator will either be positioned on {@code -1} if no
   * documents have been scored yet, {@link DocIdSetIterator#NO_MORE_DOCS}
   * if all documents have been scored already, or the last document id that
   * has been scored otherwise.
   *
   * The returned iterator is a view: calling this method several times will
   * return iterators that have the same state.
   */
  public abstract DocIdSetIterator iterator();

  /**
   * Optional method: Return a {@link TwoPhaseIterator} view of this
   * {@link Scorer}. A return value of {@code null} indicates that
   * two-phase iteration is not supported.
   *
   * Note that the returned {@link TwoPhaseIterator}'s
   * {@link TwoPhaseIterator#approximation() approximation} must
   * advance synchronously with the {@link #iterator()}: advancing the
   * approximation must advance the iterator and vice-versa.
   *
   * Implementing this method is typically useful on {@link Scorer}s
   * that have a high per-document overhead in order to confirm matches.
   *
   * The default implementation returns {@code null}.
   */
  public TwoPhaseIterator twoPhaseIterator() {
    return null;
  }
}
