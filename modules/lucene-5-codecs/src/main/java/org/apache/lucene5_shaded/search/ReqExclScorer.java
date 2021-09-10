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

/** A Scorer for queries with a required subscorer
 * and an excluding (prohibited) sub {@link Scorer}.
 */
class ReqExclScorer extends Scorer {

  private final Scorer reqScorer;
  // approximations of the scorers, or the scorers themselves if they don't support approximations
  private final DocIdSetIterator reqApproximation;
  private final DocIdSetIterator exclApproximation;
  // two-phase views of the scorers, or null if they do not support approximations
  private final TwoPhaseIterator reqTwoPhaseIterator;
  private final TwoPhaseIterator exclTwoPhaseIterator;

  /** Construct a <code>ReqExclScorer</code>.
   * @param reqScorer The scorer that must match, except where
   * @param exclScorer indicates exclusion.
   */
  public ReqExclScorer(Scorer reqScorer, Scorer exclScorer) {
    super(reqScorer.weight);
    this.reqScorer = reqScorer;
    reqTwoPhaseIterator = reqScorer.twoPhaseIterator();
    if (reqTwoPhaseIterator == null) {
      reqApproximation = reqScorer.iterator();
    } else {
      reqApproximation = reqTwoPhaseIterator.approximation();
    }
    exclTwoPhaseIterator = exclScorer.twoPhaseIterator();
    if (exclTwoPhaseIterator == null) {
      exclApproximation = exclScorer.iterator();
    } else {
      exclApproximation = exclTwoPhaseIterator.approximation();
    }
  }

  /** Confirms whether or not the given {@link TwoPhaseIterator}
   *  matches on the current document. */
  private static boolean matchesOrNull(TwoPhaseIterator it) throws IOException {
    return it == null || it.matches();
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public int docID() {
    return reqApproximation.docID();
  }

  @Override
  public int freq() throws IOException {
    return reqScorer.freq();
  }

  @Override
  public float score() throws IOException {
    return reqScorer.score(); // reqScorer may be null when next() or skipTo() already return false
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return Collections.singleton(new ChildScorer(reqScorer, "MUST"));
  }

  /**
   * Estimation of the number of operations required to call DISI.advance.
   * This is likely completely wrong, especially given that the cost of
   * this method usually depends on how far you want to advance, but it's
   * probably better than nothing.
   */
  private static final int ADVANCE_COST = 10;

  private static float matchCost(
      DocIdSetIterator reqApproximation,
      TwoPhaseIterator reqTwoPhaseIterator,
      DocIdSetIterator exclApproximation,
      TwoPhaseIterator exclTwoPhaseIterator) {
    float matchCost = 2; // we perform 2 comparisons to advance exclApproximation
    if (reqTwoPhaseIterator != null) {
      // this two-phase iterator must always be matched
      matchCost += reqTwoPhaseIterator.matchCost();
    }

    // match cost of the prohibited clause: we need to advance the approximation
    // and match the two-phased iterator
    final float exclMatchCost = ADVANCE_COST
        + (exclTwoPhaseIterator == null ? 0 : exclTwoPhaseIterator.matchCost());

    // upper value for the ratio of documents that reqApproximation matches that
    // exclApproximation also matches
    float ratio;
    if (reqApproximation.cost() <= 0) {
      ratio = 1f;
    } else if (exclApproximation.cost() <= 0) {
      ratio = 0f;
    } else {
      ratio = (float) Math.min(reqApproximation.cost(), exclApproximation.cost()) / reqApproximation.cost();
    }
    matchCost += ratio * exclMatchCost;

    return matchCost;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    final float matchCost = matchCost(reqApproximation, reqTwoPhaseIterator, exclApproximation, exclTwoPhaseIterator);

    if (reqTwoPhaseIterator == null
        || (exclTwoPhaseIterator != null && reqTwoPhaseIterator.matchCost() <= exclTwoPhaseIterator.matchCost())) {
      // reqTwoPhaseIterator is LESS costly than exclTwoPhaseIterator, check it first
      return new TwoPhaseIterator(reqApproximation) {

        @Override
        public boolean matches() throws IOException {
          final int doc = reqApproximation.docID();
          // check if the doc is not excluded
          int exclDoc = exclApproximation.docID();
          if (exclDoc < doc) {
            exclDoc = exclApproximation.advance(doc);
          }
          if (exclDoc != doc) {
            return matchesOrNull(reqTwoPhaseIterator);
          }
          return matchesOrNull(reqTwoPhaseIterator) && !matchesOrNull(exclTwoPhaseIterator);
        }

        @Override
        public float matchCost() {
          return matchCost;
        }
      };
    } else {
      // reqTwoPhaseIterator is MORE costly than exclTwoPhaseIterator, check it first
      return new TwoPhaseIterator(reqApproximation) {

        @Override
        public boolean matches() throws IOException {
          final int doc = reqApproximation.docID();
          // check if the doc is not excluded
          int exclDoc = exclApproximation.docID();
          if (exclDoc < doc) {
            exclDoc = exclApproximation.advance(doc);
          }
          if (exclDoc != doc) {
            return matchesOrNull(reqTwoPhaseIterator);
          }
          return !matchesOrNull(exclTwoPhaseIterator) && matchesOrNull(reqTwoPhaseIterator);
        }

        @Override
        public float matchCost() {
          return matchCost;
        }
      };
    }
  }
}
