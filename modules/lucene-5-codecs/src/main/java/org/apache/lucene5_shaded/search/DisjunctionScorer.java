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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene5_shaded.util.PriorityQueue;

/**
 * Base class for Scorers that score disjunctions.
 */
abstract class DisjunctionScorer extends Scorer {

  private final boolean needsScores;

  private final DisiPriorityQueue subScorers;
  private final DisjunctionDISIApproximation approximation;
  private final TwoPhase twoPhase;

  protected DisjunctionScorer(Weight weight, List<Scorer> subScorers, boolean needsScores) {
    super(weight);
    if (subScorers.size() <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }
    this.subScorers = new DisiPriorityQueue(subScorers.size());
    for (Scorer scorer : subScorers) {
      final DisiWrapper w = new DisiWrapper(scorer);
      this.subScorers.add(w);
    }
    this.needsScores = needsScores;
    this.approximation = new DisjunctionDISIApproximation(this.subScorers);

    boolean hasApproximation = false;
    float sumMatchCost = 0;
    long sumApproxCost = 0;
    // Compute matchCost as the average over the matchCost of the subScorers.
    // This is weighted by the cost, which is an expected number of matching documents.
    for (DisiWrapper w : this.subScorers) {
      long costWeight = (w.cost <= 1) ? 1 : w.cost;
      sumApproxCost += costWeight;
      if (w.twoPhaseView != null) {
        hasApproximation = true;
        sumMatchCost += w.matchCost * costWeight;
      }
    }

    if (hasApproximation == false) { // no sub scorer supports approximations
      twoPhase = null;
    } else {
      final float matchCost = sumMatchCost / sumApproxCost;
      twoPhase = new TwoPhase(approximation, matchCost);
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    if (twoPhase != null) {
      return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
    } else {
      return approximation;
    }
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhase;
  }

  private class TwoPhase extends TwoPhaseIterator {

    private final float matchCost;
    // list of verified matches on the current doc
    DisiWrapper verifiedMatches;
    // priority queue of approximations on the current doc that have not been verified yet
    final PriorityQueue<DisiWrapper> unverifiedMatches;

    private TwoPhase(DocIdSetIterator approximation, float matchCost) {
      super(approximation);
      this.matchCost = matchCost;
      unverifiedMatches = new PriorityQueue<DisiWrapper>(DisjunctionScorer.this.subScorers.size()) {
        @Override
        protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
          return a.matchCost < b.matchCost;
        }
      };
    }

    DisiWrapper getSubMatches() throws IOException {
      // iteration order does not matter
      for (DisiWrapper w : unverifiedMatches) {
        if (w.twoPhaseView.matches()) {
          w.next = verifiedMatches;
          verifiedMatches = w;
        }
      }
      unverifiedMatches.clear();
      return verifiedMatches;
    }
    
    @Override
    public boolean matches() throws IOException {
      verifiedMatches = null;
      unverifiedMatches.clear();
      
      for (DisiWrapper w = subScorers.topList(); w != null; ) {
        DisiWrapper next = w.next;
        
        if (w.twoPhaseView == null) {
          // implicitly verified, move it to verifiedMatches
          w.next = verifiedMatches;
          verifiedMatches = w;
          
          if (needsScores == false) {
            // we can stop here
            return true;
          }
        } else {
          unverifiedMatches.add(w);
        }
        w = next;
      }
      
      if (verifiedMatches != null) {
        return true;
      }
      
      // verify subs that have an two-phase iterator
      // least-costly ones first
      while (unverifiedMatches.size() > 0) {
        DisiWrapper w = unverifiedMatches.pop();
        if (w.twoPhaseView.matches()) {
          w.next = null;
          verifiedMatches = w;
          return true;
        }
      }
      
      return false;
    }
    
    @Override
    public float matchCost() {
      return matchCost;
    }
  }

  @Override
  public final int docID() {
   return subScorers.top().doc;
  }

  DisiWrapper getSubMatches() throws IOException {
    if (twoPhase == null) {
      return subScorers.topList();
    } else {
      return twoPhase.getSubMatches();
    }
  }

  @Override
  public final int freq() throws IOException {
    DisiWrapper subMatches = getSubMatches();
    int freq = 1;
    for (DisiWrapper w = subMatches.next; w != null; w = w.next) {
      freq += 1;
    }
    return freq;
  }

  @Override
  public final float score() throws IOException {
    return score(getSubMatches());
  }

  /** Compute the score for the given linked list of scorers. */
  protected abstract float score(DisiWrapper topList) throws IOException;

  @Override
  public final Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>();
    for (DisiWrapper scorer : subScorers) {
      children.add(new ChildScorer(scorer.scorer, "SHOULD"));
    }
    return children;
  }

}
