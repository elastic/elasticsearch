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
package org.apache.lucene5_shaded.search.spans;


import java.io.IOException;
import java.util.List;

import org.apache.lucene5_shaded.search.DocIdSetIterator;
import org.apache.lucene5_shaded.search.ConjunctionDISI;
import org.apache.lucene5_shaded.search.TwoPhaseIterator;

/**
 * Common super class for multiple sub spans required in a document.
 */
abstract class ConjunctionSpans extends Spans {
  final Spans[] subSpans; // in query order
  final DocIdSetIterator conjunction; // use to move to next doc with all clauses
  boolean atFirstInCurrentDoc; // a first start position is available in current doc for nextStartPosition
  boolean oneExhaustedInCurrentDoc; // one subspans exhausted in current doc

  ConjunctionSpans(List<Spans> subSpans) {
    if (subSpans.size() < 2) {
      throw new IllegalArgumentException("Less than 2 subSpans.size():" + subSpans.size());
    }
    this.subSpans = subSpans.toArray(new Spans[subSpans.size()]);
    this.conjunction = ConjunctionDISI.intersectSpans(subSpans);
    this.atFirstInCurrentDoc = true; // ensure for doc -1 that start/end positions are -1
  }

  @Override
  public int docID() {
    return conjunction.docID();
  }

  @Override
  public long cost() {
    return conjunction.cost();
  }

  @Override
  public int nextDoc() throws IOException {
    return (conjunction.nextDoc() == NO_MORE_DOCS)
            ? NO_MORE_DOCS
            : toMatchDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return (conjunction.advance(target) == NO_MORE_DOCS)
            ? NO_MORE_DOCS
            : toMatchDoc();
  }

  int toMatchDoc() throws IOException {
    oneExhaustedInCurrentDoc = false;
    while (true) {
      if (twoPhaseCurrentDocMatches()) {
        return docID();
      }
      if (conjunction.nextDoc() == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
    }
  }


  abstract boolean twoPhaseCurrentDocMatches() throws IOException;

  /**
   * Return a {@link TwoPhaseIterator} view of this ConjunctionSpans.
   */
  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    float totalMatchCost = 0;
    // Compute the matchCost as the total matchCost/positionsCostant of the sub spans.
    for (Spans spans : subSpans) {
      TwoPhaseIterator tpi = spans.asTwoPhaseIterator();
      if (tpi != null) {
        totalMatchCost += tpi.matchCost();
      } else {
        totalMatchCost += spans.positionsCost();
      }
    }
    final float matchCost = totalMatchCost;

    return new TwoPhaseIterator(conjunction) {
      @Override
      public boolean matches() throws IOException {
        return twoPhaseCurrentDocMatches();
      }

      @Override
      public float matchCost() {
        return matchCost;
      }
    };
  }

  @Override
  public float positionsCost() {
    throw new UnsupportedOperationException(); // asTwoPhaseIterator never returns null here.
  }

  public Spans[] getSubSpans() {
    return subSpans;
  }
}
