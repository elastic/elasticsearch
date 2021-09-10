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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.search.DisiPriorityQueue;
import org.apache.lucene5_shaded.search.DisiWrapper;
import org.apache.lucene5_shaded.search.DisjunctionDISIApproximation;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.search.TwoPhaseIterator;
import org.apache.lucene5_shaded.util.ToStringUtils;


/** Matches the union of its clauses.
 */
public final class SpanOrQuery extends SpanQuery {
  private List<SpanQuery> clauses;
  private String field;

  /** Construct a SpanOrQuery merging the provided clauses.
   * All clauses must have the same field.
   */
  public SpanOrQuery(SpanQuery... clauses) {
    this.clauses = new ArrayList<>(clauses.length);
    for (SpanQuery seq : clauses) {
      addClause(seq);
    }
  }

  /** Adds a clause to this query
   *  @deprecated All clauses should be provided at {@link #SpanOrQuery(SpanQuery...) construction-time}. */
  @Deprecated
  public final void addClause(SpanQuery clause) {
    if (field == null) {
      field = clause.getField();
    } else if (clause.getField() != null && !clause.getField().equals(field)) {
      throw new IllegalArgumentException("Clauses must have same field.");
    }
    this.clauses.add(clause);
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  @Override
  public String getField() { return field; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    SpanOrQuery rewritten = new SpanOrQuery();
    boolean actuallyRewritten = false;
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      actuallyRewritten |= query != c;
      rewritten.addClause(query);
    }
    if (actuallyRewritten) {
      return rewritten;
    }
    return super.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanOr([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (! super.equals(o)) {
      return false;
    }
    final SpanOrQuery that = (SpanOrQuery) o;
    return clauses.equals(that.clauses);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = (h * 7) ^ clauses.hashCode();
    return h;
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    List<SpanWeight> subWeights = new ArrayList<>(clauses.size());
    for (SpanQuery q : clauses) {
      subWeights.add(q.createWeight(searcher, false));
    }
    return new SpanOrWeight(searcher, needsScores ? getTermContexts(subWeights) : null, subWeights);
  }

  public class SpanOrWeight extends SpanWeight {
    final List<SpanWeight> subWeights;

    public SpanOrWeight(IndexSearcher searcher, Map<Term, TermContext> terms, List<SpanWeight> subWeights) throws IOException {
      super(SpanOrQuery.this, searcher, terms);
      this.subWeights = subWeights;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (final SpanWeight w: subWeights) {
        w.extractTerms(terms);
      }
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      for (SpanWeight w : subWeights) {
        w.extractTermContexts(contexts);
      }
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings)
        throws IOException {

      final ArrayList<Spans> subSpans = new ArrayList<>(clauses.size());

      for (SpanWeight w : subWeights) {
        Spans spans = w.getSpans(context, requiredPostings);
        if (spans != null) {
          subSpans.add(spans);
        }
      }

      if (subSpans.size() == 0) {
        return null;
      } else if (subSpans.size() == 1) {
        return new ScoringWrapperSpans(subSpans.get(0), getSimScorer(context));
      }

      final DisiPriorityQueue byDocQueue = new DisiPriorityQueue(subSpans.size());
      for (Spans spans : subSpans) {
        byDocQueue.add(new DisiWrapper(spans));
      }

      final SpanPositionQueue byPositionQueue = new SpanPositionQueue(subSpans.size()); // when empty use -1

      return new Spans() {
        Spans topPositionSpans = null;

        @Override
        public int nextDoc() throws IOException {
          topPositionSpans = null;
          DisiWrapper topDocSpans = byDocQueue.top();
          int currentDoc = topDocSpans.doc;
          do {
            topDocSpans.doc = topDocSpans.iterator.nextDoc();
            topDocSpans = byDocQueue.updateTop();
          } while (topDocSpans.doc == currentDoc);
          return topDocSpans.doc;
        }

        @Override
        public int advance(int target) throws IOException {
          topPositionSpans = null;
          DisiWrapper topDocSpans = byDocQueue.top();
          do {
            topDocSpans.doc = topDocSpans.iterator.advance(target);
            topDocSpans = byDocQueue.updateTop();
          } while (topDocSpans.doc < target);
          return topDocSpans.doc;
        }

        @Override
        public int docID() {
          DisiWrapper topDocSpans = byDocQueue.top();
          return topDocSpans.doc;
        }

        @Override
        public TwoPhaseIterator asTwoPhaseIterator() {
          float sumMatchCost = 0; // See also DisjunctionScorer.asTwoPhaseIterator()
          long sumApproxCost = 0;

          for (DisiWrapper w : byDocQueue) {
            if (w.twoPhaseView != null) {
              long costWeight = (w.cost <= 1) ? 1 : w.cost;
              sumMatchCost += w.twoPhaseView.matchCost() * costWeight;
              sumApproxCost += costWeight;
            }
          }

          if (sumApproxCost == 0) { // no sub spans supports approximations
            computePositionsCost();
            return null;
          }

          final float matchCost = sumMatchCost / sumApproxCost;

          return new TwoPhaseIterator(new DisjunctionDISIApproximation(byDocQueue)) {
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

        float positionsCost = -1;

        void computePositionsCost() {
          float sumPositionsCost = 0;
          long sumCost = 0;
          for (DisiWrapper w : byDocQueue) {
            long costWeight = (w.cost <= 1) ? 1 : w.cost;
            sumPositionsCost += w.spans.positionsCost() * costWeight;
            sumCost += costWeight;
          }
          positionsCost = sumPositionsCost / sumCost;
        }

        @Override
        public float positionsCost() {
          // This may be called when asTwoPhaseIterator returned null,
          // which happens when none of the sub spans supports approximations.
          assert positionsCost > 0;
          return positionsCost;
        }

        int lastDocTwoPhaseMatched = -1;

        boolean twoPhaseCurrentDocMatches() throws IOException {
          DisiWrapper listAtCurrentDoc = byDocQueue.topList();
          // remove the head of the list as long as it does not match
          final int currentDoc = listAtCurrentDoc.doc;
          while (listAtCurrentDoc.twoPhaseView != null) {
            if (listAtCurrentDoc.twoPhaseView.matches()) {
              // use this spans for positions at current doc:
              listAtCurrentDoc.lastApproxMatchDoc = currentDoc;
              break;
            }
            // do not use this spans for positions at current doc:
            listAtCurrentDoc.lastApproxNonMatchDoc = currentDoc;
            listAtCurrentDoc = listAtCurrentDoc.next;
            if (listAtCurrentDoc == null) {
              return false;
            }
          }
          lastDocTwoPhaseMatched = currentDoc;
          topPositionSpans = null;
          return true;
        }

        void fillPositionQueue() throws IOException { // called at first nextStartPosition
          assert byPositionQueue.size() == 0;
          // add all matching Spans at current doc to byPositionQueue
          DisiWrapper listAtCurrentDoc = byDocQueue.topList();
          while (listAtCurrentDoc != null) {
            Spans spansAtDoc = listAtCurrentDoc.spans;
            if (lastDocTwoPhaseMatched == listAtCurrentDoc.doc) { // matched by DisjunctionDisiApproximation
              if (listAtCurrentDoc.twoPhaseView != null) { // matched by approximation
                if (listAtCurrentDoc.lastApproxNonMatchDoc == listAtCurrentDoc.doc) { // matches() returned false
                  spansAtDoc = null;
                } else {
                  if (listAtCurrentDoc.lastApproxMatchDoc != listAtCurrentDoc.doc) {
                    if (!listAtCurrentDoc.twoPhaseView.matches()) {
                      spansAtDoc = null;
                    }
                  }
                }
              }
            }

            if (spansAtDoc != null) {
              assert spansAtDoc.docID() == listAtCurrentDoc.doc;
              assert spansAtDoc.startPosition() == -1;
              spansAtDoc.nextStartPosition();
              assert spansAtDoc.startPosition() != NO_MORE_POSITIONS;
              byPositionQueue.add(spansAtDoc);
            }
            listAtCurrentDoc = listAtCurrentDoc.next;
          }
          assert byPositionQueue.size() > 0;
        }

        @Override
        public int nextStartPosition() throws IOException {
          if (topPositionSpans == null) {
            byPositionQueue.clear();
            fillPositionQueue(); // fills byPositionQueue at first position
            topPositionSpans = byPositionQueue.top();
          } else {
            topPositionSpans.nextStartPosition();
            topPositionSpans = byPositionQueue.updateTop();
          }
          return topPositionSpans.startPosition();
        }

        @Override
        public int startPosition() {
          return topPositionSpans == null ? -1 : topPositionSpans.startPosition();
        }

        @Override
        public int endPosition() {
          return topPositionSpans == null ? -1 : topPositionSpans.endPosition();
        }

        @Override
        public int width() {
          return topPositionSpans.width();
        }

        @Override
        public void collect(SpanCollector collector) throws IOException {
          topPositionSpans.collect(collector);
        }

        @Override
        public String toString() {
          return "spanOr("+SpanOrQuery.this+")@"+docID()+": "+startPosition()+" - "+endPosition();
        }

        long cost = -1;

        @Override
        public long cost() {
          if (cost == -1) {
            cost = 0;
            for (Spans spans : subSpans) {
              cost += spans.cost();
            }
          }
          return cost;
        }
      };
    }
  }

}

