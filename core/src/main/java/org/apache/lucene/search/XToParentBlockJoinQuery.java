// Copied from Lucene 6.6.0, do not modify

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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.util.BitSet;

/**
 * This query requires that you index
 * children and parent docs as a single block, using the
 * {@link IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link
 * IndexWriter#updateDocuments IndexWriter.updateDocuments()} API.  In each block, the
 * child documents must appear first, ending with the parent
 * document.  At search time you provide a Filter
 * identifying the parents, however this Filter must provide
 * an {@link BitSet} per sub-reader.
 *
 * <p>Once the block index is built, use this query to wrap
 * any sub-query matching only child docs and join matches in that
 * child document space up to the parent document space.
 * You can then use this Query as a clause with
 * other queries in the parent document space.</p>
 *
 * <p>See {@link ToChildBlockJoinQuery} if you need to join
 * in the reverse order.
 *
 * <p>The child documents must be orthogonal to the parent
 * documents: the wrapped child query must never
 * return a parent document.</p>
 *
 * <p>See {@link org.apache.lucene.search.join} for an
 * overview. </p>
 *
 */
public class XToParentBlockJoinQuery extends Query {

  private final BitSetProducer parentsFilter;
  private final Query childQuery;
  private final ScoreMode scoreMode;

  /** Create a ToParentBlockJoinQuery.
   *
   * @param childQuery Query matching child documents.
   * @param parentsFilter Filter identifying the parent documents.
   * @param scoreMode How to aggregate multiple child scores
   * into a single parent score.
   **/
  public XToParentBlockJoinQuery(Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode) {
    super();
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new BlockJoinWeight(this, childQuery.createWeight(searcher, needsScores), parentsFilter,
            needsScores ? scoreMode : ScoreMode.None);
  }

  /** Return our child query. */
  public Query getChildQuery() {
    return childQuery;
  }

  private static class BlockJoinWeight extends FilterWeight {
    private final BitSetProducer parentsFilter;
    private final ScoreMode scoreMode;

    BlockJoinWeight(Query joinQuery, Weight childWeight, BitSetProducer parentsFilter, ScoreMode scoreMode) {
      super(joinQuery, childWeight);
      this.parentsFilter = parentsFilter;
      this.scoreMode = scoreMode;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      final ScorerSupplier scorerSupplier = scorerSupplier(context);
      if (scorerSupplier == null) {
        return null;
      }
      return scorerSupplier.get(false);
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // parent document space
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      final ScorerSupplier childScorerSupplier = in.scorerSupplier(context);
      if (childScorerSupplier == null) {
        return null;
      }

      // NOTE: this does not take accept docs into account, the responsibility
      // to not match deleted docs is on the scorer
      final BitSet parents = parentsFilter.getBitSet(context);
      if (parents == null) {
        // No matches
        return null;
      }

      return new ScorerSupplier() {

        @Override
        public Scorer get(boolean randomAccess) throws IOException {
          return new BlockJoinScorer(BlockJoinWeight.this, childScorerSupplier.get(randomAccess), parents, scoreMode);
        }

        @Override
        public long cost() {
          return childScorerSupplier.cost();
        }
      };
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      BlockJoinScorer scorer = (BlockJoinScorer) scorer(context);
      if (scorer != null && scorer.iterator().advance(doc) == doc) {
        return scorer.explain(context, in);
      }
      return Explanation.noMatch("Not a match");
    }
  }

  private static class ParentApproximation extends DocIdSetIterator {

    private final DocIdSetIterator childApproximation;
    private final BitSet parentBits;
    private int doc = -1;

    ParentApproximation(DocIdSetIterator childApproximation, BitSet parentBits) {
      this.childApproximation = childApproximation;
      this.parentBits = parentBits;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= parentBits.length()) {
        return doc = NO_MORE_DOCS;
      }
      final int firstChildTarget = target == 0 ? 0 : parentBits.prevSetBit(target - 1) + 1;
      int childDoc = childApproximation.docID();
      if (childDoc < firstChildTarget) {
        childDoc = childApproximation.advance(firstChildTarget);
      }
      if (childDoc >= parentBits.length() - 1) {
        return doc = NO_MORE_DOCS;
      }
      return doc = parentBits.nextSetBit(childDoc + 1);
    }

    @Override
    public long cost() {
      return childApproximation.cost();
    }
  }

  private static class ParentTwoPhase extends TwoPhaseIterator {

    private final ParentApproximation parentApproximation;
    private final DocIdSetIterator childApproximation;
    private final TwoPhaseIterator childTwoPhase;

    ParentTwoPhase(ParentApproximation parentApproximation, TwoPhaseIterator childTwoPhase) {
      super(parentApproximation);
      this.parentApproximation = parentApproximation;
      this.childApproximation = childTwoPhase.approximation();
      this.childTwoPhase = childTwoPhase;
    }

    @Override
    public boolean matches() throws IOException {
      assert childApproximation.docID() < parentApproximation.docID();
      do {
        if (childTwoPhase.matches()) {
          return true;
        }
      } while (childApproximation.nextDoc() < parentApproximation.docID());
      return false;
    }

    @Override
    public float matchCost() {
      // TODO: how could we compute a match cost?
      return childTwoPhase.matchCost() + 10;
    }
  }

  static class BlockJoinScorer extends Scorer {
    private final Scorer childScorer;
    private final BitSet parentBits;
    private final ScoreMode scoreMode;
    private final DocIdSetIterator childApproximation;
    private final TwoPhaseIterator childTwoPhase;
    private final ParentApproximation parentApproximation;
    private final ParentTwoPhase parentTwoPhase;
    private float score;
    private int freq;

    BlockJoinScorer(Weight weight, Scorer childScorer, BitSet parentBits, ScoreMode scoreMode) {
      super(weight);
      //System.out.println("Q.init firstChildDoc=" + firstChildDoc);
      this.parentBits = parentBits;
      this.childScorer = childScorer;
      this.scoreMode = scoreMode;
      childTwoPhase = childScorer.twoPhaseIterator();
      if (childTwoPhase == null) {
        childApproximation = childScorer.iterator();
        parentApproximation = new ParentApproximation(childApproximation, parentBits);
        parentTwoPhase = null;
      } else {
        childApproximation = childTwoPhase.approximation();
        parentApproximation = new ParentApproximation(childTwoPhase.approximation(), parentBits);
        parentTwoPhase = new ParentTwoPhase(parentApproximation, childTwoPhase);
      }
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(childScorer, "BLOCK_JOIN"));
    }

    @Override
    public DocIdSetIterator iterator() {
      if (parentTwoPhase == null) {
        // the approximation is exact
        return parentApproximation;
      } else {
        return TwoPhaseIterator.asDocIdSetIterator(parentTwoPhase);
      }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return parentTwoPhase;
    }

    @Override
    public int docID() {
      return parentApproximation.docID();
    }

    @Override
    public float score() throws IOException {
      setScoreAndFreq();
      return score;
    }
    
    @Override
    public int freq() throws IOException {
      setScoreAndFreq();
      return freq;
    }

    private void setScoreAndFreq() throws IOException {
      if (childApproximation.docID() >= parentApproximation.docID()) {
        return;
      }
      double score = scoreMode == ScoreMode.None ? 0 : childScorer.score();
      int freq = 1;
      while (childApproximation.nextDoc() < parentApproximation.docID()) {
        if (childTwoPhase == null || childTwoPhase.matches()) {
          final float childScore = childScorer.score();
          freq += 1;
          switch (scoreMode) {
            case Total:
            case Avg:
              score += childScore;
              break;
            case Min:
              score = Math.min(score, childScore);
              break;
            case Max:
              // BEGIN CHANGE
              score = Math.max(score, childScore);
              // BEGIN CHANGE
              break;
            case None:
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      if (childApproximation.docID() == parentApproximation.docID() && (childTwoPhase == null || childTwoPhase.matches())) {
        throw new IllegalStateException("Child query must not match same docs with parent filter. "
            + "Combine them as must clauses (+) to find a problem doc. "
            + "docId=" + parentApproximation.docID() + ", " + childScorer.getClass());
      }
      if (scoreMode == ScoreMode.Avg) {
        score /= freq;
      }
      this.score = (float) score;
      this.freq = freq;
    }

    public Explanation explain(LeafReaderContext context, Weight childWeight) throws IOException {
      int prevParentDoc = parentBits.prevSetBit(parentApproximation.docID() - 1);
      int start = context.docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
      int end = context.docBase + parentApproximation.docID() - 1; // -1 b/c parentDoc is parent doc

      Explanation bestChild = null;
      int matches = 0;
      for (int childDoc = start; childDoc <= end; childDoc++) {
        Explanation child = childWeight.explain(context, childDoc - context.docBase);
        if (child.isMatch()) {
          matches++;
          if (bestChild == null || child.getValue() > bestChild.getValue()) {
            bestChild = child;
          }
        }
      }

      assert freq() == matches;
      return Explanation.match(score(), String.format(Locale.ROOT,
          "Score based on %d child docs in range from %d to %d, best match:", matches, start, end), bestChild
      );
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query childRewrite = childQuery.rewrite(reader);
    if (childRewrite != childQuery) {
      return new XToParentBlockJoinQuery(childRewrite,
                                parentsFilter,
                                scoreMode);
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public String toString(String field) {
    return "ToParentBlockJoinQuery ("+childQuery.toString()+")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(XToParentBlockJoinQuery other) {
    return childQuery.equals(other.childQuery) &&
           parentsFilter.equals(other.parentsFilter) &&
           scoreMode == other.scoreMode;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = classHash();
    hash = prime * hash + childQuery.hashCode();
    hash = prime * hash + scoreMode.hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }
}
