/* @notice
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
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */
package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SmallFloat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * Copy of {@link CombinedFieldQuery} that contains a fix for LUCENE-9999.
 * TODO: remove once LUCENE-9999 is fixed and integrated
 *
 * A {@link Query} that treats multiple fields as a single stream and scores terms as if you had
 * indexed them as a single term in a single field.
 *
 * <p>The query works as follows:
 *
 * <ol>
 *   <li>Given a list of fields and weights, it pretends there is a synthetic combined field where
 *       all terms have been indexed. It computes new term and collection statistics for this
 *       combined field.
 *   <li>It uses a disjunction iterator and {@link IndexSearcher#getSimilarity} to score documents.
 * </ol>
 *
 * <p>In order for a similarity to be compatible, {@link Similarity#computeNorm} must be additive:
 * the norm of the combined field is the sum of norms for each individual field. The norms must also
 * be encoded using {@link SmallFloat#intToByte4}. These requirements hold for all similarities that
 * compute norms the same way as {@link SimilarityBase#computeNorm}, which includes {@link
 * BM25Similarity} and {@link DFRSimilarity}. Per-field similarities are not supported.
 *
 * <p>The scoring is based on BM25F's simple formula described in:
 * http://www.staff.city.ac.uk/~sb317/papers/foundations_bm25_review.pdf. This query implements the
 * same approach but allows other similarities besides {@link
 * org.apache.lucene.search.similarities.BM25Similarity}.
 *
 */
public final class XCombinedFieldQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(XCombinedFieldQuery.class);

  /** A builder for {@link XCombinedFieldQuery}. */
  public static class Builder {
    private final Map<String, FieldAndWeight> fieldAndWeights = new HashMap<>();
    private final Set<BytesRef> termsSet = new HashSet<>();

    /**
     * Adds a field to this builder.
     *
     * @param field The field name.
     */
    public Builder addField(String field) {
      return addField(field, 1f);
    }

    /**
     * Adds a field to this builder.
     *
     * @param field The field name.
     * @param weight The weight associated to this field.
     */
    public Builder addField(String field, float weight) {
      if (weight < 1) {
        throw new IllegalArgumentException("weight must be greater or equal to 1");
      }
      fieldAndWeights.put(field, new FieldAndWeight(field, weight));
      return this;
    }

    /** Adds a term to this builder. */
    public Builder addTerm(BytesRef term) {
      if (termsSet.size() > BooleanQuery.getMaxClauseCount()) {
        throw new BooleanQuery.TooManyClauses();
      }
      termsSet.add(term);
      return this;
    }

    /** Builds the {@link XCombinedFieldQuery}. */
    public XCombinedFieldQuery build() {
      int size = fieldAndWeights.size() * termsSet.size();
      if (size > BooleanQuery.getMaxClauseCount()) {
        throw new BooleanQuery.TooManyClauses();
      }
      BytesRef[] terms = termsSet.toArray(new BytesRef[0]);
      return new XCombinedFieldQuery(new TreeMap<>(fieldAndWeights), terms);
    }
  }

  static class FieldAndWeight {
    final String field;
    final float weight;

    FieldAndWeight(String field, float weight) {
      this.field = field;
      this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldAndWeight that = (FieldAndWeight) o;
      return Float.compare(that.weight, weight) == 0 && Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, weight);
    }
  }

  // sorted map for fields.
  private final TreeMap<String, FieldAndWeight> fieldAndWeights;
  // array of terms, sorted.
  private final BytesRef terms[];
  // array of terms per field, sorted
  private final Term fieldTerms[];

  private final long ramBytesUsed;

  private XCombinedFieldQuery(TreeMap<String, FieldAndWeight> fieldAndWeights, BytesRef[] terms) {
    this.fieldAndWeights = fieldAndWeights;
    this.terms = terms;
    int numFieldTerms = fieldAndWeights.size() * terms.length;
    if (numFieldTerms > BooleanQuery.getMaxClauseCount()) {
      throw new BooleanQuery.TooManyClauses();
    }
    this.fieldTerms = new Term[numFieldTerms];
    Arrays.sort(terms);
    int pos = 0;
    for (String field : fieldAndWeights.keySet()) {
      for (BytesRef term : terms) {
        fieldTerms[pos++] = new Term(field, term);
      }
    }

    this.ramBytesUsed =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(fieldAndWeights)
            + RamUsageEstimator.sizeOfObject(fieldTerms)
            + RamUsageEstimator.sizeOfObject(terms);
  }

  public List<Term> getTerms() {
    return Collections.unmodifiableList(Arrays.asList(fieldTerms));
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("CombinedFieldQuery((");
    int pos = 0;
    for (FieldAndWeight fieldWeight : fieldAndWeights.values()) {
      if (pos++ != 0) {
        builder.append(" ");
      }
      builder.append(fieldWeight.field);
      if (fieldWeight.weight != 1f) {
        builder.append("^");
        builder.append(fieldWeight.weight);
      }
    }
    builder.append(")(");
    pos = 0;
    for (BytesRef term : terms) {
      if (pos++ != 0) {
        builder.append(" ");
      }
      builder.append(term.utf8ToString());
    }
    builder.append("))");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (sameClassAs(o) == false) return false;
    XCombinedFieldQuery that = (XCombinedFieldQuery) o;
    return Objects.equals(fieldAndWeights, that.fieldAndWeights) && Arrays.equals(terms, that.terms);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hash(fieldAndWeights);
    result = 31 * result + Arrays.hashCode(terms);
    return result;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // optimize zero and single field cases
    if (terms.length == 0) {
      return new BooleanQuery.Builder().build();
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    Term[] selectedTerms =
        Arrays.stream(fieldTerms).filter(t -> visitor.acceptField(t.field())).toArray(Term[]::new);
    if (selectedTerms.length > 0) {
      QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
      v.consumeTerms(this, selectedTerms);
    }
  }

  private BooleanQuery rewriteToBoolean() {
    // rewrite to a simple disjunction if the score is not needed.
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Term term : fieldTerms) {
      bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
    }
    return bq.build();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    if (scoreMode.needsScores()) {
      return new CombinedFieldWeight(this, searcher, scoreMode, boost);
    } else {
      // rewrite to a simple disjunction if the score is not needed.
      Query bq = rewriteToBoolean();
      return searcher.rewrite(bq).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, boost);
    }
  }

  class CombinedFieldWeight extends Weight {
    private final IndexSearcher searcher;
    private final TermStates termStates[];
    private final Similarity.SimScorer simWeight;

    CombinedFieldWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(query);
      assert scoreMode.needsScores();
      this.searcher = searcher;
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[fieldTerms.length];
      for (int i = 0; i < termStates.length; i++) {
        FieldAndWeight field = fieldAndWeights.get(fieldTerms[i].field());
        TermStates ts = TermStates.build(searcher.getTopReaderContext(), fieldTerms[i], true);
        termStates[i] = ts;
        if (ts.docFreq() > 0) {
          TermStatistics termStats =
              searcher.termStatistics(fieldTerms[i], ts.docFreq(), ts.totalTermFreq());
          docFreq = Math.max(termStats.docFreq(), docFreq);
          totalTermFreq += (double) field.weight * termStats.totalTermFreq();
        }
      }
      if (docFreq > 0) {
        CollectionStatistics pseudoCollectionStats = mergeCollectionStatistics(searcher);
        TermStatistics pseudoTermStatistics =
            new TermStatistics(new BytesRef("pseudo_term"), docFreq, Math.max(1, totalTermFreq));
        this.simWeight =
            searcher.getSimilarity().scorer(boost, pseudoCollectionStats, pseudoTermStatistics);
      } else {
        this.simWeight = null;
      }
    }

    private CollectionStatistics mergeCollectionStatistics(IndexSearcher searcher)
        throws IOException {
      long maxDoc = searcher.getIndexReader().maxDoc();
      long docCount = 0;
      long sumTotalTermFreq = 0;
      long sumDocFreq = 0;
      for (FieldAndWeight fieldWeight : fieldAndWeights.values()) {
        CollectionStatistics collectionStats = searcher.collectionStatistics(fieldWeight.field);
        if (collectionStats != null) {
          docCount = Math.max(collectionStats.docCount(), docCount);
          sumDocFreq = Math.max(collectionStats.sumDocFreq(), sumDocFreq);
          sumTotalTermFreq += (double) fieldWeight.weight * collectionStats.sumTotalTermFreq();
        }
      }

      return new CollectionStatistics(
          "pseudo_field", maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
    }

    @Override
    public void extractTerms(Set<Term> termSet) {
        termSet.addAll(Arrays.asList(fieldTerms));
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      Weight weight =
          searcher.rewrite(rewriteToBoolean()).createWeight(searcher, ScoreMode.COMPLETE, 1f);
      return weight.matches(context, doc);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          final float freq;
          if (scorer instanceof CombinedFieldScorer) {
            freq = ((CombinedFieldScorer) scorer).freq();
          } else {
            assert scorer instanceof TermScorer;
            freq = ((TermScorer) scorer).freq();
          }
          final XMultiNormsLeafSimScorer docScorer =
              new XMultiNormsLeafSimScorer(
                  simWeight, context.reader(), fieldAndWeights.values(), true);
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + "), result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      List<PostingsEnum> iterators = new ArrayList<>();
      List<FieldAndWeight> fields = new ArrayList<>();
      for (int i = 0; i < fieldTerms.length; i++) {
        TermState state = termStates[i].get(context);
        if (state != null) {
          TermsEnum termsEnum = context.reader().terms(fieldTerms[i].field()).iterator();
          termsEnum.seekExact(fieldTerms[i].bytes(), state);
          PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
          iterators.add(postingsEnum);
          fields.add(fieldAndWeights.get(fieldTerms[i].field()));
        }
      }

      if (iterators.isEmpty()) {
        return null;
      }

      // we must optimize this case (term not in segment), disjunctions require >= 2 subs
      if (iterators.size() == 1) {
        final LeafSimScorer scoringSimScorer =
            new LeafSimScorer(simWeight, context.reader(), fields.get(0).field, true);
        return new TermScorer(this, iterators.get(0), scoringSimScorer);
      }
      final XMultiNormsLeafSimScorer scoringSimScorer =
          new XMultiNormsLeafSimScorer(simWeight, context.reader(), fields, true);
      LeafSimScorer nonScoringSimScorer =
          new LeafSimScorer(simWeight, context.reader(), "pseudo_field", false);
      // we use termscorers + disjunction as an impl detail
      DisiPriorityQueue queue = new DisiPriorityQueue(iterators.size());
      for (int i = 0; i < iterators.size(); i++) {
        float weight = fields.get(i).weight;
        queue.add(
            new WeightedDisiWrapper(
                new TermScorer(this, iterators.get(i), nonScoringSimScorer), weight));
      }
      // Even though it is called approximation, it is accurate since none of
      // the sub iterators are two-phase iterators.
      DocIdSetIterator iterator = new DisjunctionDISIApproximation(queue);
      return new CombinedFieldScorer(this, queue, iterator, scoringSimScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class WeightedDisiWrapper extends DisiWrapper {
    final float weight;

    WeightedDisiWrapper(Scorer scorer, float weight) {
      super(scorer);
      this.weight = weight;
    }

    float freq() throws IOException {
      return weight * ((PostingsEnum) iterator).freq();
    }
  }

  private static class CombinedFieldScorer extends Scorer {
    private final DisiPriorityQueue queue;
    private final DocIdSetIterator iterator;
    private final XMultiNormsLeafSimScorer simScorer;

    CombinedFieldScorer(
        Weight weight,
        DisiPriorityQueue queue,
        DocIdSetIterator iterator,
        XMultiNormsLeafSimScorer simScorer) {
      super(weight);
      this.queue = queue;
      this.iterator = iterator;
      this.simScorer = simScorer;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    float freq() throws IOException {
      DisiWrapper w = queue.topList();
      float freq = ((WeightedDisiWrapper) w).freq();
      for (w = w.next; w != null; w = w.next) {
        freq += ((WeightedDisiWrapper) w).freq();
        if (freq < 0) { // overflow
          return Integer.MAX_VALUE;
        }
      }
      return freq;
    }

    @Override
    public float score() throws IOException {
      return simScorer.score(iterator.docID(), freq());
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }
  }
}
