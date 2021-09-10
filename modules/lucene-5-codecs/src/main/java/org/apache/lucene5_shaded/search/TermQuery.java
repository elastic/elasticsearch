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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene5_shaded.index.IndexReaderContext;
import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.ReaderUtil;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.search.similarities.Similarity.SimScorer;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * A Query that matches documents containing a term. This may be combined with
 * other terms with a {@link BooleanQuery}.
 */
public class TermQuery extends Query {

  private final Term term;
  private final TermContext perReaderTermState;

  final class TermWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final TermContext termStates;
    private final boolean needsScores;

    public TermWeight(IndexSearcher searcher, boolean needsScores, TermContext termStates)
        throws IOException {
      super(TermQuery.this);
      this.needsScores = needsScores;
      assert termStates != null : "TermContext must not be null";
      // checked with a real exception in TermQuery constructor
      assert termStates.hasOnlyRealTerms();
      this.termStates = termStates;
      this.similarity = searcher.getSimilarity(needsScores);

      final CollectionStatistics collectionStats;
      final TermStatistics termStats;
      if (needsScores) {
        collectionStats = searcher.collectionStatistics(term.field());
        termStats = searcher.termStatistics(term, termStates);
      } else {
        // do not bother computing actual stats, scores are not needed
        final int maxDoc = searcher.getIndexReader().maxDoc();
        final int docFreq = termStates.docFreq();
        final long totalTermFreq = termStates.totalTermFreq();
        collectionStats = new CollectionStatistics(term.field(), maxDoc, -1, -1, -1);
        termStats = new TermStatistics(term.bytes(), docFreq, totalTermFreq);
      }
     
      this.stats = similarity.computeWeight(collectionStats, termStats);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      terms.add(getTerm());
    }

    @Override
    public String toString() {
      return "weight(" + TermQuery.this + ")";
    }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float boost) {
      stats.normalize(queryNorm, boost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) : "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
      final TermsEnum termsEnum = getTermsEnum(context);
      if (termsEnum == null) {
        return null;
      }
      PostingsEnum docs = termsEnum.postings(null, needsScores ? PostingsEnum.FREQS : PostingsEnum.NONE);
      assert docs != null;
      return new TermScorer(this, docs, similarity.simScorer(stats, context));
    }

    /**
     * Returns a {@link TermsEnum} positioned at this weights Term or null if
     * the term does not exist in the given context
     */
    private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
      final TermState state = termStates.get(context.ord);
      if (state == null) { // term is not present in that reader
        assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
        return null;
      }
      // System.out.println("LD=" + reader.getLiveDocs() + " set?=" +
      // (reader.getLiveDocs() != null ? reader.getLiveDocs().get(0) : "null"));
      final TermsEnum termsEnum = context.reader().terms(term.field())
          .iterator();
      termsEnum.seekExact(term.bytes(), state);
      return termsEnum;
    }

    private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
      // only called from assert
      // System.out.println("TQ.termNotInReader reader=" + reader + " term=" +
      // field + ":" + bytes.utf8ToString());
      return reader.docFreq(term) == 0;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          SimScorer docScorer = similarity.simScorer(stats, context);
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + ") ["
                  + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }
  }

  /** Constructs a query for the term <code>t</code>. */
  public TermQuery(Term t) {
    term = Objects.requireNonNull(t);
    perReaderTermState = null;
  }

  /**
   * Expert: constructs a TermQuery that will use the provided docFreq instead
   * of looking up the docFreq against the searcher.
   */
  public TermQuery(Term t, TermContext states) {
    assert states != null;
    term = Objects.requireNonNull(t);
    if (states.hasOnlyRealTerms() == false) {
      // The reason for this is that fake terms might have the same bytes as
      // real terms, and this confuses query caching because they don't match
      // the same documents
      throw new IllegalArgumentException("Term queries must be created on real terms");
    }
    perReaderTermState = Objects.requireNonNull(states);
  }

  /** Returns the term of this query. */
  public Term getTerm() {
    return term;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final IndexReaderContext context = searcher.getTopReaderContext();
    final TermContext termState;
    if (perReaderTermState == null
        || perReaderTermState.wasBuiltFor(context) == false) {
      // make TermQuery single-pass if we don't have a PRTS or if the context
      // differs!
      termState = TermContext.build(context, term);
    } else {
      // PRTS was pre-build for this IS
      termState = this.perReaderTermState;
    }

    return new TermWeight(searcher, needsScores, termState);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TermQuery)) return false;
    TermQuery other = (TermQuery) o;
    return super.equals(o) && this.term.equals(other.term);
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ term.hashCode();
  }
}
