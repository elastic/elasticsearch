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
import java.util.Map;

import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.search.CollectionStatistics;
import org.apache.lucene5_shaded.search.Explanation;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.TermStatistics;
import org.apache.lucene5_shaded.search.Weight;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.search.similarities.Similarity.SimScorer;

/**
 * Expert-only.  Public for use by other weight implementations
 */
public abstract class SpanWeight extends Weight {

  /**
   * Enumeration defining what postings information should be retrieved from the
   * index for a given Spans
   */
  public enum Postings {
    POSITIONS {
      @Override
      public int getRequiredPostings() {
        return PostingsEnum.POSITIONS;
      }
    },
    PAYLOADS {
      @Override
      public int getRequiredPostings() {
        return PostingsEnum.PAYLOADS;
      }
    },
    OFFSETS {
      @Override
      public int getRequiredPostings() {
        return PostingsEnum.PAYLOADS | PostingsEnum.OFFSETS;
      }
    };

    public abstract int getRequiredPostings();

    public Postings atLeast(Postings postings) {
      if (postings.compareTo(this) > 0)
        return postings;
      return this;
    }
  }

  protected final Similarity similarity;
  protected final Similarity.SimWeight simWeight;
  protected final String field;

  /**
   * Create a new SpanWeight
   * @param query the parent query
   * @param searcher the IndexSearcher to query against
   * @param termContexts a map of terms to termcontexts for use in building the similarity.  May
   *                     be null if scores are not required
   * @throws IOException on error
   */
  public SpanWeight(SpanQuery query, IndexSearcher searcher, Map<Term, TermContext> termContexts) throws IOException {
    super(query);
    this.field = query.getField();
    this.similarity = searcher.getSimilarity(termContexts != null);
    this.simWeight = buildSimWeight(query, searcher, termContexts);
  }

  private Similarity.SimWeight buildSimWeight(SpanQuery query, IndexSearcher searcher, Map<Term, TermContext> termContexts) throws IOException {
    if (termContexts == null || termContexts.size() == 0 || query.getField() == null)
      return null;
    TermStatistics[] termStats = new TermStatistics[termContexts.size()];
    int i = 0;
    for (Term term : termContexts.keySet()) {
      termStats[i] = searcher.termStatistics(term, termContexts.get(term));
      i++;
    }
    CollectionStatistics collectionStats = searcher.collectionStatistics(query.getField());
    return searcher.getSimilarity(true).computeWeight(collectionStats, termStats);
  }

  /**
   * Collect all TermContexts used by this Weight
   * @param contexts a map to add the TermContexts to
   */
  public abstract void extractTermContexts(Map<Term, TermContext> contexts);

  /**
   * Expert: Return a Spans object iterating over matches from this Weight
   * @param ctx a LeafReaderContext for this Spans
   * @param requiredPostings the postings information required
   * @return a Spans
   * @throws IOException on error
   */
  public abstract Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException;

  @Override
  public float getValueForNormalization() throws IOException {
    return simWeight == null ? 1.0f : simWeight.getValueForNormalization();
  }

  @Override
  public void normalize(float queryNorm, float boost) {
    if (simWeight != null) {
      simWeight.normalize(queryNorm, boost);
    }
  }

  @Override
  public SpanScorer scorer(LeafReaderContext context) throws IOException {
    final Spans spans = getSpans(context, Postings.POSITIONS);
    if (spans == null) {
      return null;
    }
    final SimScorer docScorer = getSimScorer(context);
    return new SpanScorer(this, spans, docScorer);
  }

  /**
   * Return a SimScorer for this context
   * @param context the LeafReaderContext
   * @return a SimWeight
   * @throws IOException on error
   */
  public SimScorer getSimScorer(LeafReaderContext context) throws IOException {
    return simWeight == null ? null : similarity.simScorer(simWeight, context);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    SpanScorer scorer = scorer(context);
    if (scorer != null) {
      int newDoc = scorer.iterator().advance(doc);
      if (newDoc == doc) {
        float freq = scorer.sloppyFreq();
        SimScorer docScorer = similarity.simScorer(simWeight, context);
        Explanation freqExplanation = Explanation.match(freq, "phraseFreq=" + freq);
        Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
        return Explanation.match(scoreExplanation.getValue(),
            "weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:",
            scoreExplanation);
      }
    }

    return Explanation.noMatch("no matching term");
  }
}
