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
import java.util.Set;

import org.apache.lucene5_shaded.index.IndexReaderContext;
import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.util.Bits;

/**
 * Expert: Calculate query weights and build query scorers.
 * <p>
 * The purpose of {@link Weight} is to ensure searching does not modify a
 * {@link Query}, so that a {@link Query} instance can be reused. <br>
 * {@link IndexSearcher} dependent state of the query should reside in the
 * {@link Weight}. <br>
 * {@link LeafReader} dependent state should reside in the {@link Scorer}.
 * <p>
 * Since {@link Weight} creates {@link Scorer} instances for a given
 * {@link LeafReaderContext} ({@link #scorer(LeafReaderContext)})
 * callers must maintain the relationship between the searcher's top-level
 * {@link IndexReaderContext} and the context used to create a {@link Scorer}. 
 * <p>
 * A <code>Weight</code> is used in the following way:
 * <ol>
 * <li>A <code>Weight</code> is constructed by a top-level query, given a
 * <code>IndexSearcher</code> ({@link Query#createWeight(IndexSearcher, boolean)}).
 * <li>The {@link #getValueForNormalization()} method is called on the
 * <code>Weight</code> to compute the query normalization factor
 * {@link Similarity#queryNorm(float)} of the query clauses contained in the
 * query.
 * <li>The query normalization factor is passed to {@link #normalize(float, float)}. At
 * this point the weighting is complete.
 * <li>A <code>Scorer</code> is constructed by
 * {@link #scorer(LeafReaderContext)}.
 * </ol>
 * 
 * @since 2.9
 */
public abstract class Weight {

  protected final Query parentQuery;

  /** Sole constructor, typically invoked by sub-classes.
   * @param query         the parent query
   */
  protected Weight(Query query) {
    this.parentQuery = query;
  }

  /**
   * Expert: adds all terms occurring in this query to the terms set. If the
   * {@link Weight} was created with {@code needsScores == true} then this
   * method will only extract terms which are used for scoring, otherwise it
   * will extract all terms which are used for matching.
   */
  public abstract void extractTerms(Set<Term> terms);

  /**
   * An explanation of the score computation for the named document.
   * 
   * @param context the readers context to create the {@link Explanation} for.
   * @param doc the document's id relative to the given context's reader
   * @return an Explanation for the score
   * @throws IOException if an {@link IOException} occurs
   */
  public abstract Explanation explain(LeafReaderContext context, int doc) throws IOException;

  /** The query that this concerns. */
  public final Query getQuery() {
    return parentQuery;
  }
  
  /** The value for normalization of contained query clauses (e.g. sum of squared weights). */
  public abstract float getValueForNormalization() throws IOException;

  /** Assigns the query normalization factor and boost to this. */
  public abstract void normalize(float norm, float boost);

  /**
   * Returns a {@link Scorer} which can iterate in order over all matching
   * documents and assign them a score.
   * <p>
   * <b>NOTE:</b> null can be returned if no documents will be scored by this
   * query.
   * <p>
   * <b>NOTE</b>: The returned {@link Scorer} does not have
   * {@link LeafReader#getLiveDocs()} applied, they need to be checked on top.
   * 
   * @param context
   *          the {@link LeafReaderContext} for which to return the {@link Scorer}.
   *          
   * @return a {@link Scorer} which scores documents in/out-of order.
   * @throws IOException if there is a low-level I/O error
   */
  public abstract Scorer scorer(LeafReaderContext context) throws IOException;

  /**
   * Optional method, to return a {@link BulkScorer} to
   * score the query and send hits to a {@link Collector}.
   * Only queries that have a different top-level approach
   * need to override this; the default implementation
   * pulls a normal {@link Scorer} and iterates and
   * collects the resulting hits which are not marked as deleted.
   *
   * @param context
   *          the {@link LeafReaderContext} for which to return the {@link Scorer}.
   *
   * @return a {@link BulkScorer} which scores documents and
   * passes them to a collector.
   * @throws IOException if there is a low-level I/O error
   */
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {

    Scorer scorer = scorer(context);
    if (scorer == null) {
      // No docs match
      return null;
    }

    // This impl always scores docs in order, so we can
    // ignore scoreDocsInOrder:
    return new DefaultBulkScorer(scorer);
  }

  /** Just wraps a Scorer and performs top scoring using it.
   *  @lucene.internal */
  protected static class DefaultBulkScorer extends BulkScorer {
    private final Scorer scorer;
    private final DocIdSetIterator iterator;
    private final TwoPhaseIterator twoPhase;

    /** Sole constructor. */
    public DefaultBulkScorer(Scorer scorer) {
      if (scorer == null) {
        throw new NullPointerException();
      }
      this.scorer = scorer;
      this.iterator = scorer.iterator();
      this.twoPhase = scorer.twoPhaseIterator();
    }

    @Override
    public long cost() {
      return iterator.cost();
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
      collector.setScorer(scorer);
      if (scorer.docID() == -1 && min == 0 && max == DocIdSetIterator.NO_MORE_DOCS) {
        scoreAll(collector, iterator, twoPhase, acceptDocs);
        return DocIdSetIterator.NO_MORE_DOCS;
      } else {
        int doc = scorer.docID();
        if (doc < min) {
          if (twoPhase == null) {
            doc = iterator.advance(min);
          } else {
            doc = twoPhase.approximation().advance(min);
          }
        }
        return scoreRange(collector, iterator, twoPhase, acceptDocs, doc, max);
      }
    }

    /** Specialized method to bulk-score a range of hits; we
     *  separate this from {@link #scoreAll} to help out
     *  hotspot.
     *  See <a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487</a> */
    static int scoreRange(LeafCollector collector, DocIdSetIterator iterator, TwoPhaseIterator twoPhase,
        Bits acceptDocs, int currentDoc, int end) throws IOException {
      if (twoPhase == null) {
        while (currentDoc < end) {
          if (acceptDocs == null || acceptDocs.get(currentDoc)) {
            collector.collect(currentDoc);
          }
          currentDoc = iterator.nextDoc();
        }
        return currentDoc;
      } else {
        final DocIdSetIterator approximation = twoPhase.approximation();
        while (currentDoc < end) {
          if ((acceptDocs == null || acceptDocs.get(currentDoc)) && twoPhase.matches()) {
            collector.collect(currentDoc);
          }
          currentDoc = approximation.nextDoc();
        }
        return currentDoc;
      }
    }
    
    /** Specialized method to bulk-score all hits; we
     *  separate this from {@link #scoreRange} to help out
     *  hotspot.
     *  See <a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487</a> */
    static void scoreAll(LeafCollector collector, DocIdSetIterator iterator, TwoPhaseIterator twoPhase, Bits acceptDocs) throws IOException {
      if (twoPhase == null) {
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
          if (acceptDocs == null || acceptDocs.get(doc)) {
            collector.collect(doc);
          }
        }
      } else {
        // The scorer has an approximation, so run the approximation first, then check acceptDocs, then confirm
        final DocIdSetIterator approximation = twoPhase.approximation();
        for (int doc = approximation.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = approximation.nextDoc()) {
          if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
            collector.collect(doc);
          }
        }
      }
    }
  }

}
