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

import org.apache.lucene5_shaded.util.Bits;

/** This class is used to score a range of documents at
 *  once, and is returned by {@link Weight#bulkScorer}.  Only
 *  queries that have a more optimized means of scoring
 *  across a range of documents need to override this.
 *  Otherwise, a default implementation is wrapped around
 *  the {@link Scorer} returned by {@link Weight#scorer}. */

public abstract class BulkScorer {

  /** Scores and collects all matching documents.
   * @param collector The collector to which all matching documents are passed.
   * @param acceptDocs {@link Bits} that represents the allowed documents to match, or
   *                   {@code null} if they are all allowed to match.
   */
  public void score(LeafCollector collector, Bits acceptDocs) throws IOException {
    final int next = score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assert next == DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * Collects matching documents in a range and return an estimation of the
   * next matching document which is on or after {@code max}.
   * <p>The return value must be:</p><ul>
   *   <li>&gt;= {@code max},</li>
   *   <li>{@link DocIdSetIterator#NO_MORE_DOCS} if there are no more matches,</li>
   *   <li>&lt;= the first matching document that is &gt;= {@code max} otherwise.</li>
   * </ul>
   * <p>{@code min} is the minimum document to be considered for matching. All
   * documents strictly before this value must be ignored.</p>
   * <p>Although {@code max} would be a legal return value for this method, higher
   * values might help callers skip more efficiently over non-matching portions
   * of the docID space.</p>
   * <p>For instance, a {@link Scorer}-based implementation could look like
   * below:</p>
   * <pre class="prettyprint">
   * private final Scorer scorer; // set via constructor
   *
   * public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
   *   collector.setScorer(scorer);
   *   int doc = scorer.docID();
   *   if (doc &lt; min) {
   *     doc = scorer.advance(min);
   *   }
   *   while (doc &lt; max) {
   *     if (acceptDocs == null || acceptDocs.get(doc)) {
   *       collector.collect(doc);
   *     }
   *     doc = scorer.nextDoc();
   *   }
   *   return doc;
   * }
   * </pre>
   *
   * @param  collector The collector to which all matching documents are passed.
   * @param acceptDocs {@link Bits} that represents the allowed documents to match, or
   *                   {@code null} if they are all allowed to match.
   * @param  min Score starting at, including, this document 
   * @param  max Score up to, but not including, this doc
   * @return an under-estimation of the next matching doc after max
   */
  public abstract int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException;

  /**
   * Same as {@link DocIdSetIterator#cost()} for bulk scorers.
   */
  public abstract long cost();
}
