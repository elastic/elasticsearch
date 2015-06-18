package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;

import static org.apache.lucene.search.suggest.xdocument.TopSuggestDocs.SuggestScoreDoc;

/**
 * {@link org.apache.lucene.search.Collector} that collects completion and
 * score, along with document id
 * <p>
 * Non scoring collector that collect completions in order of their
 * pre-computed scores.
 * <p>
 * NOTE: One document can be collected multiple times if a document
 * is matched for multiple unique completions for a given query
 * <p>
 * Subclasses should only override
 * {@link TopSuggestDocsCollector#collect(int, CharSequence, CharSequence, float)}.
 * <p>
 * NOTE: {@link #setScorer(org.apache.lucene.search.Scorer)} and
 * {@link #collect(int)} is not used
 *
 * @lucene.experimental
 */
public class TopSuggestDocsCollector extends SimpleCollector {

  private final SuggestScoreDocPriorityQueue priorityQueue;
  private final int num;

  /**
   * Document base offset for the current Leaf
   */
  protected int docBase;

  /**
   * Sole constructor
   *
   * Collects at most <code>num</code> completions
   * with corresponding document and weight
   */
  public TopSuggestDocsCollector(int num) {
    if (num <= 0) {
      throw new IllegalArgumentException("'num' must be > 0");
    }
    this.num = num;
    this.priorityQueue = new SuggestScoreDocPriorityQueue(num);
  }

  /**
   * Returns the number of results to be collected
   */
  public int getCountToCollect() {
    return num;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;
  }

  /**
   * Called for every matched completion,
   * similar to {@link org.apache.lucene.search.LeafCollector#collect(int)}
   * but for completions.
   *
   * NOTE: collection at the leaf level is guaranteed to be in
   * descending order of score
   */
  public void collect(int docID, CharSequence key, CharSequence[] contexts, float score) throws IOException {
    SuggestScoreDoc current = new SuggestScoreDoc(docBase + docID, key, contexts, score);
    if (current == priorityQueue.insertWithOverflow(current)) {
      // if the current SuggestScoreDoc has overflown from pq,
      // we can assume all of the successive collections from
      // this leaf will be overflown as well
      // TODO: reuse the overflow instance?
      throw new CollectionTerminatedException();
    }
  }

  /**
   * Returns at most <code>num</code> Top scoring {@link org.apache.lucene.search.suggest.xdocument.TopSuggestDocs}s
   */
  public org.apache.lucene.search.suggest.xdocument.TopSuggestDocs get() throws IOException {
    SuggestScoreDoc[] suggestScoreDocs = priorityQueue.getResults();
    if (suggestScoreDocs.length > 0) {
      return new org.apache.lucene.search.suggest.xdocument.TopSuggestDocs(suggestScoreDocs.length, suggestScoreDocs, suggestScoreDocs[0].score);
    } else {
      return org.apache.lucene.search.suggest.xdocument.TopSuggestDocs.EMPTY;
    }
  }

  /**
   * Ignored
   */
  @Override
  public void collect(int doc) throws IOException {
    // {@link #collect(int, CharSequence, CharSequence, long)} is used
    // instead
  }

  /**
   * Ignored
   */
  @Override
  public boolean needsScores() {
    return true;
  }
}
